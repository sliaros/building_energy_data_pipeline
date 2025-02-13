import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import SQLAlchemyError
from concurrent.futures import ThreadPoolExecutor
import time
import math
import pandas as pd
from tqdm import tqdm
import tempfile
import os
from typing import Optional, Dict, Union, List, Tuple
from pathlib import Path
from .base_loader import BaseDataLoader
import shutil
import csv

class PostgresDataLoader(BaseDataLoader):
    """
    PostgreSQL-specific data loader implementation.
    Handles database connections, schema generation, and data loading.
    """
    def __init__(
                self,
                config: Dict,
                logger,
                file_utils,
                max_workers: int = 4,
                max_retries: int = 3,
                retry_delay: float = 1.0,
                db_type: str = "staging",
                db_params: Optional[Dict] = None
        ):
            self._config = config
            self._logger = logger
            self._file_utils = file_utils
            self._max_workers = max_workers
            self._max_retries = max_retries
            self._retry_delay = retry_delay

            self._db_params = self._get_db_params(db_type, db_params)
            self._sqlalchemy_url = self._postgres_params_to_sqlalchemy_url()

            self._verify_connection()
            self._engine = create_engine(
                self._postgres_params_to_sqlalchemy_url(),
                pool_size=max_workers,
                max_overflow=2
            )

            # Create temp directory for chunk files
            self._temp_dir = tempfile.mkdtemp()
            self._logger.info(f"Created temporary directory at {self._temp_dir}")

    def __del__(self):
            """Cleanup temporary directory on object destruction"""
            try:
                if hasattr(self, '_temp_dir') and os.path.exists(self._temp_dir):
                    shutil.rmtree(self._temp_dir)
                    self._logger.info(f"Cleaned up temporary directory {self._temp_dir}")
            except Exception as e:
                self._logger.error(f"Failed to cleanup temporary directory: {e}")

    def _get_db_params(self, db_type: str, db_params: Optional[Dict]) -> Dict:
        """
        Retrieve database parameters based on type.

        Args:
            db_type: Type of database configuration
            db_params: Optional custom parameters

        Returns:
            Database connection parameters
        """
        return db_params or (
            self._config['staging_database'] if db_type=="staging"
            else self._config['database']
        )

    def _verify_connection(self) -> bool:
        """
        Verify and create database if it doesn't exist.

        Returns:
            bool: Connection status
        """
        try:
            with psycopg2.connect(**self._db_params) as conn:
                self._logger.info("Successfully connected to database")
                return True
        except psycopg2.OperationalError as e:
            if "does not exist" in str(e):
                return self._create_database()
            raise

    def _create_database(self) -> bool:
        """
        Create a new database if it doesn't exist.

        Returns:
            bool: Database creation status
        """
        temp_params = self._db_params.copy()
        temp_params['database'] = 'postgres'  # Connect to the default 'postgres' database

        # Open a connection without a transaction block
        conn = psycopg2.connect(**temp_params)
        conn.autocommit = True  # Enable autocommit before using the cursor

        try:
            with conn.cursor() as cur:
                cur.execute(f"CREATE DATABASE {self._db_params['database']}")
            self._logger.info(f"Created database {self._db_params['database']}")
            return True
        finally:
            conn.close()  # Ensure the connection is closed properly

    def _get_file_type_and_reader(self, file_path: Union[str, Path]) -> Tuple[str, callable]:
        """
        Determine file type and return appropriate reader function.

        Args:
            file_path: Path to the file

        Returns:
            Tuple[str, callable]: File type and corresponding reader function
        """
        file_path = Path(file_path)
        if file_path.suffix.lower()=='.parquet':
            return 'parquet', pd.read_parquet
        elif file_path.suffix.lower() in ['.csv', '.txt']:
            return 'csv', pd.read_csv
        else:
            raise ValueError(f"Unsupported file type: {file_path.suffix}")

    def load_data(
            self,
            file_path: Union[str, Path],
            table_name: str,
            chunk_size: int = 100000,
            unique_columns: List[str] = None
    ) -> Dict[str, Union[int, float]]:
        """
        Load data efficiently using COPY command and staging tables.

        Args:
            file_path: Path to the input file
            table_name: Target table name
            chunk_size: Number of rows per chunk
            unique_columns: List of columns that form the unique constraint
        """
        start_time = time.time()
        file_path = Path(file_path)
        total_rows_loaded = 0
        chunk_statuses: List[Dict] = []

        # Determine file type and get appropriate reader
        file_type, reader_func = self._get_file_type_and_reader(file_path)
        self._logger.info(f"Processing {file_type} file: {file_path}")

        # Create staging table
        staging_table = f"{table_name}_staging_{int(time.time())}"
        self._create_staging_table(table_name, staging_table)

        try:
            if file_type=='parquet':
                # For Parquet, read the file to get total rows
                df = reader_func(file_path)
                total_rows = len(df)
                total_chunks = math.ceil(total_rows / chunk_size)

                # Create progress bars
                chunk_pbar = tqdm(total=total_chunks, desc="Chunks Progress", position=0)
                rows_pbar = tqdm(total=total_rows, desc="Rows Progress", position=1)

                # Process in parallel
                with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
                    futures = []

                    for i in range(total_chunks):
                        start_idx = i * chunk_size
                        end_idx = min((i + 1) * chunk_size, total_rows)
                        chunk = df.iloc[start_idx:end_idx]

                        future = executor.submit(
                            self._process_chunk_with_retry,
                            chunk,
                            staging_table,
                            i
                        )
                        futures.append((future, {'chunk_index': i, 'rows': len(chunk)}))

                    # Process completed futures
                    for future, chunk_info in futures:
                        try:
                            result = future.result()
                            self._process_chunk_result(chunk_info, result, chunk_statuses,
                                total_rows_loaded, chunk_pbar, rows_pbar)
                            total_rows_loaded += chunk_info['rows']
                        except Exception as e:
                            self._handle_chunk_error(chunk_info, e, chunk_statuses)

            else:  # CSV file
                # Process CSV file in chunks
                chunk_pbar = tqdm(desc="Chunks Progress", position=0)
                rows_pbar = tqdm(desc="Rows Progress", position=1)

                with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
                    futures = []

                    for i, chunk in enumerate(reader_func(file_path, chunksize=chunk_size)):
                        future = executor.submit(
                            self._process_chunk_with_retry,
                            chunk,
                            staging_table,
                            i
                        )
                        futures.append((future, {'chunk_index': i, 'rows': len(chunk)}))

                    # Process completed futures
                    for future, chunk_info in futures:
                        try:
                            result = future.result()
                            self._process_chunk_result(chunk_info, result, chunk_statuses,
                                total_rows_loaded, chunk_pbar, rows_pbar)
                            total_rows_loaded += chunk_info['rows']
                        except Exception as e:
                            self._handle_chunk_error(chunk_info, e, chunk_statuses)

            # After all chunks are processed, merge data to final table
            self._merge_staging_to_final(staging_table, table_name, unique_columns)

        finally:
            # Cleanup
            chunk_pbar.close()
            rows_pbar.close()
            self._cleanup_staging(staging_table)

        # Calculate and log final statistics
        total_duration = time.time() - start_time
        stats = self._calculate_statistics(chunk_statuses, total_rows_loaded, total_duration)
        self._log_load_statistics(table_name, stats)
        return stats

    def _get_connection(self):
        """Get a raw psycopg2 connection using the current database parameters."""
        return psycopg2.connect(**self._db_params)

    def _create_staging_table(self, source_table: str, staging_table: str):
        """Create a staging table based on the source table schema."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"CREATE TABLE {staging_table} (LIKE {source_table})")
                conn.commit()
            self._logger.info(f"Created staging table {staging_table}")
        finally:
            conn.close()

    def _process_chunk_with_retry(
            self,
            df: pd.DataFrame,
            staging_table: str,
            chunk_index: int
    ) -> Dict[str, float]:
        """Process a single chunk with retries using COPY command."""
        temp_file = os.path.join(self._temp_dir, f"chunk_{chunk_index}.csv")

        for attempt in range(self._max_retries):
            try:
                start_time = time.time()

                # Convert numeric columns with proper integer handling
                def convert_to_numeric_int64(column: pd.Series) -> pd.Series:
                    # First convert to numeric, coercing errors to NaN
                    numeric_series = pd.to_numeric(column, errors="coerce")
                    # For integer columns, round and convert to integers
                    numeric_series = numeric_series.round().astype('Int64', errors='ignore')
                    # Convert NaN to None (which will become NULL in the database)
                    return numeric_series.where(numeric_series.notna(), None)

                def apply_to_columns(df: pd.DataFrame, columns_list: list, func) -> pd.DataFrame:
                    """
                    Applies a custom function to specified columns in a DataFrame if they exist.

                    Parameters:
                        df (pd.DataFrame): The input DataFrame.
                        columns_list (list): List of columns to check and apply the function to.
                        func (function): The custom function to apply to the columns.

                    Returns:
                        pd.DataFrame: The modified DataFrame.
                    """
                    existing_columns = [col for col in columns_list if col in df.columns]

                    # Apply function to the entire column (not element-wise)
                    for column in existing_columns:
                        df[column] = func(df[column])  # Apply function to the full column
                    non_existing_columns = set(columns_list) - set(existing_columns)
                    if non_existing_columns:
                        print(f"The following columns do not exist in the DataFrame: {non_existing_columns}")

                    return df

                # Integer columns that need special handling
                integer_columns = ['site_id_kaggle', 'building_id_kaggle','sqft',
                                   'yearbuilt', 'numberoffloors',
                                   'occupants']

                df = apply_to_columns(df, integer_columns, convert_to_numeric_int64)

                # for col in float_columns:
                #     if col in df.columns:
                #         df[col] = pd.to_numeric(df[col], errors="coerce")

                # Write chunk to temporary CSV file
                df.to_csv(
                    temp_file,
                    index=False,
                    header=False,
                    sep=',',
                    na_rep='',
                    quoting=csv.QUOTE_MINIMAL,  # Add quotes only when necessary
                    quotechar='"',  # Use double quotes for quoting
                    escapechar='\\',  # Use backslash as escape character
                    doublequote=True,  # Double up quote characters within fields
                    float_format = '%.2f'  # Format float numbers with 2 decimal places
                )

                # Copy data using psycopg2 connection
                conn = self._get_connection()
                try:
                    with conn.cursor() as cur:
                        with open(temp_file, 'r') as f:
                            cur.copy_expert(
                                f"""
                                            COPY {staging_table} FROM STDIN WITH (
                                                FORMAT CSV,
                                                NULL '',
                                                QUOTE '"',
                                                ESCAPE '\\',
                                                DELIMITER ','
                                            )
                                            """,
                                f
                            )
                        conn.commit()
                finally:
                    conn.close()

                duration = time.time() - start_time

                # Cleanup temporary file
                os.remove(temp_file)

                return {'duration': duration}

            except (SQLAlchemyError, psycopg2.Error) as e:
                if os.path.exists(temp_file):
                    os.remove(temp_file)

                delay = self._retry_delay * (2 ** attempt)
                if attempt < self._max_retries - 1:
                    self._logger.warning(
                        f"Chunk {chunk_index} load attempt {attempt + 1} failed. "
                        f"Retrying in {delay:.2f} seconds. Error: {e}"
                    )
                    time.sleep(delay)
                else:
                    self._logger.error(
                        f"Chunk {chunk_index} load failed after {self._max_retries} attempts. "
                        f"Error: {e}"
                    )
                    raise

    def _merge_staging_to_final(
            self,
            staging_table: str,
            target_table: str,
            unique_columns: Optional[List[str]] = None
    ):
        """Merge data from staging to final table with deduplication."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                if unique_columns:
                    # Create unique constraint if it doesn't exist
                    constraint_name = f"{target_table}_unique_constraint"
                    columns_str = ", ".join(unique_columns)
                    try:
                        cur.execute(f"""
                            ALTER TABLE {target_table}
                            ADD CONSTRAINT {constraint_name}
                            UNIQUE ({columns_str});
                        """)
                    except psycopg2.Error:
                        # Constraint might already exist
                        pass

                    # Insert with conflict handling
                    cur.execute(f"""
                        INSERT INTO {target_table}
                        SELECT DISTINCT ON ({columns_str}) *
                        FROM {staging_table}
                        ON CONFLICT ({columns_str}) DO NOTHING
                    """)
                else:
                    # Simple insert if no unique constraints
                    cur.execute(f"""
                        INSERT INTO {target_table}
                        SELECT * FROM {staging_table}
                    """)
                conn.commit()

            self._logger.info(f"Merged data from {staging_table} to {target_table}")
        finally:
            conn.close()

    def _cleanup_staging(self, staging_table: str):
        """Drop the staging table."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
                conn.commit()
            self._logger.info(f"Dropped staging table {staging_table}")
        finally:
            conn.close()

    def _process_chunk_result(self, chunk_info: Dict, result: Dict,
                              chunk_statuses: List, total_rows_loaded: int,
                              chunk_pbar: tqdm, rows_pbar: tqdm) -> None:
        """Helper method to process successful chunk results."""
        chunk_statuses.append({
            'chunk_index': chunk_info['chunk_index'],
            'rows': chunk_info['rows'],
            'success': True,
            'duration': result.get('duration', 0)
        })

        # Update progress bars
        chunk_pbar.update(1)
        rows_pbar.update(chunk_info['rows'])

        # Log chunk completion
        self._logger.info(
            f"Chunk {chunk_info['chunk_index']} completed: "
            f"{chunk_info['rows']:,} rows in {result['duration']:.2f}s "
            f"({chunk_info['rows'] / result['duration']:.0f} rows/s)"
        )

    def _handle_chunk_error(self, chunk_info: Dict, error: Exception,
                            chunk_statuses: List) -> None:
        """Helper method to handle chunk processing errors."""
        chunk_statuses.append({
            'chunk_index': chunk_info['chunk_index'],
            'rows': chunk_info['rows'],
            'success': False,
            'error': str(error)
        })
        self._logger.error(f"Failed to load chunk {chunk_info['chunk_index']}: {error}")

    def _calculate_statistics(self, chunk_statuses: List,
                              total_rows_loaded: int,
                              total_duration: float) -> Dict:
        """Calculate final loading statistics."""
        successful_chunks = [cs for cs in chunk_statuses if cs['success']]
        failed_chunks = [cs for cs in chunk_statuses if not cs['success']]

        return {
            'total_rows_loaded': total_rows_loaded,
            'total_duration': total_duration,
            'chunks_total': len(chunk_statuses) + 1,  # Add 1 for first chunk
            'chunks_successful': len(successful_chunks) + 1,
            'chunks_failed': len(failed_chunks),
            'rows_per_second': total_rows_loaded / total_duration if total_duration > 0 else 0
        }


    def _load_chunk_with_retry(
            self,
            df: pd.DataFrame,
            table_name: str,
            is_first_chunk: bool
    ) -> Dict[str, float]:
        """
        Load a single chunk with improved performance and connection handling.
        """
        for attempt in range(self._max_retries):
            try:
                start_time = time.time()

                with self._engine.begin() as conn:
                    df.to_sql(
                        table_name,
                        conn,
                        if_exists='replace' if is_first_chunk else 'append',
                        index=False,
                        method='multi',
                        chunksize=100000  # Optimize bulk insert size
                    )

                duration = time.time() - start_time
                return {'duration': duration}

            except (SQLAlchemyError, psycopg2.Error) as e:
                delay = self._retry_delay * (2 ** attempt)

                if attempt < self._max_retries - 1:
                    self._logger.warning(
                        f"Chunk load attempt {attempt + 1} failed. "
                        f"Retrying in {delay:.2f} seconds. Error: {e}"
                    )
                    time.sleep(delay)
                else:
                    self._logger.error(
                        f"Chunk load failed after {self._max_retries} attempts. "
                        f"Error: {e}"
                    )
                    raise

    def _postgres_params_to_sqlalchemy_url(self) -> URL:
        """
        Convert PostgreSQL parameters to SQLAlchemy URL.

        Returns:
            SQLAlchemy connection URL
        """
        return URL.create(
            drivername="postgresql+psycopg2",
            username=self._db_params["user"],
            password=self._db_params["password"],
            host=self._db_params["host"],
            port=self._db_params["port"],
            database=self._db_params["database"]
        )

    def _generate_schema(self):
        pass

    @staticmethod
    def _table_exists(conn, table_name):
        """Check if a table exists in the database."""
        query = """
            SELECT EXISTS (
                SELECT 1
                FROM pg_tables
                WHERE tablename = %s
            );
        """
        with conn.cursor() as cur:
            cur.execute(query, (table_name,))
            return cur.fetchone()[0]  # Returns True or False

    def _create_table(self,
                      schema_file: Union[str, Path],
                      table_name: str,
                      if_exists: str = 'fail') -> None:
        """Create a table based on the provided schema file and database connection parameters."""
        assert if_exists in ['fail', 'replace']

        try:
            schema_file = Path(schema_file)
            if not schema_file.exists():
                raise FileNotFoundError(f"Schema file not found: {schema_file}")

            self._logger.info(f"Creating table {table_name} in database {self._db_params['database']}")

            with psycopg2.connect(**self._db_params) as conn:
                with open(schema_file, 'r') as f:
                    sql_schema = f.read()
                    with conn.cursor() as cur:
                        if self._table_exists(conn, table_name):
                            self._logger.info(f"Table {table_name} already exists in database {self._db_params['database']}")
                            if if_exists == 'replace':
                                self._logger.info(f"Dropping table {table_name} from database {self._db_params['database']}")
                                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                            else:
                                self._logger.info(f"Aborting creation of table {table_name} in database {self._db_params['database']}")
                                return
                        cur.execute(sql_schema)
                        self._logger.info(f"Successfully created table {table_name} in database {self._db_params['database']}")
        except Exception as e:
            self._logger.error(f"Failed to create table: {str(e)}")
            raise

    def _log_load_statistics(
            self,
            table_name: str,
            stats: Dict[str, Union[int, float]]
    ):
        """
        Log comprehensive loading statistics.

        Args:
            table_name: Name of the table being loaded
            stats: Loading statistics dictionary
        """
        log_message = (
            f"Data Load Statistics for Table '{table_name}':\n"
            f"  Total Rows Loaded: {stats['total_rows_loaded']:,}\n"
            f"  Total Duration: {stats['total_duration']:.2f} seconds\n"
            f"  Rows/Second: {stats['rows_per_second']:.2f}\n"
            f"  Chunks Total: {stats['chunks_total']}\n"
            f"  Chunks Successful: {stats['chunks_successful']}\n"
            f"  Chunks Failed: {stats['chunks_failed']}"
        )

        # Log as info or warning based on success
        log_method = (
            self._logger.warning
            if stats['chunks_failed'] > 0
            else self._logger.info
        )
        log_method(log_message)