import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import SQLAlchemyError
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import math
import pandas as pd
from tqdm import tqdm

from typing import Optional, Dict, Union, List, Tuple
from pathlib import Path

from .base_loader import BaseDataLoader
from .file_processor import FileProcessor
from src.data_loading.schema_generator import SchemaAnalyzer, SamplingStrategy, SQLSchemaGenerator


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
        """
        Initialize PostgreSQL data loader.

        Args:
            config: Project configuration dictionary
            logger: Logging utility
            file_utils: File utility functions
            max_workers: Maximum number of concurrent threads
            db_type: Type of database (staging or production)
            db_params: Optional database connection parameters
        """
        self._config = config
        self._logger = logger
        self._file_utils = file_utils
        self._max_workers = max_workers
        self._max_retries = max_retries
        self._retry_delay = retry_delay

        self._sampling_strategy = SamplingStrategy(
            method="stratified",
            max_rows=100000,
            sampling_ratio=0.005
        )

        self._db_params = self._get_db_params(db_type, db_params)
        self._sqlalchemy_url = self._postgres_params_to_sqlalchemy_url()

        self._verify_connection()

        self._engine = create_engine(
            self._postgres_params_to_sqlalchemy_url(),
            pool_size=max_workers,
            max_overflow=2
        )

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
        temp_params['database'] = 'postgres'

        with psycopg2.connect(**temp_params) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                cur.execute(f"CREATE DATABASE {self._db_params['database']}")

        self._logger.info(f"Created database {self._db_params['database']}")
        return True

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
            chunk_size: int = 100000
    ) -> Dict[str, Union[int, float]]:
        """
        Load data from CSV or Parquet files with progress tracking.
        """
        start_time = time.time()
        total_rows_loaded = 0
        file_path = Path(file_path)

        # Determine file type and get appropriate reader
        file_type, reader_func = self._get_file_type_and_reader(file_path)
        self._logger.info(f"Processing {file_type} file: {file_path}")

        # Initialize tracking variables
        chunk_statuses: List[Dict] = []
        processed_chunks = 0

        try:
            if file_type=='parquet':
                # For Parquet, read the file to get total rows
                df = reader_func(file_path)
                total_rows = len(df)
                total_chunks = math.ceil(total_rows / chunk_size)

                # Create progress bars
                chunk_pbar = tqdm(total=total_chunks, desc="Chunks Progress", position=0)
                rows_pbar = tqdm(total=total_rows, desc="Rows Progress", position=1)

                # Process Parquet file in chunks
                with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
                    futures = []

                    # Load first chunk to create table
                    first_chunk = df.iloc[:chunk_size]
                    self._load_chunk_with_retry(first_chunk, table_name, True)
                    total_rows_loaded += len(first_chunk)
                    chunk_pbar.update(1)
                    rows_pbar.update(len(first_chunk))

                    # Process remaining chunks
                    for i in range(1, total_chunks):
                        start_idx = i * chunk_size
                        end_idx = min((i + 1) * chunk_size, total_rows)
                        chunk = df.iloc[start_idx:end_idx]

                        future = executor.submit(
                            self._load_chunk_with_retry,
                            chunk,
                            table_name,
                            False
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
                with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
                    futures = []

                    # Read and process first chunk to create table
                    first_chunk = next(reader_func(file_path, chunksize=chunk_size))
                    self._load_chunk_with_retry(first_chunk, table_name, True)
                    total_rows_loaded += len(first_chunk)

                    # Create progress tracking (without total for CSV)
                    chunk_pbar = tqdm(desc="Chunks Progress", position=0)
                    rows_pbar = tqdm(desc="Rows Progress", position=1)
                    chunk_pbar.update(1)
                    rows_pbar.update(len(first_chunk))

                    # Process remaining chunks
                    for i, chunk in enumerate(reader_func(file_path, chunksize=chunk_size, skiprows=chunk_size)):
                        future = executor.submit(
                            self._load_chunk_with_retry,
                            chunk,
                            table_name,
                            False
                        )
                        futures.append((future, {'chunk_index': i + 1, 'rows': len(chunk)}))

                    # Process completed futures
                    for future, chunk_info in futures:
                        try:
                            result = future.result()
                            self._process_chunk_result(chunk_info, result, chunk_statuses,
                                total_rows_loaded, chunk_pbar, rows_pbar)
                            total_rows_loaded += chunk_info['rows']
                        except Exception as e:
                            self._handle_chunk_error(chunk_info, e, chunk_statuses)

        finally:
            # Close progress bars
            chunk_pbar.close()
            rows_pbar.close()

        # Calculate and log final statistics
        total_duration = time.time() - start_time
        stats = self._calculate_statistics(chunk_statuses, total_rows_loaded, total_duration)
        self._log_load_statistics(table_name, stats)
        return stats

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

    def _generate_schema(
            self,
            file_path: Union[str, Path],
            table_name: Optional[str] = None,
            output_folder: Optional[Union[str, Path]] = None,
            if_exists: str = 'fail'
    ) -> Optional[Dict[str, Union[str, Path]]]:
        """
        Generate PostgreSQL schema from a data file.

        Args:
            file_path: Path to source data file
            table_name: Optional custom table name
            output_folder: Optional output directory for schema

        Returns:
            Dict with schema generation details
        """
        assert if_exists in ['fail', 'replace']

        try:
            output_folder = output_folder or self._config['project_data']['schemas_dir_path']
            table_name = table_name or SQLSchemaGenerator._derive_table_name(file_path)

            schema_path = Path(output_folder) / f"{table_name}_schema.sql"

            if schema_path.exists():
                if if_exists == 'fail':
                    self._logger.info(f"Schema file exists: {schema_path}")
                    return {f"Table name": table_name,
                            "Sql schema file path": schema_path}
                else:
                    self._logger.info(f"Replacing schema file: {schema_path}")

            columns = SchemaAnalyzer(file_path, self._sampling_strategy).analyze()
            generator = SQLSchemaGenerator(table_name)
            sql_schema = generator.generate_schema(columns, file_path)

            schema_path.write_text(sql_schema)

            return {
                "table_name": table_name,
                "schema_file_path": schema_path
            }

        except Exception as e:
            self._logger.error(f"Schema generation failed: {e}")
            raise
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