import psycopg2
from psycopg2 import pool
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
from typing import Optional, Dict, Union, List, Tuple, Any
from pathlib import Path
from .base_loader import BaseDataLoader
import shutil
import csv
from src.schema_generator.sampling_strategies import BaseSamplingStrategy, RandomSamplingStrategy
import pyarrow.parquet as pq
import pyarrow as pa
import json
import re

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
                db_params: Optional[Dict] = None,
                sampling_strategy: Optional[BaseSamplingStrategy] = None
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

            self._pool = pool.SimpleConnectionPool(
                minconn=1,
                maxconn=self._max_workers + 2,
                **self._db_params
            )
            self._logger.info(f"Created connection pool with max {max_workers + 2} connections.")

            # Create temp directory for chunk files
            self._temp_dir = tempfile.mkdtemp()
            self._logger.info(f"Created temporary directory at {self._temp_dir}")

            self._sampling_strategy = sampling_strategy or RandomSamplingStrategy()

    def __del__(self):
            """Cleanup temporary directory on object destruction"""
            try:
                if hasattr(self, '_temp_dir') and os.path.exists(self._temp_dir):
                    shutil.rmtree(self._temp_dir)
                    self._logger.info(f"Cleaned up temporary directory {self._temp_dir}")

                    # **Close the connection pool**
                if hasattr(self, '_pool'):
                    self._pool.closeall()
                    self._logger.info("Closed all connections in the pool.")
            except Exception as e:
                self._logger.error(f"Cleanup failed: {e}")

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

    def _create_database(self) -> bool|None:
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
            self._release_connection(conn)  # Ensure the connection is closed properly

    def _get_file_type_and_reader(self, file_path: Union[str, Path]) -> Tuple[str, callable]:
        """
        Determine file type and return appropriate reader function.

        Args:
            file_path: Path to the file

        Returns:
            Tuple[str, callable]: File type and corresponding reader function
        """
        file_path = Path(file_path)

        def read_parquet(file_path: Union[str, Path], nrows: int = None) -> pd.DataFrame:
            """
            Reads a Parquet file into a Pandas DataFrame, optionally limiting the number of rows.

            Args:
                file_path (Union[str, Path]): Path to the Parquet file.
                nrows (int, optional): Number of rows to read. If None, reads the entire file.

            Returns:
                pd.DataFrame: A Pandas DataFrame containing the data.
            """
            # Open the Parquet file
            parquet_file = pq.ParquetFile(file_path)

            # If nrows is not specified, read the entire file
            if nrows is None:
                return parquet_file.read().to_pandas()

            # Read the first n rows
            rows_read = 0
            tables = []

            # Iterate through row groups until we have enough rows
            for i in range(parquet_file.num_row_groups):
                table = parquet_file.read_row_groups(row_groups=[i], columns=None)
                tables.append(table)
                rows_read += table.num_rows
                if rows_read >= nrows:
                    break

            # Combine tables and select the first nrows rows
            combined_table = pa.Table.from_batches([batch for table in tables for batch in table.to_batches()])
            df = combined_table.to_pandas().head(nrows)
            return df

        if file_path.suffix.lower()=='.parquet':
            return 'parquet', read_parquet
        elif file_path.suffix.lower() in ['.csv', '.txt']:
            return 'csv', pd.read_csv
        else:
            raise ValueError(f"Unsupported file type: {file_path.suffix}")

    def load_data(
            self,
            file_path: Union[str, Path],
            table_name: str,
            chunk_size: int = 200000,
            unique_columns: List[str] = None
    ) -> Dict[str, Union[int, float]]|None:
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

        self._logger.info(f"Checking for duplicated or overlapping data")
        df = reader_func(file_path, nrows = 1000)
        if self._check_data_overlap(df, table_name)['has_overlap']:
            self._logger.info(f"Data already exists in {table_name}")
            return

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
            self._merge_staging_to_final(staging_table, table_name)

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
        """Get a connection from the pool instead of creating a new one."""
        try:
            _connection = self._pool.getconn()
            if _connection.closed:
                self._pool.putconn(_connection, close=True)  # Remove bad connection
                _connection = self._pool.getconn()
            self._logger.info(f"Total: {self._pool.maxconn}, In Use: {len(self._pool._used)}, Available: {len(self._pool._pool)}")
            return _connection
        except Exception as e:
            self._logger.error(f"Failed to get a connection from the pool: {e}")
            raise

    def _release_connection(self, conn):
        """Return the connection back to the pool."""
        try:
            if conn:
                self._pool.putconn(conn)
        except Exception as e:
            self._logger.error(f"Failed to return connection to pool: {e}")

    def _create_staging_table(self, source_table: str, staging_table: str):
        """Create a staging table without indexes."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"CREATE UNLOGGED TABLE {staging_table} (LIKE {source_table})")
                cur.execute(f"ALTER TABLE {staging_table} SET unlogged")
                conn.commit()
                self._logger.info(f"Created staging table: {staging_table}")
        finally:
            self._release_connection(conn)

    def _check_data_overlap(self, df: pd.DataFrame, target_table: str) -> Dict[str, Any]:
        """
        Quick check for overlapping time series data before upload.
        Returns detailed information about any overlaps found.

        Args:
            file_path: Path to the data file
            target_table: Name of the target table ('raw', 'weather', or 'metadata')

        Returns:
            Dict with overlap information:
            {
                'has_overlap': bool,
                'overlap_details': str,
                'overlap_range': Tuple[datetime, datetime] or None,
                'affected_entities': List[str]  # buildings/sites affected
            }
        """
        # Load just enough rows to determine key entities and time range if applicable

        if target_table=='raw':
            return self._check_existing_data(df, 'raw')
        elif target_table=='weather':
            return self._check_existing_data(df, 'weather')
        elif target_table=='metadata':
            return self._check_existing_data(df, 'metadata')
        else:
            raise ValueError(f"Unsupported table for overlap check: {target_table}")

    def _check_existing_data(self, df: pd.DataFrame, target_table: str) -> Dict[str, Any]:
        """
        Common function to check for overlaps across all table types.
        Handles both time series data (raw, weather) and metadata.
        """
        if target_table=='metadata':
            return self._check_metadata_overlap(df)

        # For time series data (raw and weather)
        try:
            min_time = pd.to_datetime(df['timestamp'].min())
            max_time = pd.to_datetime(df['timestamp'].max())

            # Get entity IDs based on table type
            if target_table=='raw':
                entities = list(df['building_id'].unique())  # Changed to list
                meters = list(df['meter'].unique())  # Changed to list
                entity_column = 'building_id'
                additional_conditions = 'AND r.meter = ANY(%s::varchar[])'
                table_alias = 'r'
            else:  # weather
                entities = list(df['site_id'].unique())  # Changed to list
                entity_column = 'site_id'
                additional_conditions = ''
                table_alias = 'w'
                meters = None

        except KeyError as e:
            raise ValueError(f"Missing required column: {e}")

        query = f"""
        WITH file_bounds AS (
            SELECT 
                %s::timestamp as min_time,
                %s::timestamp as max_time
        )
        SELECT 
            EXISTS(
                SELECT 1 
                FROM {target_table} {table_alias}, file_bounds fb
                WHERE {table_alias}.{entity_column} = ANY(%s::varchar[])
                {additional_conditions}
                AND {table_alias}.timestamp::timestamp BETWEEN fb.min_time - interval '1 hour' 
                    AND fb.max_time + interval '1 hour'
            ) as has_overlap,
            CASE WHEN EXISTS(
                SELECT 1 
                FROM {target_table} {table_alias}, file_bounds fb
                WHERE {table_alias}.{entity_column} = ANY(%s::varchar[])
                {additional_conditions}
                AND {table_alias}.timestamp::timestamp BETWEEN fb.min_time - interval '1 hour' 
                    AND fb.max_time + interval '1 hour'
            ) THEN
                json_build_object(
                    'start_time', (
                        SELECT MIN({table_alias}.timestamp::timestamp)
                        FROM {target_table} {table_alias}, file_bounds fb
                        WHERE {table_alias}.{entity_column} = ANY(%s::varchar[])
                        {additional_conditions}
                        AND {table_alias}.timestamp::timestamp BETWEEN fb.min_time - interval '1 hour' 
                            AND fb.max_time + interval '1 hour'
                    ),
                    'end_time', (
                        SELECT MAX({table_alias}.timestamp::timestamp)
                        FROM {target_table} {table_alias}, file_bounds fb
                        WHERE {table_alias}.{entity_column} = ANY(%s::varchar[])
                        {additional_conditions}
                        AND {table_alias}.timestamp::timestamp BETWEEN fb.min_time - interval '1 hour' 
                            AND fb.max_time + interval '1 hour'
                    ),
                    'entities', (
                        SELECT array_agg(DISTINCT {table_alias}.{entity_column})
                        FROM {target_table} {table_alias}, file_bounds fb
                        WHERE {table_alias}.{entity_column} = ANY(%s::varchar[])
                        {additional_conditions}
                        AND {table_alias}.timestamp::timestamp BETWEEN fb.min_time - interval '1 hour' 
                            AND fb.max_time + interval '1 hour'
                    )
                )::text
            ELSE
                NULL
            END as overlap_details
        """

        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                # Prepare query parameters
                params = [min_time, max_time]
                # Add entities for each condition
                for _ in range(5):  # We use entities 5 times in the query
                    params.append(entities)
                    if meters is not None:
                        params.append(meters)

                cur.execute(query, tuple(params))
                has_overlap, overlap_details = cur.fetchone()

                if not has_overlap:
                    return {
                        'has_overlap': False,
                        'overlap_details': None,
                        'overlap_range': None,
                        'affected_entities': []
                    }

                details = json.loads(overlap_details)
                entity_type = 'building(s)' if target_table=='raw' else 'site(s)'

                return {
                    'has_overlap': True,
                    'overlap_details': (
                        f"Found overlapping data for {entity_type} {', '.join(details['entities'])} "
                        f"between {details['start_time']} and {details['end_time']}"
                    ),
                    'overlap_range': (
                        pd.to_datetime(details['start_time']),
                        pd.to_datetime(details['end_time'])
                    ),
                    'affected_entities': details['entities']
                }
        finally:
            self._release_connection(conn)

    def _check_metadata_overlap(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check for overlapping building metadata."""
        try:
            buildings = list(df['building_id'].unique())  # Changed to list
        except KeyError as e:
            raise ValueError(f"Missing required column: {e}")

        query = """
        SELECT 
            EXISTS(
                SELECT 1 
                FROM metadata m
                WHERE m.building_id = ANY(%s::varchar[])
            ) as has_overlap,
            CASE WHEN EXISTS(
                SELECT 1 
                FROM metadata m
                WHERE m.building_id = ANY(%s::varchar[])
            ) THEN
                (SELECT json_agg(m.building_id)
                 FROM metadata m
                 WHERE m.building_id = ANY(%s::varchar[]))::text
            ELSE
                NULL
            END as existing_buildings
        """

        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(query, (buildings, buildings, buildings))
                has_overlap, existing_buildings = cur.fetchone()

                if not has_overlap:
                    return {
                        'has_overlap': False,
                        'overlap_details': None,
                        'overlap_range': None,
                        'affected_entities': []
                    }

                existing = json.loads(existing_buildings)
                return {
                    'has_overlap': True,
                    'overlap_details': f"Found existing metadata for building(s): {', '.join(existing)}",
                    'overlap_range': None,  # Metadata doesn't have time range
                    'affected_entities': existing
                }
        finally:
            self._release_connection(conn)

    def _process_chunk_with_retry(
            self,
            df: pd.DataFrame,
            staging_table: str,
            chunk_index: int
    ) -> Dict[str, float]|None:
        """Process a single chunk with retries using COPY command."""

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
            # if non_existing_columns:
            #     print(f"The following columns do not exist in the DataFrame: {non_existing_columns}")

            return df

        temp_file = os.path.join(self._temp_dir, f"chunk_{chunk_index}.csv")

        for attempt in range(self._max_retries):
            try:
                start_time = time.time()

                # Integer columns that need special handling
                integer_columns = ['site_id_kaggle', 'building_id_kaggle','sqft',
                                   'yearbuilt', 'numberoffloors',
                                   'occupants', 'precipDepth1HR', 'windDirection',
                                   'cloudCoverage', 'windDirection','precipDepth6HR']

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
                            cur.execute("SET synchronous_commit = OFF;")
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
                            cur.execute("SET synchronous_commit = ON;")
                        conn.commit()
                finally:
                    self._release_connection(conn)

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

    def _ensure_unique_constraint(self, target_table: str, unique_columns: List[str], cur):
        """
        Ensure a unique constraint exists on the target table.
        """
        try:
            columns_str = ", ".join(unique_columns)
            constraint_name = f"uq_{target_table}_{'_'.join(unique_columns)}"

            # Check if the unique constraint already exists
            cur.execute(f"""
                SELECT conname
                FROM pg_constraint
                WHERE conrelid = '{target_table}'::regclass
                AND contype = 'u';
            """)
            existing_constraints = {row[0] for row in cur.fetchall()}

            if constraint_name not in existing_constraints:
                self._logger.info(f"Creating unique constraint {constraint_name} on {target_table} ({columns_str})")
                cur.execute(f"""
                    ALTER TABLE {target_table}
                    ADD CONSTRAINT {constraint_name} UNIQUE ({columns_str});
                """)
        except Exception as e:
            self._logger.error(f"Failed to create unique constraint: {e}")
            raise

    def _merge_staging_to_final(self, staging_table: str, target_table: str, batch_size: int = 1000000):
        """
        Merge data from staging to final table using highly optimized batch operations.

        This implementation avoids expensive COUNT operations and uses PostgreSQL's
        statistics to get approximate row counts quickly. It processes data in batches
        while maintaining transactional integrity.

        Args:
            staging_table (str): Name of the source staging table
            target_table (str): Name of the target table for the merge
            batch_size (int): Number of rows to process in each batch
        """
        conn = None
        try:
            conn = self._get_connection()
            conn.autocommit = False

            with conn.cursor() as cur:
                try:
                    # Get an estimated row count from PostgreSQL statistics
                    # This is much faster than COUNT(*) for large tables
                    cur.execute(f"""
                        SELECT reltuples::bigint AS estimate
                        FROM pg_class
                        WHERE relname = %s
                    """, (staging_table,))
                    estimated_rows = cur.fetchone()[0]

                    self._logger.info(f"Processing approximately {estimated_rows:,} rows from {staging_table}")

                    if estimated_rows==0:
                        self._logger.info("No rows to merge. Skipping process.")
                        return

                    # Get column information for the insert statement
                    columns = self._get_table_columns(staging_table)
                    placeholders = ','.join(['%s'] * len(columns))

                    # Prepare the optimized insert statement
                    insert_stmt = f"""
                        INSERT INTO {target_table} ({','.join(columns)})
                        SELECT *
                        FROM {staging_table}
                        OFFSET %s
                        LIMIT %s
                    """

                    # Process data in optimized batches
                    offset = 0
                    total_processed = 0

                    with tqdm(total=estimated_rows, desc="Merging Rows", unit="rows") as pbar:
                        while True:
                            # Use a single query for the batch insert
                            # This is more efficient than fetching and then inserting
                            cur.execute(f"""
                                WITH batch AS (
                                    SELECT *
                                    FROM {staging_table}
                                    OFFSET {offset}
                                    LIMIT {batch_size}
                                )
                                INSERT INTO {target_table}
                                SELECT * FROM batch
                                RETURNING 1
                            """)

                            # Get the actual number of rows inserted
                            inserted_rows = cur.rowcount

                            if inserted_rows==0:
                                break

                            conn.commit()  # Commit after each batch

                            total_processed += inserted_rows
                            offset += batch_size
                            pbar.update(inserted_rows)

                            # Log progress periodically
                            if total_processed % 100000==0:
                                self._logger.info(f"Processed {total_processed:,} rows so far")

                    self._logger.info(f"Successfully merged {total_processed:,} rows into {target_table}")

                except Exception as cur_error:
                    self._logger.error(f"Cursor operation failed: {cur_error}")
                    conn.rollback()
                    raise

        finally:
            if conn:
                self._release_connection(conn)

    def _get_table_columns(self, table_name: str) -> list:
        """
        Helper function to get column names from a table.
        Uses parameterized query to prevent SQL injection.

        Args:
            table_name (str): Name of the table to get columns from

        Returns:
            list: Ordered list of column names
        """
        conn = self._get_connection()  # Get connection explicitly
        try:
            with conn.cursor() as cur:
                cur.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = %s 
                        ORDER BY ordinal_position
                    """, (table_name,))
                return [row[0] for row in cur.fetchall()]
        finally:
            self._release_connection(conn)  # FIX: Release connection

    def _cleanup_staging(self, staging_table: str):
        """Drop the staging table."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
                conn.commit()
            self._logger.info(f"Dropped staging table {staging_table}")
        finally:
            self._release_connection(conn)

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

    def _table_exists(self, conn, table_name):
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_tables
                        WHERE tablename = %s
                    );
                """, (table_name,))
                return cur.fetchone()[0]
        finally:
            self._release_connection(conn)

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