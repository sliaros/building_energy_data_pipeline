import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from concurrent.futures import ThreadPoolExecutor
import time
import math
import pandas as pd
from tqdm import tqdm
import tempfile
import os
from typing import Optional, Dict, Union, List, Any
from pathlib import Path
import shutil
import csv
from src.schema_generator.sampling_strategies import BaseSamplingStrategy, RandomSamplingStrategy
import json
from src.utility.file_utils import FileUtils
from logs.logging_config import setup_logging
import logging
from src.postgres_managing.postgres_manager import PostgresManager, DatabaseConfig
from abc import ABC, abstractmethod
import backoff

class BaseDataLoader(ABC):
    """
    Abstract base class for data loading strategies.
    Defines the common interface for different data loading implementations.
    """

    @abstractmethod
    def load_data(
            self,
            file_path: Union[str, Path],
            table_name: str,
            chunk_size: int = 100000
    ) -> None:
        """
        Abstract method to load data from a file into a database.

        Args:
            file_path: Path to the source data file
            table_name: Name of the target database table
            chunk_size: Number of rows to process in each batch
        """
        pass

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
            self._logger = logger or logging.getLogger(self.__class__.__name__)

            if not self._logger.hasHandlers():
                setup_logging(
                    log_file='C:\\slPrivateData\\00_portfolio\\building_energy_data_pipeline\\logs\\application.log')

            self._file_utils = file_utils

            self._max_workers = max_workers
            self._max_retries = max_retries
            self._retry_delay = retry_delay

            self._db_params = self._get_db_params(db_type, db_params)
            self._db_config = DatabaseConfig(
                                    host=self._db_params['host'],
                                    port=self._db_params['port'],
                                    database=self._db_params['database'],
                                    user=self._db_params['user'],
                                    password=self._db_params['password']
                                )

            self._database_manager = PostgresManager(self._db_config)

            if not self._database_manager.verify_connection():
                raise Exception(f"Failed to connect to or create database {self._db_config.database}")

            self._sqlalchemy_url = self._database_manager.postgres_params_to_sqlalchemy_url()

            self._engine = create_engine(
                self._database_manager.postgres_params_to_sqlalchemy_url(),
                pool_size=max_workers,
                max_overflow=2
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
                if hasattr(self, '_database_manager'):
                    self._database_manager.close_all_connections()
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
        file_type, reader_func = FileUtils.FileReader.get_file_type_and_reader(file_path)
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

    def _create_staging_table(self, source_table: str, staging_table: str):
        """Create a staging table without indexes using a single transaction."""
        query = f"""
            CREATE UNLOGGED TABLE {staging_table} (LIKE {source_table});
            ALTER TABLE {staging_table} SET UNLOGGED;
        """
        try:
            self._database_manager.execute_query(query, fetch_all=False)
            self._logger.info(f"Created staging table: {staging_table}")
        except Exception as e:
            self._logger.error(f"Failed to create staging table {staging_table}: {e}")
            raise

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

    def _check_existing_data(self, df: pd.DataFrame, target_table: str) -> Dict[str, Any] | None:
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

        # Prepare query parameters
        params = [min_time, max_time]
        # Add entities for each condition
        for _ in range(5):  # We use entities 5 times in the query
            params.append(entities)
            if meters is not None:
                params.append(meters)

        # Execute the query using execute_query
        result = self._database_manager.execute_query(query, params=tuple(params), fetch_all=False)
        has_overlap, overlap_details = result['has_overlap'], result['overlap_details']

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
        result = self._database_manager.execute_query(query, params=(buildings, buildings, buildings), fetch_all=False)
        has_overlap, existing_buildings = result['has_overlap'], result['existing_buildings']

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

    def _process_chunk_with_retry(
            self,
            df: pd.DataFrame,
            staging_table: str,
            chunk_index: int
    ) -> Dict[str, float] | None:
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
            for column in existing_columns:
                df[column] = func(df[column])  # Apply function to the full column
            return df

        temp_file = os.path.join(self._temp_dir, f"chunk_{chunk_index}.csv")

        @backoff.on_exception(
            backoff.expo,
            (psycopg2.OperationalError, psycopg2.InterfaceErro),
            max_tries=self._max_retries,
            on_backoff=lambda details: self._logger.warning(
                f"Chunk {chunk_index} load attempt {details['tries']} failed. "
                f"Retrying in {details['wait']:.2f}s..."
            ),
        )
        def _process_chunk(df: pd.DataFrame):  # Accept df as an argument
            start_time = time.time()

            # Integer columns that need special handling
            integer_columns = [
                'site_id_kaggle', 'building_id_kaggle', 'sqft', 'yearbuilt', 'numberoffloors',
                'occupants', 'precipDepth1HR', 'windDirection', 'cloudCoverage', 'precipDepth6HR'
            ]

            # Apply numeric conversion to integer columns
            df = apply_to_columns(df, integer_columns, convert_to_numeric_int64)

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
                float_format='%.2f'  # Format float numbers with 2 decimal places
            )

            # Copy data using psycopg2 connection
            with self._database_manager.connection_context() as conn:
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

            duration = time.time() - start_time

            # Cleanup temporary file
            os.remove(temp_file)

            return {'duration': duration}

        try:
            return _process_chunk(df)  # Pass df as an argument
        except Exception as e:
            self._logger.error(
                f"Chunk {chunk_index} load failed after {self._max_retries} attempts. "
                f"Error: {e}"
            )
            raise

    def _ensure_unique_constraint(self, target_table: str, unique_columns: List[str]):
        """
        Ensure a unique constraint exists on the target table.
        """
        try:
            columns_str = ", ".join(unique_columns)
            constraint_name = f"uq_{target_table}_{'_'.join(unique_columns)}"

            with self._database_manager.connection_context() as conn:
                with conn.cursor() as cur:
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
        try:
            # Get an estimated row count from PostgreSQL statistics
            # This is much faster than COUNT(*) for large tables
            query = """
                SELECT reltuples::bigint AS estimate
                FROM pg_class
                WHERE relname = %s
            """
            result = self._database_manager.execute_query(query, params=(staging_table,), fetch_all=False)
            estimated_rows = result['estimate']

            self._logger.info(f"Processing approximately {estimated_rows:,} rows from {staging_table}")

            if estimated_rows==0:
                self._logger.info("No rows to merge. Skipping process.")
                return

            # Get column information for the insert statement
            columns = self._get_table_columns(staging_table)

            # Process data in optimized batches
            offset = 0
            total_processed = 0

            with tqdm(total=estimated_rows, desc="Merging Rows", unit="rows") as pbar:
                while True:
                    # Use a single query for the batch insert
                    # This is more efficient than fetching and then inserting
                    with self._database_manager.connection_context() as conn:
                        with conn.cursor() as cur:
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
                                tqdm.write(f"Processed {total_processed:,} rows so far")

            self._logger.info(f"Successfully merged {total_processed:,} rows into {target_table}")

        except Exception as e:
            self._logger.error(f"Failed to merge data from {staging_table} to {target_table}: {e}")
            raise

    def _get_table_columns(self, table_name: str) -> list:
        """
        Helper function to get column names from a table.
        Uses parameterized query to prevent SQL injection.

        Args:
            table_name (str): Name of the table to get columns from

        Returns:
            list: Ordered list of column names
        """
        query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s 
            ORDER BY ordinal_position
        """
        result = self._database_manager.execute_query(query, params=(table_name,), fetch_all=True)
        return [row['column_name'] for row in result]

    def _cleanup_staging(self, staging_table: str):
        """Drop the staging table."""
        try:
            query = f"DROP TABLE IF EXISTS {staging_table}"
            self._database_manager.execute_query(query, fetch_all=False)
            self._logger.info(f"Dropped staging table {staging_table}")
        except Exception as e:
            self._logger.error(f"Failed to drop staging table {staging_table}: {e}")
            raise

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


    @backoff.on_exception(
        backoff.expo,
        (psycopg2.OperationalError, psycopg2.InterfaceError),
        max_tries=super()._max_retries,
        on_backoff=lambda details: super()._logger.warning(
            f"Chunk load attempt {details['tries']} failed. "
            f"Retrying in {details['wait']:.2f}s..."
        ),
    )
    def _load_chunk_with_retry(
            self,
            df: pd.DataFrame,
            table_name: str,
            is_first_chunk: bool
    ) -> Dict[str, float]:
        """
        Load a single chunk with improved performance and connection handling.
        """
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

    def create_table(self,
                      schema_file: Union[str, Path],
                      table_name: str,
                      if_exists: str = 'fail') -> None:
        """Create a table based on the provided schema file and database connection parameters."""
        self._database_manager.create_table_from_schema(schema_file, table_name, if_exists)

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