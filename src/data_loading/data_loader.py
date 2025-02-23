import psycopg2
from sqlalchemy import create_engine
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
                db_manager: PostgresManager,
                max_workers: int = 4,
                max_retries: int = 3,
                sampling_strategy: Optional[BaseSamplingStrategy] = None
        ) -> None:
            """
            Initialize the PostgresDataLoader with configuration parameters.

            Args:
                config: A dictionary with configuration settings, e.g. database host, port, user, password, etc.
                max_workers: The maximum number of threads to use for data loading (default: 4)
                max_retries: The maximum number of retries for failed database operations (default: 3)
                db_type: The type of database to connect to (default: "staging")
                db_params: An optional dictionary with database connection parameters
                sampling_strategy: An optional sampling strategy to use for data sampling (default: RandomSamplingStrategy)

            Returns:
                None
            """

            # Create a logger for this class
            self._logger = logging.getLogger(self.__class__.__name__)

            # Set the maximum number of threads to use for data loading
            self._max_workers = max_workers

            # Set the maximum number of retries for failed database operations
            self._max_retries = max_retries

            # Create a PostgresManager object with the database configuration
            self._database_manager = db_manager

            # Create a SQLAlchemy URL from the database connection parameters
            self._sqlalchemy_url = self._database_manager.postgres_params_to_sqlalchemy_url()

            # Create a connection pool with the given number of workers
            self._engine = create_engine(
                self._sqlalchemy_url,
                pool_size=max_workers,
                max_overflow=2
            )

            # Log the creation of the connection pool
            self._logger.info(f"Created connection pool with max {max_workers + 2} connections.")

            # Create a temporary directory for storing chunk files
            self._temp_dir = tempfile.mkdtemp()
            self._logger.info(f"Created temporary directory at {self._temp_dir}")

            # Set the sampling strategy to use for data sampling
            self._sampling_strategy = sampling_strategy or RandomSamplingStrategy()

    def __del__(self):
        """Cleanup temporary directory and database connections on object destruction

        When the DataLoader object is destroyed (e.g. goes out of scope), we need to
        clean up any temporary files and close any open connections to the database.
        """

        # Check if the temporary directory has been created
        if hasattr(self, '_temp_dir') and os.path.exists(self._temp_dir):
            try:
                # Attempt to remove the temporary directory
                shutil.rmtree(self._temp_dir)
            except Exception as e:
                # If there's an error, log it
                self._logger.error(
                    f"Failed to remove temporary directory {self._temp_dir}: {e}"
                )

        # Check if the database manager has been initialized
        if hasattr(self, '_database_manager'):
            try:
                # Attempt to close all connections in the database connection pool
                self._database_manager.close_all_connections()
            except Exception as e:
                # If there's an error, log it
                self._logger.error(
                    f"Failed to close all connections in the pool: {e}"
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

        Returns:
            A dictionary containing statistics about the loading process, including
                - total_rows_loaded: The total number of rows loaded into the database
                - total_duration: The total time taken to complete the loading process
                - chunks_total: The total number of chunks processed
                - chunks_successful: The number of chunks that were successfully processed
                - chunks_failed: The number of chunks that failed to be processed
                - rows_per_second: The number of rows loaded per second
        """
        start_time = time.time()
        file_path = Path(file_path)
        total_rows_loaded = 0
        chunk_statuses: List[Dict] = []

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        if not file_path.is_file():
            raise ValueError(f"Expected a file, not a directory: {file_path}")

        # Determine file type and get appropriate reader
        file_type, reader_func = FileUtils.FileReader.get_file_type_and_reader(file_path)
        self._logger.info(f"Processing {file_type} file: {file_path}")

        self._logger.info(f"Checking for duplicated or overlapping data")
        df = reader_func(file_path, nrows = 1000)
        if self._check_data_overlap(df, table_name)['has_overlap']:
            self._logger.info(f"Data already exists in {table_name}")
            return None

        try:
            # Create staging table
            staging_table = f"{table_name}_staging_{int(time.time())}"
            self._create_staging_table(table_name, staging_table)

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

                        # Submit a task to process the chunk
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
                            # Get the result of the task
                            result = future.result()
                            # Process the result
                            self._process_chunk_result(chunk_info, result, chunk_statuses,
                                total_rows_loaded, chunk_pbar, rows_pbar)
                            # Update the total number of rows loaded
                            total_rows_loaded += chunk_info['rows']
                        except Exception as e:
                            # Handle any exceptions that occur while processing the chunk
                            self._handle_chunk_error(chunk_info, e, chunk_statuses)

            else:  # CSV file
                # Process CSV file in chunks
                chunk_pbar = tqdm(desc="Chunks Progress", position=0)
                rows_pbar = tqdm(desc="Rows Progress", position=1)

                with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
                    futures = []

                    for i, chunk in enumerate(reader_func(file_path, chunksize=chunk_size)):
                        # Submit a task to process the chunk
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
                            # Get the result of the task
                            result = future.result()
                            # Process the result
                            self._process_chunk_result(chunk_info, result, chunk_statuses,
                                total_rows_loaded, chunk_pbar, rows_pbar)
                            # Update the total number of rows loaded
                            total_rows_loaded += chunk_info['rows']
                        except Exception as e:
                            # Handle any exceptions that occur while processing the chunk
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
        """
        Create a staging table based on the structure of the source table,
        without any indexes, in a single transaction.

        This is an optimization to speed up the data loading process. By
        creating the staging table without indexes, we can avoid the extra
        overhead of index maintenance during the data load process. We also
        set the table to be UNLOGGED, which means that it won't be included in
        any backups and won't take up space in the write-ahead log.

        Args:
            source_table: Name of the table to use as a template for the staging table
            staging_table: Name of the staging table to create
        """
        query = f"""
            -- Create a new table with the same structure as the source table
            CREATE UNLOGGED TABLE {staging_table} (LIKE {source_table});

            -- Set the table to be UNLOGGED, which means it won't be included in
            -- any backups and won't take up space in the write-ahead log.
            ALTER TABLE {staging_table} SET UNLOGGED;
        """
        try:
            # Execute the query as a single transaction
            self._database_manager.execute_query(query, fetch_all=False)
            self._logger.info(f"Created staging table: {staging_table}")
        except Exception as e:
            # If anything goes wrong, log the error and raise the exception
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
        if target_table == 'metadata':
            return self._check_metadata_overlap(df)

        # For time series data (raw and weather)
        try:
            # Get the minimum and maximum timestamps from the dataframe
            min_time = pd.to_datetime(df['timestamp'].min())
            max_time = pd.to_datetime(df['timestamp'].max())

            # Get the entity IDs based on the table type
            if target_table == 'raw':
                # Get the unique building IDs from the dataframe
                entities = list(df['building_id'].unique())
                # Get the unique meter IDs from the dataframe
                meters = list(df['meter'].unique())
                # Set the entity column to 'building_id'
                entity_column = 'building_id'
                # Set the additional condition to include the meter column
                additional_conditions = 'AND r.meter = ANY(%s::varchar[])'
                # Set the table alias to 'r'
                table_alias = 'r'
            else:  # weather
                # Get the unique site IDs from the dataframe
                entities = list(df['site_id'].unique())
                # Set the entity column to 'site_id'
                entity_column = 'site_id'
                # Set the additional condition to an empty string
                additional_conditions = ''
                # Set the table alias to 'w'
                table_alias = 'w'
                # Set the meters to None
                meters = None

        except KeyError as e:
            # Raise a ValueError if any required columns are missing
            raise ValueError(f"Missing required column: {e}")

        # Construct a query to check for overlaps
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

        # Prepare the query parameters
        params = [min_time, max_time]
        # Add the entities for each condition
        for _ in range(5):  # We use entities 5 times in the query
            params.append(entities)
            if meters is not None:
                params.append(meters)

        # Execute the query using execute_query
        result = self._database_manager.execute_query(query, params=tuple(params), fetch_all=False)
        # Get the result of the query
        has_overlap, overlap_details = result['has_overlap'], result['overlap_details']

        # If there is no overlap, return a dictionary with the appropriate values
        if not has_overlap:
            return {
                'has_overlap': False,
                'overlap_details': None,
                'overlap_range': None,
                'affected_entities': []
            }

        # If there is an overlap, parse the overlap details and return a dictionary
        details = json.loads(overlap_details)
        entity_type = 'building(s)' if target_table == 'raw' else 'site(s)'

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
        """Check for overlapping building metadata in the `metadata` table.

        This function takes a DataFrame with a 'building_id' column and checks if any of the buildings
        in the DataFrame already have metadata in the database. If there is an overlap, it returns a
        dictionary with details about the overlap, including the affected buildings and a message.
        If there is no overlap, it returns a dictionary with the appropriate values set to None or [].
        """
        try:
            # Get a list of unique building IDs from the DataFrame
            buildings = list(df['building_id'].unique())  # Changed to list
        except KeyError as e:
            # Raise a ValueError if the DataFrame doesn't have a 'building_id' column
            raise ValueError(f"Missing required column: {e}")

        # Construct a query to check for overlapping buildings in the metadata table
        query = """
        SELECT 
            -- Check if any of the buildings in the DataFrame already exist in the metadata table
            EXISTS(
                SELECT 1 
                FROM metadata m
                WHERE m.building_id = ANY(%s::varchar[])
            ) as has_overlap,
            -- If there is an overlap, get a list of the existing building IDs
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

        # Execute the query using execute_query
        result = self._database_manager.execute_query(query, params=(buildings, buildings, buildings), fetch_all=False)
        # Get the result of the query
        has_overlap, existing_buildings = result['has_overlap'], result['existing_buildings']

        # If there is no overlap, return a dictionary with the appropriate values
        if not has_overlap:
            return {
                'has_overlap': False,
                'overlap_details': None,
                'overlap_range': None,
                'affected_entities': []
            }

        # If there is an overlap, parse the existing building IDs and return a dictionary
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
        """
        Process a single chunk with retries using COPY command.

        This function does the following:
        1. Converts numeric columns with proper integer handling by converting to numeric, rounding, and converting to Int64.
        2. Writes the chunk to a temporary CSV file.
        3. Copies the data from the temporary file to the staging table using psycopg2 connection.
        4. Commits the transaction.
        5. Removes the temporary file.

        If any of these steps fail, the function will retry up to _max_retries times with exponential backoff.
        """

        # Define a helper function to convert numeric columns with proper integer handling
        def convert_to_numeric_int64(column: pd.Series) -> pd.Series:
            """
            Converts numeric columns with proper integer handling.

            Parameters:
                column (pd.Series): The input Series.

            Returns:
                pd.Series: The converted Series.
            """
            # First convert to numeric, coercing errors to NaN
            # This is necessary because `pd.to_numeric` will raise a ValueError
            # if the column contains non-numeric values.
            # We use `errors="coerce"` to convert non-numeric values to NaN.
            numeric_series = pd.to_numeric(column, errors="coerce")
            # For integer columns, round and convert to integers
            # This is because the database will not accept decimal values
            # for integer columns.
            # We use `round` to round the values to the nearest integer,
            # and `astype('Int64', errors='ignore')` to convert the values
            # to integers.
            # The `errors='ignore'` parameter is used to ignore any errors
            # that may occur if the column contains non-numeric values.
            numeric_series = numeric_series.round().astype('Int64', errors='ignore')
            # Convert NaN to None (which will become NULL in the database)
            # This is necessary because NaN is not a valid value in the database,
            # and it will cause an error if we try to insert it.
            # We use `where` to replace NaN values with None.
            return numeric_series.where(numeric_series.notna(), None)

        # Define a helper function to apply a custom function to specified columns in a DataFrame if they exist
        def apply_to_columns(df: pd.DataFrame, columns_list: list, func) -> pd.DataFrame:
            """
            Applies a custom function to specified columns in a DataFrame if they exist.

            Parameters:
                df (pd.DataFrame): The input DataFrame.
                columns_list (list): List of columns to check and apply the function to.
                func (function): The custom function to apply to the columns.

            Returns:
                pd.DataFrame: The modified DataFrame with the function applied to specified columns.
            """
            # Identify columns that are both in the DataFrame and in the provided list
            existing_columns = [col for col in columns_list if col in df.columns]

            # Iterate over each column that exists in the DataFrame
            for column in existing_columns:
                # Apply the provided function to the entire column
                # This modifies the DataFrame in place by updating the column with the transformed data
                df[column] = func(df[column])

            # Return the modified DataFrame
            return df

        # Define a temporary file path
        temp_file = os.path.join(self._temp_dir, f"chunk_{chunk_index}.csv")

        # Define a function to process a single chunk
        @backoff.on_exception(
            backoff.expo,
            (psycopg2.OperationalError, psycopg2.InterfaceError),
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

        This function checks if a unique constraint on the specified columns of the
        target table already exists. If it does not exist, it creates a new unique
        constraint. This helps maintain data integrity by preventing duplicate entries
        in the target table for the specified columns.

        Args:
            target_table (str): The name of the table where the unique constraint should be applied.
            unique_columns (List[str]): A list of column names on which the unique constraint should be imposed.
        """
        try:
            # Join the list of unique columns into a comma-separated string
            columns_str = ", ".join(unique_columns)

            # Construct a constraint name using the target table and unique columns
            # This name should be unique within the database
            constraint_name = f"uq_{target_table}_{'_'.join(unique_columns)}"

            # Use a database connection context to ensure the connection is properly closed
            with self._database_manager.connection_context() as conn:
                # Create a cursor to execute SQL commands
                with conn.cursor() as cur:
                    # Query to fetch existing unique constraints on the target table
                    cur.execute(f"""
                        SELECT conname
                        FROM pg_constraint
                        WHERE conrelid = '{target_table}'::regclass
                        AND contype = 'u';
                    """)
                    # Fetch all constraint names and store them in a set for quick lookup
                    existing_constraints = {row[0] for row in cur.fetchall()}

                    # Check if the desired constraint name is not already present
                    if constraint_name not in existing_constraints:
                        # Log the creation of a new unique constraint
                        self._logger.info(f"Creating unique constraint {constraint_name} on {target_table} ({columns_str})")
                        # Execute the command to add a new unique constraint
                        cur.execute(f"""
                            ALTER TABLE {target_table}
                            ADD CONSTRAINT {constraint_name} UNIQUE ({columns_str});
                        """)
        except Exception as e:
            # Log any error that occurs and re-raise the exception
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

            # We're using an estimated row count instead of COUNT(*) because it's much faster
            # This is especially important for large tables
            self._logger.info(f"Processing approximately {estimated_rows:,} rows from {staging_table}")

            # If there are no rows to merge, we can skip the process
            if estimated_rows==0:
                self._logger.info("No rows to merge. Skipping process.")
                return

            # Get column information for the insert statement
            columns = self._get_table_columns(staging_table)

            # Process data in optimized batches
            offset = 0
            total_processed = 0

            # Use a tqdm progress bar to show progress
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

                            # If we've inserted all the rows, we can exit the loop
                            if inserted_rows==0:
                                break

                            # Commit after each batch
                            conn.commit()

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
        This is useful for when we need to dynamically generate SQL queries.

        This function uses a parameterized query to get the column names.
        This is important because it prevents SQL injection attacks.

        The query is:
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position

        The %s is a placeholder that gets replaced with the actual table name
        when the query is executed. This is how we prevent SQL injection.

        The result of the query is a list of dictionaries, where each dictionary
        has a single key-value pair: 'column_name'. We extract the column names
        from the dictionaries and return them as an ordered list.

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
        result = self._database_manager.execute_query(
            query,
            params=(table_name,),
            fetch_all=True
        )
        return [row['column_name'] for row in result]

    def _cleanup_staging(self, staging_table: str):
        """
        Drop the staging table.

        This function takes the name of the staging table as an argument.
        It then constructs a SQL query to drop the table.
        The query is executed using the execute_query method of the database manager.
        If the query is successful, the function logs a message indicating that the table was dropped.
        If the query fails, the function logs a message with the error message and raises the exception.

        Args:
            staging_table (str): Name of the staging table to drop
        """
        try:
            # Construct the SQL query to drop the table
            query = f"DROP TABLE IF EXISTS {staging_table}"
            # Execute the query using the database manager
            self._database_manager.execute_query(query, fetch_all=False)
            # Log a message indicating that the table was dropped
            self._logger.info(f"Dropped staging table {staging_table}")
        except Exception as e:
            # Log a message with the error message and raise the exception
            self._logger.error(f"Failed to drop staging table {staging_table}: {e}")
            raise

    def _process_chunk_result(self, chunk_info: Dict, result: Dict,
                              chunk_statuses: List, total_rows_loaded: int,
                              chunk_pbar: tqdm, rows_pbar: tqdm) -> None:
        """Helper method to process successful chunk results."""

        # Append the result of the processed chunk to the chunk_statuses list
        # The dictionary includes the chunk index, number of rows processed,
        # success status, and the duration it took to process the chunk
        chunk_statuses.append({
            'chunk_index': chunk_info['chunk_index'],  # Index of the current chunk
            'rows': chunk_info['rows'],                # Number of rows in the current chunk
            'success': True,                           # Status indicating the chunk was processed successfully
            'duration': result.get('duration', 0)      # Duration to process the chunk, default to 0 if not provided
        })

        # Update the progress bars to reflect the completion of the current chunk
        chunk_pbar.update(1)                          # Increment the chunk progress bar by 1
        rows_pbar.update(chunk_info['rows'])          # Increment the row progress bar by the number of rows processed

        # Log an informational message indicating the completion of the chunk
        # Includes details such as the chunk index, number of rows processed,
        # the duration, and the rows processed per second
        self._logger.info(
            f"Chunk {chunk_info['chunk_index']} completed: "
            f"{chunk_info['rows']:,} rows in {result['duration']:.2f}s "
            f"({chunk_info['rows'] / result['duration']:.0f} rows/s)"
        )

    def _handle_chunk_error(self, chunk_info: Dict, error: Exception,
                            chunk_statuses: List) -> None:
        """Helper method to handle errors when processing chunks.

        When a chunk fails to process, this method is called with the
        following arguments:

        - chunk_info: A dictionary with information about the chunk that
          failed, including the chunk index and the number of rows in the
          chunk.
        - error: The exception that was raised when processing the chunk.
        - chunk_statuses: A list of dictionaries, where each dictionary
          represents the result of processing a chunk. If the chunk was
          processed successfully, the dictionary will contain the chunk
          index, number of rows in the chunk, a boolean indicating
          success, and the duration it took to process the chunk. If the
          chunk failed to process, the dictionary will contain the chunk
          index, number of rows in the chunk, a boolean indicating failure,
          and a string representation of the error that was raised.

        This method appends a new dictionary to the chunk_statuses list,
        indicating that the chunk failed to process. It also logs an error
        message with the chunk index and the error message.
        """
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
        """
        Calculate final loading statistics.

        This function processes the list of chunk statuses to determine
        various metrics related to the data loading operation. It calculates
        the total number of chunks processed, the number of successful and
        failed chunks, and the rate at which rows were processed.

        Args:
            chunk_statuses (List): A list of dictionaries containing the status
                                   of each chunk processed. Each dictionary
                                   should have a 'success' key indicating the
                                   success status of the chunk.
            total_rows_loaded (int): The total number of rows successfully
                                     loaded into the database.
            total_duration (float): The total time (in seconds) taken to load
                                    the data.

        Returns:
            Dict: A dictionary containing calculated statistics including
                  'total_rows_loaded', 'total_duration', 'chunks_total',
                  'chunks_successful', 'chunks_failed', and 'rows_per_second'.
        """
        # Filter the list of chunk statuses to find all successful chunks
        successful_chunks = [cs for cs in chunk_statuses if cs['success']]

        # Filter the list of chunk statuses to find all failed chunks
        failed_chunks = [cs for cs in chunk_statuses if not cs['success']]

        # Return a dictionary containing various loading statistics
        return {
            'total_rows_loaded': total_rows_loaded,  # Total rows loaded
            'total_duration': total_duration,        # Total duration of loading
            'chunks_total': len(chunk_statuses) + 1,  # Total chunks processed, including the first one
            'chunks_successful': len(successful_chunks) + 1,  # Successful chunks, including the first one
            'chunks_failed': len(failed_chunks),  # Total failed chunks
            # Calculate rows per second, with a check to avoid division by zero
            'rows_per_second': total_rows_loaded / total_duration if total_duration > 0 else 0
        }


    @backoff.on_exception(
        backoff.expo,
        (psycopg2.OperationalError, psycopg2.InterfaceError),
        max_tries=3,
        on_backoff=lambda details: logging.getLogger(__name__).warning(
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

        This method is a wrapper around the pandas.to_sql() method which is used
        to load a DataFrame into a PostgreSQL database. It adds a few features
        to make the process more robust and efficient:

        - It uses SQLAlchemy's connection pooling to reduce the overhead of
          connecting to the database for each chunk.
        - It uses psycopg2's bulk copy support (via the 'multi' method) to
          improve performance when inserting large amounts of data.
        - It uses backoff's retry decorator to handle some common transient
          errors that can occur when connecting to the database.

        The method takes a DataFrame and a table name as input, and loads the
        DataFrame into the specified table. It returns a dictionary containing
        a single key-value pair, where the key is 'duration' and the value is
        the time it took to load the chunk (in seconds).

        This method is called by the _load_data method, which coordinates the
        loading of all chunks in the DataFrame.
        """
        start_time = time.time()

        # Acquire a connection from the connection pool
        with self._engine.begin() as conn:
            # Use the 'multi' method to enable bulk copy support
            df.to_sql(
                table_name,
                conn,
                if_exists='replace' if is_first_chunk else 'append',
                index=False,
                method='multi',
                chunksize=100000  # Optimize bulk insert size
            )

        # Calculate the duration of the load operation
        duration = time.time() - start_time

        # Return a dictionary containing the load duration
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
        Verbosely log the results of loading data into a table.

        This method takes a table name and a dictionary of loading statistics
        as input, and logs a formatted message containing the results of the
        load operation. The message includes the total number of rows loaded,
        the total duration of the load operation, the number of rows loaded per
        second, and the number of chunks processed (both total and successful).

        Args:
            table_name: Name of the table being loaded
            stats: Loading statistics dictionary
        """
        # Extract the relevant statistics from the input dictionary
        total_rows_loaded = stats['total_rows_loaded']
        total_duration = stats['total_duration']
        rows_per_second = stats['rows_per_second']
        chunks_total = stats['chunks_total']
        chunks_successful = stats['chunks_successful']
        chunks_failed = stats['chunks_failed']

        # Format the log message
        log_message = (
            f"Data Load Statistics for Table '{table_name}':\n"
            f"  Total Rows Loaded: {total_rows_loaded:,}\n"
            f"  Total Duration: {total_duration:.2f} seconds\n"
            f"  Rows/Second: {rows_per_second:.2f}\n"
            f"  Chunks Total: {chunks_total}\n"
            f"  Chunks Successful: {chunks_successful}\n"
            f"  Chunks Failed: {chunks_failed}"
        )

        # Determine the log level based on the success of the load operation
        log_level = logging.WARNING if chunks_failed > 0 else logging.INFO

        # Log the message at the appropriate level
        self._logger.log(log_level, log_message)