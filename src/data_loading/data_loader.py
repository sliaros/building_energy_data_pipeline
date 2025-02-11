import psycopg2
from psycopg2.extensions import connection, ISOLATION_LEVEL_AUTOCOMMIT
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import pyarrow.parquet as pq
from typing import Optional, Union, Dict
from pathlib import Path

from sqlalchemy import create_engine

from sqlalchemy.engine.url import URL

from src.data_loading.schema_generator import SchemaAnalyzer, SamplingStrategy, SQLSchemaGenerator

class DataLoader():
    def __init__(self, config, logger, file_utils, max_workers=4,
                 db_type = "stagging",
                 db_params: Optional[dict] = None):
        self._config = config
        self._schemas_folder = Path(self._config['project_data']['schemas_dir_path'])
        self._logger = logger
        self._file_utils = file_utils
        self._logger.info('Data Loader initiated')
        self._max_workers = max_workers

        self._sampling_strategy = SamplingStrategy(
            method="stratified",
            max_rows=50000,
            sampling_ratio=0.005  # 0.5%
        )

        self._db_params = db_params or self._config['staging_database'] if db_type == "stagging" else self._config['database']
        self._sqlalchemy_url = self._postgres_params_to_sqlalchemy_url(self._db_params)

        self._verify_connection(db_params=self._db_params)

        # engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/building_energy_staging_db", echo=True)  # Enables SQL debugging
        # try:
        #     with engine.connect() as conn:
        #         print("Connected successfully!")
        # except Exception as e:
        #     print(f"Connection failed: {e}")

    def _verify_connection(self, db_params: Optional[dict] = None) -> None:
        """Verify database connection and create if doesn't exist"""
        try:
            # Try connecting to the specified database
            with self._get_connection(db_params) as conn:
                self._logger.info("Successfully connected to database")
        except psycopg2.OperationalError as e:
            if "does not exist" in str(e):
                # Connect to default database to create new one
                _temp_params = db_params.copy()
                _temp_params['database'] = 'postgres'
                conn = psycopg2.connect(**_temp_params)

                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

                with conn.cursor() as cur:
                    cur.execute(
                        f"CREATE DATABASE {db_params['database']}"
                        )
                conn.close()

                self._logger.info(f"Created database {db_params['database']}")
            else:
                raise

    def _get_connection(self, db_params: Optional[dict]) -> connection:
        """Create and return a database connection"""
        return psycopg2.connect(**db_params)

    def _generate_schema(
            self,
            file_path: Union[str, Path],
            output_folder: Union[str, Path] = None,
            table_name: Optional[str] = None,
            sampling_strategy: Optional[SamplingStrategy] = None,
            if_exists: str = 'fail'
    ) -> Optional[Dict[str, Union[str, Path]]]:
        """
        Main function to generate a PostgreSQL schema from a data file.

        Args:
            file_path: Path to the input file (CSV or Parquet)
            output_folder: Directory where the SQL schema file will be saved
            table_name: Optional name for the database table
            sampling_strategy: Optional configuration for data sampling

        Returns:
            Path to the generated schema file, or None if generation failed
        """
        try:
            file_path = Path(file_path)

            # Validate input file
            if not file_path.exists():
                raise FileNotFoundError(f"Input file not found: {file_path}")

            self._logger.info(f"Starting schema generation for {file_path}")

            output_folder = Path(output_folder or self._config['project_data']['schemas_dir_path'])
            self._logger.info(f"Schemas' output folder set to {output_folder}")
            output_folder.mkdir(parents=True, exist_ok=True)

            if not table_name:
                table_name = SQLSchemaGenerator._derive_table_name(file_path)

            output_file_path = output_folder / f"{table_name}_schema.sql"

            if output_file_path.exists():
                if if_exists == 'fail':
                    self._logger.info(f"Schema file exists: {output_file_path}")
                    return {f"Table name": table_name,
                            "Sql schema file path": output_file_path}
                elif if_exists == 'replace':
                    self._logger.info(f"Replacing schema file: {output_file_path}")
                else:
                    raise ValueError(f"Invalid value for 'if_exists': {if_exists}")

            # Use provided sampling strategy or create default
            strategy = sampling_strategy or SamplingStrategy()

            # Analyze the schema using sampling
            columns = SchemaAnalyzer(file_path, strategy).analyze()

            # Generate the SQL
            generator = SQLSchemaGenerator(table_name)
            sql_schema = generator.generate_schema(columns, file_path)

            output_file_path.write_text(sql_schema)

            return {f"Table name": table_name,
            "Sql schema file path": output_file_path}

        except Exception as e:
            self._logger.error(f"Failed to generate schema: {str(e)}")
            raise

    def _create_table(self,
                      schema_file: Union[str, Path],
                      table_name: str,
                      if_exists: str = 'fail') -> None:
        """Create a table based on the provided schema file and database connection parameters."""
        try:
            schema_file = Path(schema_file)
            if not schema_file.exists():
                raise FileNotFoundError(f"Schema file not found: {schema_file}")

            self._logger.info(f"Creating table {table_name} in database {self._db_params['database']}")

            with self._get_connection(self._db_params) as conn:
                with open(schema_file, 'r') as f:
                    sql_schema = f.read()
                    with conn.cursor() as cur:
                        if if_exists == 'replace':
                            self._logger.info(f"Dropping table {table_name} from database {self._db_params['database']}")
                            cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                        cur.execute(sql_schema)
                        self._logger.info(f"Successfully created table {table_name} in database {self._db_params['database']}")
        except Exception as e:
            self._logger.error(f"Failed to create table: {str(e)}")
            raise

    def load_data(
            self,
            file_path: Union[str, Path],
            table_name: str,
            chunk_size: int = 100000,
    ) -> None:
        """
        Load data from file into the specified table.

        Args:
            file_path: Path to the data file
            table_name: Name of the target table
            chunk_size: Number of rows to load in each batch
        """
        file_path = Path(file_path)
        self._logger.info(f"Loading data from {file_path} into {table_name}")

        if file_path.suffix.lower()=='.parquet':
            self._load_parquet(file_path, table_name, chunk_size)
        else:
            self._load_csv(file_path, table_name, chunk_size)

    def _load_parquet(
            self,
            file_path: Path,
            table_name: str,
            chunk_size: int,
            max_workers: int = None
    ) -> None:
        """Load data from a Parquet file"""
        # Read Parquet metadata
        parquet_file = pq.ParquetFile(file_path)
        num_row_groups = parquet_file.num_row_groups  # Get the correct number of row groups

        _max_workers = max_workers or self._max_workers

        with ThreadPoolExecutor(max_workers=_max_workers) as executor:
            futures = []

            for i in range(num_row_groups):  # Iterate over row groups
                df = parquet_file.read_row_group(i).to_pandas()  # Read by row group index

                futures.append(
                    executor.submit(
                        self._load_dataframe_chunk,
                        df,
                        table_name,
                        i==0  # Only replace table for the first chunk
                    )
                )

            # Wait for all chunks to complete
            for future in futures:
                future.result()

    def _load_csv(
            self,
            db_params,
            file_path: Path,
            table_name: str,
            chunk_size: int,
            max_workers: int = None
    ) -> None:
        """Load data from a CSV file"""
        _max_workers = max_workers or self._max_workers

        with ThreadPoolExecutor(max_workers=_max_workers) as executor:
            futures = []

            for i, chunk in enumerate(
                    pd.read_csv(file_path, chunksize=chunk_size)
            ):
                futures.append(
                    executor.submit(
                        self._load_dataframe_chunk,
                        chunk,
                        table_name,
                        i==0
                    )
                )

            # Wait for all chunks to complete
            for future in futures:
                future.result()



    def _postgres_params_to_sqlalchemy_url(self, db_params: dict = None) -> URL:
        """
        Converts PostgreSQL connection parameters to an SQLAlchemy connection URL.

        Args:
            params (dict): Dictionary containing PostgreSQL connection parameters.

        Returns:
            str: SQLAlchemy connection string.
        """
        _db_params = db_params or self._db_params

        return URL.create(
            drivername="postgresql+psycopg2",
            username=db_params["user"],
            password=db_params["password"],
            host=db_params["host"],
            port=db_params["port"],
            database=db_params["database"]
        )

    def _load_dataframe_chunk(
            self,
            df: pd.DataFrame,
            table_name: str,
            is_first_chunk: bool,
    ) -> None:
        """Load a chunk of data into the database"""

        _engine = create_engine(self._sqlalchemy_url)

        with _engine.begin() as conn:
            # Use pandas to_sql for the actual loading
            df.to_sql(
                table_name,
                conn,
                if_exists='append' if not is_first_chunk else 'replace',
                index=False,
                method='multi'
            )