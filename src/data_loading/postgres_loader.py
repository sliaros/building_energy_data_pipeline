import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from concurrent.futures import ThreadPoolExecutor

from typing import Optional, Dict, Union
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

        self._sampling_strategy = SamplingStrategy(
            method="stratified",
            max_rows=50000,
            sampling_ratio=0.005
        )

        self._db_params = self._get_db_params(db_type, db_params)
        self._sqlalchemy_url = self._postgres_params_to_sqlalchemy_url()

        self._verify_connection()

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

    def load_data(
            self,
            file_path: Union[str, Path],
            table_name: str,
            chunk_size: int = 100000
    ) -> None:
        """
        Load data into PostgreSQL database in chunks.

        Args:
            file_path: Path to source data file
            table_name: Target database table name
            chunk_size: Number of rows per batch
        """
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures = []

            for i, chunk in enumerate(FileProcessor.read_file_chunks(file_path, chunk_size)):
                futures.append(
                    executor.submit(
                        self._load_dataframe_chunk,
                        chunk,
                        table_name,
                        i==0  # First chunk replaces table
                    )
                )

            # Wait for all chunks to complete
            for future in futures:
                future.result()

    def _load_dataframe_chunk(
            self,
            df,
            table_name: str,
            is_first_chunk: bool
    ) -> None:
        """
        Load a single DataFrame chunk into the database.

        Args:
            df: DataFrame chunk
            table_name: Target table name
            is_first_chunk: Whether this is the first chunk (replace vs append)
        """
        engine = create_engine(self._sqlalchemy_url)

        with engine.begin() as conn:
            df.to_sql(
                table_name,
                conn,
                if_exists='replace' if is_first_chunk else 'append',
                index=False,
                method='multi'
            )

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
            output_folder: Optional[Union[str, Path]] = None
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
        try:
            output_folder = output_folder or self._config['project_data']['schemas_dir_path']
            table_name = table_name or SQLSchemaGenerator._derive_table_name(file_path)

            columns = SchemaAnalyzer(file_path, self._sampling_strategy).analyze()
            generator = SQLSchemaGenerator(table_name)

            sql_schema = generator.generate_schema(columns, file_path)
            schema_path = Path(output_folder) / f"{table_name}_schema.sql"

            schema_path.write_text(sql_schema)

            return {
                "table_name": table_name,
                "schema_file_path": schema_path
            }

        except Exception as e:
            self._logger.error(f"Schema generation failed: {e}")
            raise