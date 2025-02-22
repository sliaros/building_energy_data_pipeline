import os
import yaml
import logging
from typing import Dict, Any
from src.data_extraction.data_extractor import DataExtractor
from src.data_loading.data_loader import PostgresDataLoader
from src.data_transformation.data_transformer import DataTransformer
from src.logging_configuration.logging_config import setup_logging
from src.utility.file_utils import FileUtils
from src.schema_generator.schema_analysis_orchestrator import SchemaAnalysisManager
from src.postgres_managing.postgres_manager import PostgresManager, DatabaseConfig
from src.configuration_managing.config_manager import ConfigManager
import pandas as pd
import json
import atexit

class Orchestrator:

    def __init__(self, database_name:str = None):
        """
        Initializes the Orchestrator object.

        :param config_path: The path to a YAML configuration file
        """

        """Initializes the Orchestrator with configurations from ConfigManager."""
        self.config_manager = ConfigManager(
            ["project_structure_config.yaml", "app_config.yaml"],
        "./config")
        self.config = self.config_manager.config  # Store config for easy access
        self.config_manager.validate_config()
        self._create_directories_from_yaml(self.config.get("project_structure", {}))

        setup_logging(log_file=self.config_manager.get("logging.log_file_path"))
        self._logger = logging.getLogger(self.__class__.__name__)

        self._logger.info("Orchestrator started")

        if not database_name:
            self._logger.info("No database selected, reverting to default database")

        db_config = DatabaseConfig(**self.config_manager.get(database_name, default="default_database"))
        default_db_config = DatabaseConfig(**self.config_manager.get(None, default="default_database"))

        self.db_manager = PostgresManager(db_config, default_db_config)

        atexit.register(self.cleanup)

    def load_config(self, config_files: list[str] = []):
        """Reloads the configuration if needed."""
        self.config_manager._load_configs(config_files)
        self.config_manager.validate_config()
        self._logger.info("Configuration reloaded successfully")

    @staticmethod
    def _create_directories_from_yaml(yaml_content, base_path='.'):
        """
        Create directories as specified in a YAML content string and add an __init__.py.py file to each.

        Args:
            yaml_content (str): YAML string defining the directory structure.
            base_path (str): The base directory where the structure will be created. Defaults to the current directory.
        """

        def _create_dirs(structure, current_path):
            if isinstance(structure, dict):
                for key, value in structure.items():
                    new_path = os.path.join(current_path, key)
                    os.makedirs(new_path, exist_ok=True)
                    if "src" in new_path:
                        init_file_path = os.path.join(new_path, '__init__.py')
                        if not os.path.exists(init_file_path):
                            with open(init_file_path, 'w') as init_file:
                                init_file.write('# This file makes this directory a Python package\n')

                    _create_dirs(value, new_path)
            # else:
            #     print(f"Expected a dictionary but got {type(structure).__name__} at {current_path}")

        try:
            _create_dirs(yaml_content, base_path)
        except yaml.YAMLError as exc:
            print(f"Error parsing YAML content: {exc}")

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            config = FileUtils()._load_yaml_file("./config/app_config.yaml")
            self._logger.info("Successfully loaded pipeline configuration")
            return config
        except Exception as e:
            self._logger.error(f"Failed to load configuration: {e}")
            raise

    def retrieve_data(self):
        _data_retriever = DataExtractor(self._config)
        _data_retriever._download_data_from_zenodo()
        _data_retriever._unzip_downloaded_data()

    def transform_data(self):
        DataTransformer(self._config, self._logger, FileUtils()).process_and_convert_to_parquet_in_chunks()

    def read_parquet_info(self):
        for folder, files in FileUtils().find_folders_with_extension('data_sources','parquet').items():
            for _file in files:
                DataTransformer(self._config, self._logger, FileUtils()).get_parquet_info(_file)
                print(pd.read_parquet(_file).head())

    def load_data(self):

        loader = PostgresDataLoader(self._config, self._logger, FileUtils())
        schema_manager = SchemaAnalysisManager(config=self._config, logger=self._logger)

        for folder, files in FileUtils().find_folders_with_extension('data_sources','parquet').items():
            for _file in files:

                # Generate schema with custom table name and output location
                _result = schema_manager.generate_schema(
                    file_path=_file,
                    table_name=None,
                    output_folder=None,
                    if_exists="fail"
                )

                _table_name, _scema_file = _result['table_name'], _result['schema_file_path']

                loader.create_table(_scema_file,
                    _table_name,
                if_exists='fail')

                loader.load_data(
                    _file,
                    _table_name,
                    chunk_size=500000,
                    unique_columns=self._config['project_data']['unique_columns'][_table_name]
                )

    def return_active_sessions(self, filters=None):
        """
        Retrieve active sessions with optional filtering and return JSON.

        Args:
            filters (dict, optional): Filtering conditions (e.g., {"datname": "db_name", "state": "active"}).

        Returns:
            str: JSON string of active sessions.
        """
        sessions = self.db_manager.get_active_sessions(filters)
        return json.dumps(sessions, default=str, indent=4)

    def terminate_sessions(self, datname, state=None):
        """
        Terminate all active sessions for a given database.

        Args:
            datname (str): Database name.
            state (str, optional): Session state to filter by (e.g., "active", "idle").
        """
        filters = {"datname": datname}
        if state:
            filters["state"] = state

        sessions = self.db_manager.get_active_sessions(filters)

        for session in sessions:
            self.db_manager.terminate_session_by_pid(session["pid"])

    def delete_database(self, database_name: str, force: bool = True) -> None:
        """Drop a PostgreSQL database.

        Args:
            database_name: The name of the database to delete.
            force: If True, attempts to reconnect and retry deletion if the database is currently open.

        Raises:
            ValueError: If attempting to delete the default database.
        """

        if database_name==self.db_manager.default_db_config.database:
            raise ValueError("Cannot delete the default database.")
        else:
            try:
                self.db_manager.drop_database(database_name)
            except Exception as e:
                if "by other users" or "currently open" in str(e) and force:
                    self._logger.info("Attempting to cleanup and retry deletion...")
                    self.cleanup()
                    self.db_manager.drop_database(database_name)
                elif force and "currently open" in str(e):
                    self.cleanup()
                    # Reconnect with the default database configuration and retry deletion
                    self.db_manager.drop_database(database_name)

    def cleanup(self):
        """Closes all PostgreSQL connections before exiting"""
        self.db_manager.close_all_connections()