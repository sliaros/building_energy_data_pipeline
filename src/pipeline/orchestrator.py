import os
import yaml
import logging
from typing import Dict, Any
from src.data_extraction.data_extractor import DataExtractor
from src.data_loading.postgres_loader import PostgresDataLoader
from src.data_transformation.data_transformer import DataTransformer
from logs.logging_config import setup_logging
from src.utility.file_utils import FileUtils
from src.schema_generator.schema_analysis_orchestrator import SchemaAnalysisManager
import pandas as pd

class Orchestrator:

    def __init__(self):
        """
        Initializes the Orchestrator object.

        :param config_path: The path to a YAML configuration file
        """

        self._project_file_structure, self._config = (
            FileUtils()._load_yaml_file(
                FileUtils()._define_local_file_path(
                    'config', _config_file
                )
            ) for _config_file in ['project_structure_config.yaml', 'app_config.yaml']
        )

        self._create_directories_from_yaml(self._project_file_structure)

        setup_logging(log_file=self._config['logging']['log_file_path'])
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.info('Orchestrator started')

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
