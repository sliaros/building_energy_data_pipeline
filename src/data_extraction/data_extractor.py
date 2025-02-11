import os
import logging
from src.utility.zenodo_client import ZenodoClient
from src.utility.file_utils import FileUtils

class DataExtractor:
    def __init__(self, config):
        self._config = config
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.info('Data Extractor initiated')

    def _download_data_from_zenodo(self):
        """
        Downloads a file from Zenodo based on the configuration.

        Returns:
            str: The path to the downloaded file, or None if the download failed.
        """
        _record_id = self._config['database_file']['record_id']
        _file_name = self._config['database_file']['file_name']
        _folder_path = os.path.normpath(self._config['database_file']['raw_data_file_path'])

        try:
            _zclient = ZenodoClient()
            downloaded_file_path = _zclient.download_file_from_zenodo( _record_id, _file_name, _folder_path)
            self._downloaded_file_path = downloaded_file_path
        except Exception as e:
            self._logger.critical(f"Error downloading file from Zenodo: {e}")
            return None

    def _unzip_downloaded_data(self):
        try:
            FileUtils().unzip_folders(self._downloaded_file_path, None,
                self._config['database_file']['zip_file_folders_to_extract'])
        except Exception as e:
            self._logger.critical(f"Unhandled Exception: {e}")
            return None
