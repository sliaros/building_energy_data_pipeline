import os
import logging
from src.utility.zenodo_client import ZenodoClient
from src.utility.file_utils import FileUtils
from typing import Optional

class DataExtractor:
    def __init__(self, config: dict, logger: Optional[logging.Logger] = None) -> None:
        """
        Initializes the DataExtractor.

        Args:
            config (dict): Configuration dictionary for the extractor.
            logger (Optional[logging.Logger]): Logger instance for logging. A new logger is created if not provided.
        """
        self._config = config
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self._logger.info('Data Extractor initiated')

    def download_data_from_zenodo(self) -> Optional[str]:
        """
        Downloads a file from Zenodo based on the configuration.
        The file is downloaded to the 'raw_data_file_path' location specified in the configuration.
        The file is downloaded from the Zenodo record specified by the 'record_id' in the configuration.
        The file is identified by the 'file_name' specified in the configuration.

        The method returns the path to the downloaded file, or None if an error occurs.
        """
        config: dict = self._config['project_data']

        if not config:
            raise ValueError("No configuration found for data extraction")

        # Create a ZenodoClient to communicate with the Zenodo API
        zenodo_client: ZenodoClient = ZenodoClient()

        # Download the file from Zenodo
        try:
            self._downloaded_file_path: str = zenodo_client.download_file_from_zenodo(
                # The ID of the Zenodo record that contains the file
                config['record_id'],
                # The name of the file to download
                config['file_name'],
                # The path to the folder where the file will be saved
                os.path.normpath(config['raw_data_file_path'])
            )

            # Return the path to the downloaded file
            return self._downloaded_file_path
        except (KeyError, TypeError) as e:
            self._logger.critical(f"Error downloading file from Zenodo: {e}")
            return None

    def unzip_downloaded_data(self):
        # Retrieve the list of folder names to extract from the configuration
        folder_names = self._config['project_data'].get('zip_file_folders_to_extract', [])

        try:
            # Attempt to unzip the downloaded file into the specified folders
            # If folder_names is empty, all files will be extracted
            FileUtils().unzip_folders(self._downloaded_file_path, None, folder_names)
        except Exception as e:
            # Log any exceptions that occur during the unzipping process as critical errors
            self._logger.critical(f"Unhandled Exception: {e}")
            # Return None to indicate that the operation was unsuccessful
            return None
