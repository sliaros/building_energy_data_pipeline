import logging
import requests
import os
from src.utility.file_utils import FileUtils
from src.utility.http_utils import HTTPUtils

class ZenodoClient:
    BASE_URL = "https://zenodo.org/api/records/"

    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)

    def find_file_url(self, record_id, file_name):
        """
        Find the URL of a file in a Zenodo record.

        Args:
            record_id (str): The ID of the Zenodo record.
            file_name (str): The name of the file to find.

        Returns:
            str: The URL of the file if found, None otherwise.
        """
        url = f"{ZenodoClient.BASE_URL}{record_id}"
        data = HTTPUtils.fetch_json_data(url)
        for file in data.get("files", []):
            if file_name in file["key"]:
                return file["links"]["self"]
        self._logger.error(f"File '{file_name}' not found in Zenodo record {record_id}")
        raise FileNotFoundError(f"File '{file_name}' not found in Zenodo record {record_id}")

    def download_file_from_zenodo(self, record_id, file_name, folder_path):
        """
        Download a file from Zenodo and save it to a specified folder.

        Args:
            record_id (str): The ID of the Zenodo record that contains the file.
            file_name (str): The name of the file to download.
            folder_path (str): The path to the folder where the file will be saved.

        Returns:
            str: The path to the downloaded file, or None if the download failed.
        """
        file_path = os.path.join(folder_path, file_name)

        if FileUtils().file_exists(file_path):
            file_size = FileUtils().get_file_size(file_path)
            self._logger.info(
                f"File '{file_name}' with size '{file_size}' bytes already exists. Skipping download.")
            return file_path

        try:
            self._logger.info(
                f"Downloading file '{file_name}' from Zenodo record {record_id}.")
            file_url = self.find_file_url(record_id, file_name)
            self._logger.info(
                f"File '{file_name}' downloaded and saved to: {folder_path}")
            return HTTPUtils.download_file(file_url, file_path)
        except requests.RequestException as e:
            raise type(e)(f"{type(e).__name__} when downloading file: {e}")