import logging
import requests

class HTTPUtils:

    @staticmethod
    def fetch_json_data(url):
        """
        Fetch JSON data from a given URL.

        Args:
            url (str): The URL to fetch data from.

        Returns:
            dict: Parsed JSON data.
        """
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def download_file(url, destination):
        """
        Download a file from a URL to a specified destination.

        Args:
            url (str): The URL to download the file from.
            destination (str): The path to save the downloaded file.

        Returns:
            str: The path to the downloaded file.
        """
        response = requests.get(url)
        response.raise_for_status()
        with open(destination, "wb") as file:
            file.write(response.content)
        return destination

