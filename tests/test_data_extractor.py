import unittest
from unittest.mock import Mock, patch
from src.data_extraction.data_extractor import DataExtractor
from src.utility.zenodo_client import ZenodoClient

class TestDataExtractor(unittest.TestCase):
    def setUp(self):
        self.data_extractor = DataExtractor({'project_data': {}}, logger=Mock())

    def test_successful_download(self):
        config = {
            'project_data': {
                'record_id': '12345',
                'file_name': 'example.txt',
                'raw_data_file_path': '/path/to/raw/data'
            }
        }
        self.data_extractor._config = config
        zenodo_client = ZenodoClient()
        zenodo_client.download_file_from_zenodo = Mock(return_value='/path/to/downloaded/file')
        self.assertEqual(self.data_extractor.download_data_from_zenodo(), '/path/to/downloaded/file')

    def test_invalid_configuration_missing_project_data(self):
        config = {}
        self.data_extractor._config = config
        with self.assertRaises(ValueError):
            self.data_extractor.download_data_from_zenodo()

    def test_invalid_configuration_missing_record_id_or_file_name(self):
        config = {
            'project_data': {
                'raw_data_file_path': '/path/to/raw/data'
            }
        }
        self.data_extractor._config = config
        with self.assertRaises(KeyError):
            self.data_extractor.download_data_from_zenodo()

    def test_invalid_configuration_invalid_raw_data_file_path(self):
        config = {
            'project_data': {
                'record_id': '12345',
                'file_name': 'example.txt',
                'raw_data_file_path': ' invalid/path'
            }
        }
        self.data_extractor._config = config
        with self.assertRaises(TypeError):
            self.data_extractor.download_data_from_zenodo()

    @patch('src.utility.zenodo_client.ZenodoClient.download_file_from_zenodo')
    def test_download_failure_with_zenodo_api_error(self, mock_download_file_from_zenodo):
        config = {
            'project_data': {
                'record_id': '12345',
                'file_name': 'example.txt',
                'raw_data_file_path': '/path/to/raw/data'
            }
        }
        self.data_extractor._config = config
        mock_download_file_from_zenodo.side_effect = KeyError('Test error')
        self.assertIsNone(self.data_extractor.download_data_from_zenodo())
        self.data_extractor._logger.critical.assert_called_once()