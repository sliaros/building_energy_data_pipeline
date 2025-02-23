import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path
from src.data_loading.data_loader import BaseDataLoader
from typing import Union

class TestDataLoader(unittest.TestCase):

    def setUp(self):
        self.data_loader = BaseDataLoader()

    @patch('BaseDataLoader.load_data')
    def test_file_path_as_string(self, mock_load_data):
        file_path = 'path/to/file.csv'
        table_name = 'my_table'
        chunk_size = 100000
        self.data_loader.load_data(file_path, table_name, chunk_size)
        mock_load_data.assert_called_once_with(file_path, table_name, chunk_size)

    @patch('BaseDataLoader.load_data')
    def test_file_path_as_path_object(self, mock_load_data):
        file_path = Path('path/to/file.csv')
        table_name = 'my_table'
        chunk_size = 100000
        self.data_loader.load_data(file_path, table_name, chunk_size)
        mock_load_data.assert_called_once_with(file_path, table_name, chunk_size)

    @patch('BaseDataLoader.load_data')
    def test_table_name_as_string(self, mock_load_data):
        file_path = 'path/to/file.csv'
        table_name = 'my_table'
        chunk_size = 100000
        self.data_loader.load_data(file_path, table_name, chunk_size)
        mock_load_data.assert_called_once_with(file_path, table_name, chunk_size)

    @patch('BaseDataLoader.load_data')
    def test_chunk_size_as_integer(self, mock_load_data):
        file_path = 'path/to/file.csv'
        table_name = 'my_table'
        chunk_size = 100000
        self.data_loader.load_data(file_path, table_name, chunk_size)
        mock_load_data.assert_called_once_with(file_path, table_name, chunk_size)

    @patch('BaseDataLoader.load_data')
    def test_chunk_size_with_default_value(self, mock_load_data):
        file_path = 'path/to/file.csv'
        table_name = 'my_table'
        self.data_loader.load_data(file_path, table_name)
        mock_load_data.assert_called_once_with(file_path, table_name, 100000)

    def test_invalid_file_path_type(self):
        file_path = 123  # invalid type
        table_name = 'my_table'
        chunk_size = 100000
        with self.assertRaises(TypeError):
            self.data_loader.load_data(file_path, table_name, chunk_size)

    def test_invalid_table_name_type(self):
        file_path = 'path/to/file.csv'
        table_name = 123  # invalid type
        chunk_size = 100000
        with self.assertRaises(TypeError):
            self.data_loader.load_data(file_path, table_name, chunk_size)

    def test_invalid_chunk_size_type(self):
        file_path = 'path/to/file.csv'
        table_name = 'my_table'
        chunk_size = 'abc'  # invalid type
        with self.assertRaises(TypeError):
            self.data_loader.load_data(file_path, table_name, chunk_size)

if __name__ == '__main__':
    unittest.main()