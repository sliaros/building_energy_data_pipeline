import pytest
import os
import pandas as pd
import yaml
import zipfile
import logging
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
import pyarrow as pa
import pyarrow.parquet as pq
from src.utility.file_utils import FileUtils  # Replace with actual import


class TestFileUtils:
    @pytest.fixture
    def file_utils(self):
        """Create a FileUtils instance with a mock logger"""
        logger = Mock(spec=logging.Logger)
        return FileUtils(logger=logger)

    @pytest.fixture
    def temp_dir(self, tmp_path):
        """Create a temporary directory for file operations"""
        return tmp_path

    @pytest.fixture
    def sample_csv(self, temp_dir):
        """Create a sample CSV file"""
        csv_path = temp_dir / "test.csv"
        df = pd.DataFrame({'col1': range(5), 'col2': range(5)})
        df.to_csv(csv_path, index=False)
        return csv_path

    @pytest.fixture
    def sample_parquet(self, temp_dir):
        """Create a sample Parquet file"""
        parquet_path = temp_dir / "test.parquet"
        df = pd.DataFrame({'col1': range(5), 'col2': range(5)})
        df.to_parquet(parquet_path, index=False)
        return parquet_path

    @pytest.fixture
    def sample_zip(self, temp_dir, sample_csv):
        """Create a sample zip file with test data"""
        zip_path = temp_dir / "test.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.write(sample_csv, "folder1/test.csv")
            zf.write(sample_csv, "folder2/test.csv")
        return zip_path

    def test_create_directory(self, file_utils, temp_dir):
        """Test directory creation"""
        test_dir = temp_dir / "test_dir"
        file_utils.create_directory(test_dir)
        assert test_dir.exists()
        assert test_dir.is_dir()

    def test_create_directory_permission_error(self, file_utils):
        """Test directory creation with permission error"""
        with patch('os.makedirs', side_effect=PermissionError("Permission denied")):
            file_utils.create_directory("/root/test")
            file_utils._logger.error.assert_called_once()

    def test_file_exists(self, file_utils, sample_csv):
        """Test file existence check"""
        assert file_utils.file_exists(sample_csv)
        assert not file_utils.file_exists("nonexistent.file")

    def test_get_file_size(self, file_utils, sample_csv):
        """Test file size retrieval"""
        size = file_utils.get_file_size(sample_csv)
        assert isinstance(size, int)
        assert size > 0

    def test_get_file_size_error(self, file_utils):
        """Test file size retrieval error"""
        size = file_utils.get_file_size("nonexistent.file")
        assert size is None
        file_utils._logger.error.assert_called_once()

    def test_unzip_folders(self, file_utils, temp_dir, sample_zip):
        """Test unzipping folders"""
        target_folder = temp_dir / "extracted"
        file_utils.unzip_folders(sample_zip, target_folder, ["folder1"])

        assert (target_folder / "folder1" / "test.csv").exists()
        assert not (target_folder / "folder2" / "test.csv").exists()

    def test_unzip_folders_bad_zip(self, file_utils, temp_dir):
        """Test unzipping with corrupt zip file"""
        bad_zip = temp_dir / "bad.zip"
        bad_zip.write_bytes(b"not a zip file")

        file_utils.unzip_folders(bad_zip, temp_dir)
        file_utils._logger.error.assert_called_once()

    def test_load_yaml_file(self, file_utils, temp_dir):
        """Test YAML file loading"""
        yaml_content = """
        key1: value1
        key2: value2
        """
        yaml_file = temp_dir / "test.yaml"
        yaml_file.write_text(yaml_content)

        result = file_utils._load_yaml_file(yaml_file)
        assert result=={'key1': 'value1', 'key2': 'value2'}

    def test_define_local_file_path(self, file_utils):
        """Test local file path definition"""
        result = file_utils._define_local_file_path("folder", "file.txt")
        assert isinstance(result, str)
        assert "folder" in result
        assert "file.txt" in result

    def test_ensure_directory_exists(self, temp_dir):
        """Test directory existence ensuring"""
        test_path = temp_dir / "deep" / "nested" / "file.txt"
        FileUtils.ensure_directory_exists(test_path)
        assert test_path.parent.exists()

    def test_save_parquet(self, temp_dir):
        """Test saving DataFrame to Parquet"""
        df = pd.DataFrame({'col1': range(5)})
        output_path = temp_dir / "test.parquet"
        FileUtils.save_parquet(df, output_path)
        assert output_path.exists()

    def test_save_csv(self, temp_dir):
        """Test saving DataFrame to CSV"""
        df = pd.DataFrame({'col1': range(5)})
        output_path = temp_dir / "test.csv"
        FileUtils.save_csv(df, output_path)
        assert output_path.exists()

    def test_csv_to_parquet(self, file_utils, sample_csv, temp_dir):
        """Test CSV to Parquet conversion"""
        output_path = temp_dir / "output.parquet"
        file_utils.csv_to_parquet(sample_csv, output_path)
        assert output_path.exists()

        # Verify content
        original_df = pd.read_csv(sample_csv)
        converted_df = pd.read_parquet(output_path)
        pd.testing.assert_frame_equal(original_df, converted_df)

    def test_csv_to_parquet_in_chunks(self, file_utils, sample_csv, temp_dir):
        """Test CSV to Parquet conversion in chunks"""
        output_path = temp_dir / "output.parquet"
        file_utils.csv_to_parquet_in_chunks(sample_csv, output_path, chunk_size=2)
        assert output_path.exists()

        # Verify content
        original_df = pd.read_csv(sample_csv)
        converted_df = pd.read_parquet(output_path)
        pd.testing.assert_frame_equal(original_df, converted_df)

    def test_find_folders_with_extension(self, file_utils, temp_dir):
        """Test finding folders with specific file extensions"""
        # Create test directory structure
        (temp_dir / "dir1").mkdir()
        (temp_dir / "dir2").mkdir()
        (temp_dir / "dir1" / "test1.csv").touch()
        (temp_dir / "dir2" / "test2.txt").touch()

        result = file_utils.find_folders_with_extension(temp_dir, "csv")
        assert len(result)==1
        assert str(temp_dir / "dir1") in result

    def test_create_directories_from_yaml(self, temp_dir):
        """Test creating directories from YAML"""
        yaml_content = {
            'src': {
                'module1': {},
                'module2': {}
            }
        }

        FileUtils.create_directories_from_yaml(yaml_content, temp_dir)

        assert (temp_dir / "src" / "module1").exists()
        assert (temp_dir / "src" / "module2").exists()
        assert (temp_dir / "src" / "module1" / "__init__.py").exists()


class TestFileReader:
    @pytest.fixture
    def reader(self):
        return FileUtils.FileReader()

    def test_get_file_type_and_reader_parquet(self, reader, temp_dir):
        """Test getting reader for Parquet files"""
        file_path = temp_dir / "test.parquet"
        file_type, reader_func = reader.get_file_type_and_reader(file_path)
        assert file_type=="parquet"
        assert callable(reader_func)

    def test_get_file_type_and_reader_csv(self, reader, temp_dir):
        """Test getting reader for CSV files"""
        file_path = temp_dir / "test.csv"
        file_type, reader_func = reader.get_file_type_and_reader(file_path)
        assert file_type=="csv"
        assert reader_func==pd.read_csv

    def test_get_file_type_and_reader_unsupported(self, reader, temp_dir):
        """Test getting reader for unsupported file type"""
        file_path = temp_dir / "test.xyz"
        with pytest.raises(ValueError):
            reader.get_file_type_and_reader(file_path)

    def test_read_parquet(self, reader, temp_dir):
        """Test reading Parquet file"""
        # Create test Parquet file
        df = pd.DataFrame({'col1': range(10), 'col2': range(10)})
        file_path = temp_dir / "test.parquet"
        df.to_parquet(file_path)

        # Test reading all rows
        result = reader._read_parquet(file_path)
        assert len(result)==10
        pd.testing.assert_frame_equal(result, df)

        # Test reading limited rows
        result = reader._read_parquet(file_path, nrows=5)
        assert len(result)==5
        pd.testing.assert_frame_equal(result, df.head(5))