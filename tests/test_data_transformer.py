import pytest
import pandas as pd
import pyarrow.parquet as pq
import os
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from src.data_transformation.data_transformer import DataTransformer


@pytest.fixture
def sample_config():
    return {
        'database_file': {
            'raw_data_file_path': 'test_data/raw'
        }
    }


@pytest.fixture
def transformer(sample_config):
    return DataTransformer(sample_config)


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'timestamp': ['2024-01-01', '2024-01-02'],
        'building_id1': [100, 200],
        'building_id2': [150, 250]
    })


@pytest.fixture
def sample_parquet(tmp_path, sample_df):
    parquet_path = tmp_path / "test.parquet"
    sample_df.to_parquet(parquet_path)
    return str(parquet_path)


class TestDataTransformer:
    def test_init(self, sample_config):
        """Test initialization of DataTransformer"""
        transformer = DataTransformer(sample_config)
        assert transformer.config==sample_config
        assert transformer._logger is not None

    def test_convert_parquet_to_csv(self, transformer, tmp_path, sample_parquet):
        """Test parquet to CSV conversion"""
        output_path = str(tmp_path / "output.csv")

        transformer.convert_parquet_to_csv(sample_parquet, output_path)

        assert os.path.exists(output_path)
        result_df = pd.read_csv(output_path)
        original_df = pd.read_parquet(sample_parquet)
        pd.testing.assert_frame_equal(result_df, original_df)

    def test_convert_parquet_to_csv_file_not_found(self, transformer, tmp_path):
        """Test handling of non-existent parquet file"""
        with pytest.raises(FileNotFoundError):
            transformer.convert_parquet_to_csv(
                "nonexistent.parquet",
                str(tmp_path / "output.csv")
            )

    def test_get_parquet_info(self, transformer, sample_parquet, capsys):
        """Test parquet info retrieval"""
        transformer.get_parquet_info(sample_parquet)
        captured = capsys.readouterr()

        assert "Parquet File:" in captured.out
        assert "Rows:" in captured.out
        assert "Columns:" in captured.out
        assert "Schema:" in captured.out

    def test_default_process_chunk(self, transformer, sample_df):
        """Test default chunk processing"""
        result = transformer.default_process_chunk(sample_df, "test_meter")
        pd.testing.assert_frame_equal(result, sample_df)

    def test_normalize_chunk(self, sample_df):
        """Test chunk normalization"""
        sample_df['meter_reading'] = [10, 20]
        result = DataTransformer.normalize_chunk(sample_df, "test_meter")

        assert 'normalized_reading' in result.columns
        assert result['normalized_reading'].min()==0
        assert result['normalized_reading'].max()==1

    def test_melt_chunk(self, transformer, sample_df):
        """Test chunk melting transformation"""
        result = transformer.melt_chunk(sample_df, "electricity")

        assert set(result.columns)=={'timestamp', 'building_id', 'meter_reading', 'meter'}
        assert result['meter'].unique()==['electricity']
        assert len(result)==len(sample_df) * 2  # 2 building_id columns

    @pytest.mark.parametrize("chunk_size", [1, 2])
    def test_read_csv_in_chunks(self, transformer, tmp_path, chunk_size):
        """Test CSV chunk reading with different chunk sizes"""
        # Create test CSV
        df = pd.DataFrame({'col1': range(4), 'col2': range(4)})
        csv_path = tmp_path / "test.csv"
        df.to_csv(csv_path, index=False)

        chunks = list(transformer._read_csv_in_chunks(str(csv_path), chunk_size))
        assert len(chunks)==(4 // chunk_size) + (1 if 4 % chunk_size else 0)

        # Verify total rows read matches original
        total_rows = sum(len(chunk) for chunk in chunks)
        assert total_rows==4

    def test_save_chunk_to_parquet(self, transformer, sample_df, tmp_path):
        """Test saving chunk to parquet"""
        output_path = str(tmp_path)
        result_path = transformer._save_chunk_to_parquet(
            sample_df,
            output_path,
            "test_meter",
            1
        )

        assert os.path.exists(result_path)
        saved_df = pd.read_parquet(result_path)
        pd.testing.assert_frame_equal(saved_df, sample_df)

    def test_merge_parquet_files(self, transformer, tmp_path):
        """Test merging multiple parquet files"""
        # Create test parquet files
        df1 = pd.DataFrame({'col1': [1, 2]})
        df2 = pd.DataFrame({'col1': [3, 4]})

        file1 = tmp_path / "chunk_1.parquet"
        file2 = tmp_path / "chunk_2.parquet"
        df1.to_parquet(file1)
        df2.to_parquet(file2)

        temp_files = [str(file1), str(file2)]
        output_path = str(tmp_path)

        transformer._merge_parquet_files(temp_files, output_path)

        # Check if temporary files were removed
        assert not os.path.exists(file1)
        assert not os.path.exists(file2)

        # Check if merged file exists and contains all data
        merged_file = tmp_path / f"{tmp_path.name}.parquet"
        assert merged_file.exists()

        merged_df = pd.read_parquet(merged_file)
        assert len(merged_df)==len(df1) + len(df2)

    @patch('glob.glob')
    def test_process_meter_data(self, mock_glob, transformer, tmp_path, sample_df):
        """Test complete meter data processing"""
        # Setup mock files
        csv_path = str(tmp_path / "test.csv")
        sample_df.to_csv(csv_path, index=False)
        mock_glob.return_value = [csv_path]

        # Process data
        transformer._process_meter_data(
            output_path=str(tmp_path),
            chunk_size=1000
        )

        # Check if final parquet file exists
        final_file = tmp_path / f"{tmp_path.name}.parquet"
        assert final_file.exists()

    @patch('src.data_transformer.FileUtils')
    def test_process_and_convert_to_parquet_in_chunks(self, mock_file_utils, transformer):
        """Test processing and converting files to parquet in chunks"""
        # Mock FileUtils.find_folders_with_extension
        mock_file_utils.return_value.find_folders_with_extension.return_value = {
            'data_sources/metadata': ['file1.csv'],
            'data_sources/meter_data': ['file2.csv']
        }

        # Mock _process_meter_data
        with patch.object(transformer, '_process_meter_data') as mock_process:
            transformer.process_and_convert_to_parquet_in_chunks()

            # Verify _process_meter_data was called twice with correct arguments
            assert mock_process.call_count==2

            # Verify process function selection based on folder name
            calls = mock_process.call_args_list
            assert calls[0][1]['process_function'] is None  # metadata folder
            assert calls[1][1]['process_function']==transformer.melt_chunk  # meter_data folder

    def test_error_handling(self, transformer, tmp_path):
        """Test error handling in processing"""
        with pytest.raises(Exception):
            transformer._process_meter_data(
                file_paths=["nonexistent.csv"],
                output_path=str(tmp_path)
            )