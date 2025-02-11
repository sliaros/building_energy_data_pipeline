import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from typing import Generator, Union


class FileProcessor:
    """
    Utility class for processing different file types efficiently.
    Supports chunked reading for large files.
    """

    @staticmethod
    def read_file_chunks(
            file_path: Union[str, Path],
            chunk_size: int = 100000,
            file_type: str = None
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Read file in chunks, supporting CSV and Parquet files.

        Args:
            file_path: Path to the file
            chunk_size: Number of rows per chunk
            file_type: Optional file type (inferred if not provided)

        Yields:
            DataFrame chunks
        """
        file_path = Path(file_path)
        file_type = file_type or file_path.suffix.lower().replace('.', '')

        if file_type=='parquet':
            return FileProcessor._read_parquet_chunks(file_path)
        elif file_type=='csv':
            return pd.read_csv(file_path, chunksize=chunk_size)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

    @staticmethod
    def _read_parquet_chunks(file_path: Path):
        """
        Read Parquet file in row group chunks.

        Args:
            file_path: Path to Parquet file

        Yields:
            DataFrame chunks
        """
        parquet_file = pq.ParquetFile(file_path)
        for i in range(parquet_file.num_row_groups):
            yield parquet_file.read_row_group(i).to_pandas()