import os
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Callable
from src.utility.file_utils import FileUtils
from glob import glob
import logging

class DataTransformer:
    def __init__(self, config):
        self._config = config
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.info('Data Transformer initiated')

    def convert_parquet_to_csv(self,
            parquet_path: str, output_path: str, chunk_size: int = 100000, include_header: bool = True
    ) -> None:
        """
        Converts a Parquet file to CSV by processing in chunks for memory efficiency.
        """
        if not os.path.exists(parquet_path):
            raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

        FileUtils.ensure_directory_exists(output_path)
        parquet_file = pq.ParquetFile(parquet_path)
        total_rows = parquet_file.metadata.num_rows
        stats = {'rows_processed': 0, 'chunks_processed': 0, 'start_time': datetime.now()}

        for chunk_number, chunk in enumerate(parquet_file.iter_batches(batch_size=chunk_size)):
            df_chunk = chunk.to_pandas()
            mode = 'w' if chunk_number==0 else 'a'
            FileUtils.save_csv(df_chunk, output_path, mode=mode, header=include_header if chunk_number==0 else False)
            stats['rows_processed'] += len(df_chunk)
            stats['chunks_processed'] += 1
            if chunk_number % 10==0:
                self._logger.info(f"Progress: {stats['rows_processed']:,}/{total_rows:,} rows")

        self._logger.info(f"Conversion completed: {output_path}")

    def get_parquet_info(self, parquet_path: str) -> None:
        """
        Prints metadata and schema details of a Parquet file.
        """
        try:
            parquet_file = pq.ParquetFile(parquet_path)
            print(f"Parquet File: {parquet_path}\n" + "-" * 50)
            print(f"Rows: {parquet_file.metadata.num_rows:,}")
            print(f"Columns: {len(parquet_file.schema)}")
            print(f"Row Groups: {parquet_file.metadata.num_row_groups}")
            print("\nSchema:")
            for field in parquet_file.schema:
                print(f"{field.name}: {field.physical_type}")
        except Exception as e:
            self._logger.error(f"Error retrieving Parquet info: {str(e)}")

    def _read_csv_in_chunks(self, file_path: str, chunk_size: int):
        """Generator function to read CSV file in chunks."""
        return pd.read_csv(file_path, chunksize=chunk_size)

    def default_process_chunk(self, chunk: pd.DataFrame, meter_type: str) -> pd.DataFrame:
        return chunk

    def normalize_chunk(chunk: pd.DataFrame, meter_type: str) -> pd.DataFrame:
        chunk["normalized_reading"] = (chunk["meter_reading"] - chunk["meter_reading"].min()) / \
                                      (chunk["meter_reading"].max() - chunk["meter_reading"].min())
        return chunk

    def melt_chunk(selfj, chunk: pd.DataFrame, meter_type: str) -> pd.DataFrame:
        """
        Transforms the data using a melt operation.

        - `timestamp` remains as an identifier.
        - Other columns (buildings) are melted into `building_id` and `meter_reading`.
        - A new column `meter` is added with the `meter_type`.

        :param chunk: DataFrame chunk to process.
        :param meter_type: The type of meter (extracted from filename).
        :return: Melted DataFrame.
        """
        melted = chunk.melt(id_vars="timestamp", var_name="building_id", value_name="meter_reading")
        melted["meter"] = meter_type
        return melted

    def _save_chunk_to_parquet(self, chunk: pd.DataFrame, output_path: str, meter_type: str, chunk_num: int) -> str:
        """Saves a processed chunk as a Parquet file."""
        temp_file_name = f"{meter_type}_chunk_{chunk_num}.parquet"
        temp_file = os.path.join(output_path, temp_file_name)
        FileUtils.save_parquet(chunk, temp_file)
        return temp_file

    def _process_file(
            self, file_path: str, chunk_size: int, process_function: Callable, output_path: Optional[str]
    ) -> List[str]:
        """Processes a single CSV file in chunks and saves each chunk as Parquet."""
        meter_type = Path(file_path).stem
        self._logger.info(f"Processing file: {meter_type}")

        temp_files = []
        for chunk_num, chunk in enumerate(self._read_csv_in_chunks(file_path, chunk_size), start=1):
            processed_chunk = process_function(chunk, meter_type)

            if output_path:
                temp_file = self._save_chunk_to_parquet(processed_chunk, output_path, meter_type, chunk_num)
                temp_files.append(temp_file)

            if chunk_num % 10==0:
                self._logger.info(f"Processed {chunk_num} chunks")

        return temp_files

    def _merge_parquet_files(self, temp_files: List[str], output_path: str) -> None:
        """Combines all chunked Parquet files into one and removes temporary files."""
        combined_data = pd.concat([pd.read_parquet(f) for f in temp_files], ignore_index=True)
        final_parquet_path = os.path.join(output_path, f"{os.path.basename(os.path.normpath(output_path))}.parquet")
        FileUtils.save_parquet(combined_data, final_parquet_path)

        # Remove temporary files
        for temp_file in temp_files:
            os.remove(temp_file)

        self._logger.info(f"Final data saved to {final_parquet_path}")

    def _process_meter_data(
            self,
            file_paths: Optional[List[str]] = None,
            chunk_size: int = 10_000,
            process_function: Optional[Callable] = None,
            output_path: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        Generalized method to process CSV meter data using a custom processing function.

        - `process_function`: Callable that transforms a chunk of data (default: melting).
        - `output_path`: If provided, saves processed data to Parquet, else returns DataFrame.
        """
        if output_path is None:
            output_path = os.path.normpath(
                os.path.join(self._config['database_file']['raw_data_file_path'], 'raw')
            )

        process_function = process_function or self.default_process_chunk
        temp_files = []

        try:
            if file_paths is None:
                file_paths = glob(os.path.join(output_path, '*.csv'))

            for file_path in file_paths:
                temp_files.extend(self._process_file(file_path, chunk_size, process_function, output_path))

            if output_path and temp_files:
                self._merge_parquet_files(temp_files, output_path)
                return None

        except Exception as e:
            self._logger.error(f"Error processing meter data: {str(e)}")
            for temp_file in temp_files:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            raise

    def process_and_convert_to_parquet_in_chunks(self,
                                                 root_path = "data_sources",
                                                 file_extension = "csv"):
        for folder, files in FileUtils().find_folders_with_extension(root_path, file_extension).items():
            if any(["metadata" in folder, 'weather' in folder]):
                process_function = None
            else:
                process_function = self.melt_chunk
            self._process_meter_data(
                file_paths=files,
                chunk_size=50_000,
                process_function=process_function,
                output_path=folder)



