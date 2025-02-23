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
        # Store the configuration dictionary provided during initialization
        self.config = config

        # Create a logger specific to this class to handle logging
        # The logger is named after the class name for better traceability
        self._logger = logging.getLogger(self.__class__.__name__)

        # Log an informational message indicating that the Data Transformer has been initiated
        self._logger.info('Data Transformer initiated')

    def convert_parquet_to_csv(
        self, parquet_path: str, output_path: str, chunk_size: int = 100000, include_header: bool = True
    ) -> None:
        """
        Converts a Parquet file to CSV by processing in chunks for memory efficiency.

        :param parquet_path: Path to the input Parquet file
        :param output_path: Path to the output CSV file
        :param chunk_size: Number of rows to process in each chunk (default: 100,000)
        :param include_header: Whether to include the column names in the output CSV (default: True)
        """
        self._logger.info('Converting Parquet to CSV')

        # Check if the Parquet file exists
        if not Path(parquet_path).exists():
            raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

        # Ensure the output directory exists
        FileUtils.ensure_directory_exists(output_path)
        self._logger.info(f'Output directory created: {output_path}')

        # Open the Parquet file
        parquet_file = pq.ParquetFile(parquet_path)
        self._logger.info(f'Opened Parquet file: {parquet_path}')

        # Get the total number of rows in the Parquet file
        total_rows = parquet_file.metadata.num_rows
        self._logger.info(f'Total rows in Parquet file: {total_rows:,}')

        # Initialize the statistics dictionary to keep track of progress
        stats = {"rows_processed": 0, "chunks_processed": 0, "start_time": datetime.now()}
        self._logger.info('Initializing statistics dictionary')

        # Open the output CSV file in write mode (or append mode if include_header is False)
        with open(output_path, "w" if include_header else "a") as csvfile:
            self._logger.info(f'Opened output CSV file: {output_path}')

            # Iterate over the Parquet file in chunks
            for chunk_number, chunk in enumerate(parquet_file.iter_batches(batch_size=chunk_size)):
                self._logger.info(f'Processing chunk {chunk_number+1}')

                # Convert the Parquet chunk to a Pandas DataFrame
                df_chunk = chunk.to_pandas()
                self._logger.info(f'Converted Parquet chunk to Pandas DataFrame')

                # Write the DataFrame to the CSV file
                df_chunk.to_csv(csvfile, mode="a", header=include_header and chunk_number == 0, index=False)
                self._logger.info(f'Written DataFrame to CSV file')

                # Update the statistics dictionary
                stats["rows_processed"] += len(df_chunk)
                stats["chunks_processed"] += 1
                self._logger.info(f'Updated statistics dictionary')

                # Log the progress every 10 chunks
                if chunk_number % 10 == 0:
                    self._logger.info(
                        f'Progress: {stats["rows_processed"]:,}/{total_rows:,} rows'
                    )

        # Log the completion of the conversion
        self._logger.info(f'Conversion completed: {output_path}')

    def get_parquet_info(self, parquet_path: str) -> None:
        """
        Prints metadata and schema details of a Parquet file.
        This method is useful for understanding the structure of a Parquet file.
        """
        try:
            # Open the Parquet file using the ParquetFile class
            with pq.ParquetFile(parquet_path) as parquet_file:
                # Print the file name and a separator
                print(f"Parquet File: {parquet_path}\n" + "-" * 50)

                # Print the number of rows in the Parquet file
                # This is obtained from the metadata.num_rows attribute
                print(f"Rows: {parquet_file.metadata.num_rows:,}")

                # Print the number of columns in the Parquet file
                # This is obtained from the length of the schema attribute, which is a list of fields
                print(f"Columns: {len(parquet_file.schema)}")

                # Print the number of row groups in the Parquet file
                # A row group is a logical grouping of rows in the Parquet file
                # See https://parquet.apache.org/docs/file-format/ for more information
                print(f"Row Groups: {parquet_file.metadata.num_row_groups}")

                # Print the schema of the Parquet file
                # This is a list of fields, each of which has a name, physical type, and other attributes
                print("\nSchema:")
                for field in parquet_file.schema:
                    # Print the name, physical type, and other attributes of the field
                    print(f"{field.name}: {field.physical_type}")

        except Exception as e:
            # Log any exceptions that occur during the execution of this method
            self._logger.error(f"Error retrieving Parquet info: {str(e)}", exc_info=True)

    def _read_csv_in_chunks(self, file_path: str, chunk_size: int):
        """Generator function to read CSV file in chunks."""
        return pd.read_csv(file_path, chunksize=chunk_size)

    def default_process_chunk(self, chunk: pd.DataFrame, meter_type: str) -> pd.DataFrame:
        """
        This is the default method used to process a DataFrame chunk.
        It simply returns the chunk unchanged.

        This method is used by the _process_csv_file method to process
        each chunk of the CSV file. The method takes two parameters:
            - chunk: The DataFrame chunk to be processed.
            - meter_type: The type of meter being processed.

        The method returns the processed DataFrame.
        """
        return chunk

    def normalize_chunk(chunk: pd.DataFrame, meter_type: str) -> pd.DataFrame:
        """
        Normalize the `meter_reading` column of the given DataFrame chunk.

        The normalization is done by subtracting the minimum value from the column,
        then dividing by the range of the values in the column (i.e., the difference
        between the maximum and minimum values). This maps the original values to
        the range [0, 1].

        This is done to prepare the data for training a machine learning model,
        which often expects the input data to be in a certain range.
        """
        # Calculate the minimum value of the `meter_reading` column
        min_value = chunk["meter_reading"].min()

        # Calculate the maximum value of the `meter_reading` column
        max_value = chunk["meter_reading"].max()

        # Calculate the range of the values in the `meter_reading` column
        range_value = max_value - min_value

        # Normalize the values in the `meter_reading` column
        normalized_values = (chunk["meter_reading"] - min_value) / range_value

        # Assign the normalized values to a new column in the DataFrame
        chunk["normalized_reading"] = normalized_values

        # Return the DataFrame with the normalized values
        return chunk

    def melt_chunk(self, chunk: pd.DataFrame, meter_type: str) -> pd.DataFrame:
        """
        Transforms the data using a melt operation.

        The melt operation is a type of data transformation that takes a DataFrame
        with columns of different variables (e.g. building_id1, building_id2, etc.)
        and transforms it into a DataFrame with a single column for the variable
        name (e.g. building_id) and a single column for the corresponding value
        (e.g. meter_reading).

        In this case, we melt the DataFrame to create a new DataFrame with the
        following columns:

        - `timestamp`: the original timestamp column, which is preserved as an
          identifier.
        - `building_id`: a new column created by melting the original building_id
          columns into a single column.
        - `meter_reading`: a new column created by melting the original meter_reading
          columns into a single column.
        - `meter`: a new column created by assigning the `meter_type` parameter to
          each row of the DataFrame.

        :param chunk: DataFrame chunk to process.
        :param meter_type: The type of meter (extracted from filename).
        :return: Melted DataFrame.
        """
        # Create a new DataFrame by melting the original DataFrame
        melted = pd.melt(
            # The DataFrame to melt
            chunk,
            # The columns to preserve as identifiers
            id_vars=["timestamp"],
            # The name of the new column for the variable name
            var_name="building_id",
            # The name of the new column for the value
            value_name="meter_reading"
        )

        # Add a new column to the melted DataFrame with the meter type
        melted["meter"] = meter_type

        # Return the melted DataFrame
        return melted

    def _save_chunk_to_parquet(self, chunk: pd.DataFrame, output_path: str, meter_type: str, chunk_num: int) -> str:
        """
        Saves a processed chunk as a Parquet file.

        This method is called by the _process_file method for each chunk of the CSV file.
        It takes the processed chunk, the output path, the meter type, and the chunk number as input.
        It then saves the chunk as a Parquet file in the output path, with a file name that includes the
        meter type and the chunk number. The method returns the path to the saved file.

        :param chunk: The processed chunk to be saved.
        :param output_path: The path to save the file in.
        :param meter_type: The type of meter being processed (extracted from filename).
        :param chunk_num: The number of the chunk (used in the filename).
        :return: The path to the saved file.
        """
        # Create a file name that includes the meter type and the chunk number
        temp_file_name = f"{meter_type}_chunk_{chunk_num}.parquet"

        # Create the full path to the output file
        temp_file = os.path.join(output_path, temp_file_name)

        # Save the chunk to the output file using the FileUtils.save_parquet method
        FileUtils.save_parquet(chunk, temp_file)

        # Return the path to the saved file
        return temp_file

    def _process_file(
            self, file_path: str, chunk_size: int, process_function: Callable, output_path: Optional[str]
    ) -> List[str]:
        """
        Processes a single CSV file in chunks and saves each chunk as Parquet.

        This method takes a file path, a chunk size, a process function, and an output path as input.
        It reads the CSV file in chunks, processes each chunk using the provided process function,
        and saves the processed chunk as a Parquet file in the specified output path.

        The method returns a list of temporary file paths that were created during processing.
        These temporary files are removed after all chunks have been processed.

        :param file_path: The path to the CSV file to be processed.
        :param chunk_size: The number of rows to read into each chunk.
        :param process_function: The function to be used to process each chunk.
        :param output_path: The path to save the processed chunks in.
        :return: A list of temporary file paths that were created during processing.
        """
        # Get the meter type from the file name
        meter_type = Path(file_path).stem
        self._logger.info(f"Processing file: {meter_type}")

        # Initialize an empty list to store the temporary file paths
        temp_files = []

        # Iterate over each chunk of the CSV file
        for chunk_num, chunk in enumerate(self._read_csv_in_chunks(file_path, chunk_size), start=1):
            # Process the chunk using the provided process function
            processed_chunk = process_function(chunk, meter_type)

            # If an output path is specified, save the processed chunk as a Parquet file
            if output_path:
                # Create a file name that includes the meter type and the chunk number
                temp_file_name = f"{meter_type}_chunk_{chunk_num}.parquet"

                # Create the full path to the output file
                temp_file = os.path.join(output_path, temp_file_name)

                # Save the chunk to the output file using the FileUtils.save_parquet method
                FileUtils.save_parquet(processed_chunk, temp_file)

                # Add the path to the temporary file to the list of temporary file paths
                temp_files.append(temp_file)

            # Log a message every 10 chunks
            if chunk_num % 10 == 0:
                self._logger.info(f"Processed {chunk_num} chunks")

        # Return the list of temporary file paths
        return temp_files

    def _merge_parquet_files(self, temp_files: List[str], output_path: str) -> None:
        """
        Combines all chunked Parquet files into one consolidated Parquet file and removes the temporary files.

        :param temp_files: List of file paths to the temporary Parquet files generated during processing.
        :param output_path: Directory path where the final consolidated Parquet file will be saved.
        """

        # Read each temporary Parquet file into a Pandas DataFrame and concatenate them into a single DataFrame
        combined_data = pd.concat([pd.read_parquet(f) for f in temp_files], ignore_index=True)

        # Construct the final Parquet file path using the output directory name as the file name
        final_parquet_path = os.path.join(
            output_path, f"{os.path.basename(os.path.normpath(output_path))}.parquet"
        )

        # Save the combined DataFrame as a single Parquet file using FileUtils
        FileUtils.save_parquet(combined_data, final_parquet_path)

        # Iterate over the list of temporary files and remove each one from the filesystem
        for temp_file in temp_files:
            os.remove(temp_file)

        # Log the completion of the merge process with the path to the final Parquet file
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
        # If output_path is not specified, set it to the default raw data file path from the config
        if output_path is None:
            output_path = os.path.normpath(
                os.path.join(self.config['database_file']['raw_data_file_path'], 'raw')
            )

        # Use the provided process function or default to the standard processing function
        process_function = process_function or self.default_process_chunk
        # Initialize a list to keep track of temporary files generated during processing
        temp_files = []

        try:
            # If file_paths is not provided, find all CSV files in the output directory
            if file_paths is None:
                file_paths = glob(os.path.join(output_path, '*.csv'))

            # Process each file path provided
            for file_path in file_paths:
                # Process the file and extend the temp_files list with the paths of generated temp files
                temp_files.extend(self._process_file(file_path, chunk_size, process_function, output_path))

            # If an output path is provided and temp files were created, merge them into a single Parquet file
            if output_path and temp_files:
                self._merge_parquet_files(temp_files, output_path)
                return None  # Return None since data is saved to Parquet

        except Exception as e:
            # Log any exceptions encountered during processing
            self._logger.error(f"Error processing meter data: {str(e)}")
            # Cleanup any temporary files that were created
            for temp_file in temp_files:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            # Re-raise the exception to allow upstream handling
            raise

    def process_and_convert_to_parquet_in_chunks(self,
                                                 root_path = "data_sources",
                                                 file_extension = "csv"):
        """
        Loop through all subdirectories in 'data_sources' and process each CSV file in chunks.
        Process each chunk using the melt_chunk function unless the folder contains
        'metadata' or 'weather', in which case don't modify the data.
        Finally, save the processed data to Parquet files in the same directory as the input files.
        """
        for folder, files in FileUtils().find_folders_with_extension(root_path, file_extension).items():
            # If the folder contains 'metadata' or 'weather', don't modify the data
            if any(["metadata" in folder, 'weather' in folder]):
                process_function = None
            # Otherwise, use the melt_chunk function to modify the data
            else:
                process_function = self.melt_chunk

            # Process each file in the folder
            self._process_meter_data(
                file_paths=files,
                chunk_size=50_000,
                process_function=process_function,
                output_path=folder)



