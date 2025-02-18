import os
import zipfile
import logging
import yaml
import pandas as pd
from collections import defaultdict
from typing import Dict, List, Union, Tuple
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path

class FileUtils:
    """Utility class for handling file-related operations."""

    def __init__(self, logger=None):
        self._logger = logger or logging.getLogger(self.__class__.__name__)  # Use orchestrator's logger if available

    def create_directory(self, directory_path):
        """Creates a directory if it does not exist, handling errors gracefully"""

        try:
            os.makedirs(directory_path, exist_ok=True)
            self._logger.info(f"Directory created or already exists: {directory_path}")
        except PermissionError as e:
            self._logger.error(f"Permission denied creating directory {directory_path}: {e}")
        except OSError as e:
            self._logger.error(f"Error creating directory {directory_path}: {e}")

    def file_exists(self, file_path):
        """Checks if a file exists, logging any access issues."""
        try:
            exists = os.path.exists(file_path) and os.path.isfile(file_path)
            self._logger.debug(f"Checked existence for {file_path}: {exists}")
            return exists
        except Exception as e:
            self._logger.error(f"Error checking file existence {file_path}: {e}")
            return False  # Default to False on error

    def get_file_size(self, file_path):
        """Helper method to get the size of a file."""
        try:
            return os.path.getsize(file_path)
        except OSError as e:
            self._logger.error(f"Failed to get file size: {e}")
            return None

    def extract_file(self, zip_file, file_info, output_file_path):
        """Extracts a single file from a zip archive, handling errors."""
        try:
            with zip_file.open(file_info) as source, open(output_file_path, 'wb') as target:
                target.write(source.read())
            self._logger.info(f"Extracted: {os.path.basename(output_file_path)}")
        except FileNotFoundError:
            self._logger.error(f"File not found when extracting {output_file_path}")
        except PermissionError:
            self._logger.error(f"Permission denied writing {output_file_path}")
        except zipfile.BadZipFile:
            self._logger.error(f"Bad zip file encountered while extracting {output_file_path}")
        except Exception as e:
            self._logger.error(f"Unexpected error extracting file {output_file_path}: {e}")

    def unzip_folders(self, zip_file_path, target_folder=None, folder_names=None):
        """
        Unzips folders from a zip file that contain names from a list.
        """
        if target_folder is None:
            target_folder = os.path.dirname(zip_file_path)

        folder_names = folder_names or []
        self._logger.info(f"Attempting to unzip {zip_file_path} into {target_folder}")

        try:
            if not os.path.exists(zip_file_path):
                raise FileNotFoundError(f"Zip file {zip_file_path} does not exist.")

            with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
                for file_info in zip_file.infolist():
                    if file_info.is_dir():
                        continue  # Skip directories

                    parent_folder = os.path.dirname(file_info.filename)

                    # If specific folder names are given, check if the file belongs to one
                    if folder_names:
                        for folder_name in folder_names:
                            if folder_name in parent_folder:
                                self._extract_to_target(zip_file, file_info, target_folder, folder_name)
                    else:
                        zip_file.extractall(target_folder)
                        self._logger.info(f"Unzipped all files to {target_folder}")
                        return
        except FileNotFoundError as e:
            self._logger.error(f"File not found: {e}")
        except zipfile.BadZipFile as e:
            self._logger.error(f"Invalid or corrupted zip file {zip_file_path}: {e}")
        except PermissionError as e:
            self._logger.error(f"Permission denied accessing {zip_file_path}: {e}")
        except Exception as e:
            self._logger.error(f"Unexpected error while unzipping {zip_file_path}: {e}")

    def _extract_to_target(self, zip_file, file_info, target_folder, folder_name):
        """Helper method to extract a file into a specific target folder."""
        file_name = os.path.basename(file_info.filename)
        output_dir = os.path.join(target_folder, folder_name)
        output_file_path = os.path.join(output_dir, file_name)

        self.create_directory(output_dir)

        if self.file_exists(output_file_path):
            self._logger.warning(f"File '{file_name}' already exists. Skipping extraction.")
        else:
            self.extract_file(zip_file, file_info, output_file_path)

    def _load_yaml_file(self,file_path):
        """
        Loads a YAML file and returns its contents as a dictionary.

        Args:
            file_path (str): The path to the YAML file.

        Returns:
            dict: The contents of the YAML file.
        """
        try:
            with open(file_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            print("Error loading YAML file: ", e)
            self._logger.error("Error loading YAML file: %s", e)

    def _define_local_file_path(self, folder_path=None, file_name=None):
        """
        Defines the file path based on the local file structure.

        Args:
            folder_path (str): The path to the folder relative to the parent directory.
            file_name (str): The name of the file.

        Returns:
            str: The file path.
        """
        def _get_parent_dir():
            """
            Returns the parent directory path.
            """
            # return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            return os.getcwd()

        if folder_path:
            if isinstance(folder_path, list):
                folder_path = os.path.join(_get_parent_dir(), *folder_path)
            else:
                folder_path = os.path.join(_get_parent_dir(), folder_path)
        else:
            folder_path = _get_parent_dir()

        if file_name is None:
            return folder_path
        else:
            return os.path.join(folder_path, file_name)

    @staticmethod
    def ensure_directory_exists(path: str) -> None:
        """Ensure that the directory for a given path exists."""
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def save_parquet(self, df: pd.DataFrame, path: str) -> None:
        """Save DataFrame as a Parquet file."""
        FileUtils.ensure_directory_exists(path)
        df.to_parquet(path, index=False)
        self._logger.info(f"{path} created")

    @staticmethod
    def save_csv(df: pd.DataFrame, path: str, mode: str = 'w', header: bool = True) -> None:
        """Save DataFrame as a CSV file."""
        FileUtils.ensure_directory_exists(path)
        df.to_csv(path, mode=mode, index=False, header=header)

    def csv_to_parquet(self, csv_path: str, parquet_path: str, use_gzip: bool = False) -> None:
        """
        Converts a CSV file to Parquet format.

        Parameters:
        -----------
        csv_path : str
            Path to the input CSV file.
        parquet_path : str
            Path to save the output Parquet file.
        use_gzip : bool, optional
            If True, compresses the Parquet file using gzip (default: False).

        Returns:
        --------
        None
        """
        try:
            # Read CSV into DataFrame
            df = pd.read_csv(csv_path)

            # Save as Parquet with optional gzip compression
            compression = "gzip" if use_gzip else None
            df.to_parquet(parquet_path, engine="pyarrow", compression=compression)

            self._logger.info(f"Conversion successful: {parquet_path}")

        except Exception as e:
            self._logger.info(f"Unhandled Exception: {e}")

    def csv_to_parquet_in_chunks(self, csv_path: str, parquet_path: str, chunk_size: int = 50000, use_gzip: bool = False) -> None:
        """
        Converts a large CSV file to Parquet in chunks.

        Parameters:
        -----------
        csv_path : str
            Path to the input CSV file.
        parquet_path : str
            Path to save the output Parquet file.
        chunk_size : int, optional
            Number of rows per chunk (default: 10,000).
        use_gzip : bool, optional
            If True, compresses the Parquet file using gzip (default: False).

        Returns:
        --------
        None
        """
        try:
            compression = "gzip" if use_gzip else None
            temp_files = []

            # Read CSV in chunks
            for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunk_size)):
                temp_file = f"{parquet_path}_chunk_{i}.parquet"
                chunk.to_parquet(temp_file, engine="pyarrow", compression=compression)
                temp_files.append(temp_file)
                self._logger.info(f"Processed chunk {i + 1}")

            # Combine chunks into a single Parquet file
            df_list = [pd.read_parquet(f) for f in temp_files]
            combined_df = pd.concat(df_list, ignore_index=True)
            combined_df.to_parquet(parquet_path, engine="pyarrow", compression=compression)

            # Cleanup temporary files
            for temp_file in temp_files:
                os.remove(temp_file)

            self._logger.info(f"Conversion successful: {parquet_path}")

        except Exception as e:
            self._logger.info(f"Unhandled Exception: {e}")

    def find_folders_with_extension(self, root_folder: str, extension: str) -> Dict[str, List[str]]:
        """
        Finds all folders that contain at least one file with the given extension
        and lists the file paths for each folder.

        Parameters:
        -----------
        root_folder : str
            The root directory to search for files.
        extension : str
            The file extension to look for (e.g., "csv", "parquet", "txt").

        Returns:
        --------
        Dict[str, List[str]]
            A dictionary where keys are folder paths and values are lists of file paths in those folders.
        """
        extension = extension.lstrip(".")  # Remove leading dot if provided
        file_folders = defaultdict(list)

        for dirpath, _, filenames in os.walk(root_folder):
            matching_files = [os.path.join(dirpath, file) for file in filenames if file.endswith(f'.{extension}')]
            if matching_files:
                file_folders[dirpath].extend(matching_files)

        file_folders_detected = len(file_folders)

        if file_folders_detected > 0:
            self._logger.info(f"Detected {file_folders_detected} file folders")
            return file_folders
        else:
            self._logger.warning(f"No file folders detected in root folder {root_folder} containing files with extension {extension}")
            return file_folders

    @staticmethod
    def get_file_type_and_reader(file_path: Union[str, Path]) -> Tuple[str, callable]:
        """
        Determine file type and return appropriate reader function.

        Args:
            file_path: Path to the file

        Returns:
            Tuple[str, callable]: File type and corresponding reader function
        """
        file_path = Path(file_path)

        def read_parquet(file_path: Union[str, Path], nrows: int = None) -> pd.DataFrame:
            """
            Reads a Parquet file into a Pandas DataFrame, optionally limiting the number of rows.

            Args:
                file_path (Union[str, Path]): Path to the Parquet file.
                nrows (int, optional): Number of rows to read. If None, reads the entire file.

            Returns:
                pd.DataFrame: A Pandas DataFrame containing the data.
            """
            # Open the Parquet file
            parquet_file = pq.ParquetFile(file_path)

            # If nrows is not specified, read the entire file
            if nrows is None:
                return parquet_file.read().to_pandas()

            # Read the first n rows
            rows_read = 0
            tables = []

            # Iterate through row groups until we have enough rows
            for i in range(parquet_file.num_row_groups):
                table = parquet_file.read_row_groups(row_groups=[i], columns=None)
                tables.append(table)
                rows_read += table.num_rows
                if rows_read >= nrows:
                    break

            # Combine tables and select the first nrows rows
            combined_table = pa.Table.from_batches([batch for table in tables for batch in table.to_batches()])
            df = combined_table.to_pandas().head(nrows)
            return df

        if file_path.suffix.lower()=='.parquet':
            return 'parquet', read_parquet
        elif file_path.suffix.lower() in ['.csv', '.txt']:
            return 'csv', pd.read_csv
        else:
            raise ValueError(f"Unsupported file type: {file_path.suffix}")