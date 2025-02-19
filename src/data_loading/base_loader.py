from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union, Optional, Dict

class BaseDataLoader(ABC):
    """
    Abstract base class for data loading strategies.
    Defines the common interface for different data loading implementations.
    """

    @abstractmethod
    def load_data(
            self,
            file_path: Union[str, Path],
            table_name: str,
            chunk_size: int = 100000
    ) -> None:
        """
        Abstract method to load data from a file into a database.

        Args:
            file_path: Path to the source data file
            table_name: Name of the target database table
            chunk_size: Number of rows to process in each batch
        """
        pass