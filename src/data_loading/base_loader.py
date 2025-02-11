from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union, Optional, Dict

class BaseDataLoader(ABC):
    """
    Abstract base class for data loading strategies.
    Defines the common interface for different data loading implementations.
    """

    @abstractmethod
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

    @abstractmethod
    def _verify_connection(self) -> bool:
        """
        Verify database connection.

        Returns:
            bool: Connection status
        """
        pass

    @abstractmethod
    def _generate_schema(self, file_path: Union[str, Path]) -> Optional[Dict[str, Union[str, Path]]]:
        """
        Generate database schema from a data file.

        Args:
            file_path: Path to the source data file

        Returns:
            Dict containing schema generation details or None
        """
        pass