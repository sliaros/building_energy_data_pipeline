from abc import ABC, abstractmethod
from typing import List, Union, Dict, Optional
from pathlib import Path
from dataclasses import dataclass


@dataclass
class BaseColumnInfo:
    """Base class for column information"""
    name: str
    data_type: str
    nullable: bool
    original_type: str
    stats: Dict
    sample_values: Optional[List] = None
    metadata: Optional[Dict] = None


class BaseSchemaAnalyzer(ABC):
    """
    Abstract base class for schema analysis.
    Defines the interface that all schema analyzers must implement.
    """

    @abstractmethod
    def analyze_schema(self, data_source: Union[str, Path]) -> List[BaseColumnInfo]:
        """
        Analyze the schema of a data source.

        Args:
            data_source: Path to the data file or data source

        Returns:
            List of column information objects
        """
        pass

    @abstractmethod
    def validate_source(self, data_source: Union[str, Path]) -> bool:
        """
        Validate if the data source is supported and accessible.

        Args:
            data_source: Path to the data file or data source

        Returns:
            True if source is valid, False otherwise
        """
        pass

    @abstractmethod
    def get_sample_data(self, data_source: Union[str, Path], sample_size: int) -> Dict:
        """
        Get a sample of data for analysis.

        Args:
            data_source: Path to the data file or data source
            sample_size: Number of records to sample

        Returns:
            Dictionary containing sample data and metadata
        """
        pass