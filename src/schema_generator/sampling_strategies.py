from abc import ABC, abstractmethod
from typing import Any, List, Union
from pathlib import Path
import random
import pandas as pd
import numpy as np


class BaseSamplingStrategy(ABC):
    """
    Abstract base class for different sampling strategies.
    """

    @abstractmethod
    def sample_data(self, data_source: Union[str, Path], sample_size: int) -> Any:
        """Sample data using the strategy's algorithm"""
        pass


class RandomSamplingStrategy(BaseSamplingStrategy):
    """Implements random sampling of data"""

    def __init__(self, seed: int = 42):
        self.seed = seed
        random.seed(seed)
        np.random.seed(seed)

    def sample_data(self, data_source: Union[str, Path], sample_size: int) -> pd.DataFrame:
        # Implementation for random sampling
        df = pd.read_csv(data_source) if str(data_source).endswith('.csv') else pd.read_parquet(data_source)
        if len(df) <= sample_size:
            return df
        return df.sample(n=sample_size, random_state=self.seed)


class StratifiedSamplingStrategy(BaseSamplingStrategy):
    """Implements stratified sampling based on column values"""

    def __init__(self, strata_column: str, seed: int = 42):
        self.strata_column = strata_column
        self.seed = seed

    def sample_data(self, data_source: Union[str, Path], sample_size: int) -> pd.DataFrame:
        df = pd.read_csv(data_source) if str(data_source).endswith('.csv') else pd.read_parquet(data_source)
        if len(df) <= sample_size:
            return df

        # Calculate samples per stratum
        strata = df[self.strata_column].unique()
        samples_per_stratum = max(sample_size // len(strata), 1)

        sampled_data = []
        for stratum in strata:
            stratum_data = df[df[self.strata_column]==stratum]
            sampled_data.append(
                stratum_data.sample(
                    n=min(samples_per_stratum, len(stratum_data)),
                    random_state=self.seed
                )
            )

        return pd.concat(sampled_data)