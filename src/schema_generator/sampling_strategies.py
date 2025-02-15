from abc import ABC, abstractmethod
from typing import Any, List, Union, Tuple
from pathlib import Path
from dataclasses import dataclass
import random
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq


@dataclass
class SamplingConfig:
    """Configuration for data sampling strategy"""
    max_rows: int = 100000  # Maximum number of rows to sample
    max_file_size: int = 1024 * 1024 * 100  # 100MB chunk size for reading
    sampling_ratio: float = 0.01  # 1% sampling ratio for large files
    random_seed: int = 42  # For reproducibility


class BaseSamplingStrategy(ABC):
    """
    Abstract base class for different sampling strategies.
    """

    def __init__(self, config: SamplingConfig = None):
        self.config = config or SamplingConfig()
        random.seed(self.config.random_seed)
        np.random.seed(self.config.random_seed)

    @abstractmethod
    def sample_data(self, data_source: Union[str, Path], sample_size: int) -> Any:
        """Sample data using the strategy's algorithm"""
        pass

    def _handle_file_input(self, data_source: Union[str, Path]) -> Path:
        """Convert input to Path and validate"""
        path = Path(data_source)
        if not path.exists():
            raise FileNotFoundError(f"Data source not found: {data_source}")
        return path


class RandomSamplingStrategy(BaseSamplingStrategy):
    """Implements intelligent random sampling of data"""

    def sample_data(self, data_source: Union[str, Path], sample_size: int) -> pd.DataFrame:
        path = self._handle_file_input(data_source)

        if path.suffix == '.parquet':
            return self._sample_parquet(path, sample_size)
        elif path.suffix == '.csv':
            return self._sample_csv(path, sample_size)
        else:
            raise ValueError(f"Unsupported file type: {path.suffix}")

    def _sample_parquet(self, file_path: Path, sample_size: int) -> pd.DataFrame:
        dataset = ds.dataset(file_path, format='parquet')

        # Get total row count from metadata
        total_rows = sum(fragment.metadata.num_rows for fragment in dataset.get_fragments())

        if total_rows <= sample_size:
            return dataset.to_table().to_pandas()

        # Calculate actual sample size
        final_sample_size = min(
            sample_size,
            self.config.max_rows,
            int(total_rows * self.config.sampling_ratio)
        )

        # Ensure at least 2 rows (first and last)
        if final_sample_size < 2:
            final_sample_size = 2

        # Generate random row indices, including first and last
        indices = sorted(random.sample(range(1, total_rows - 1), final_sample_size - 2))
        indices = [0] + indices + [total_rows - 1]

        # Stream through the dataset and collect only needed rows
        selected_rows = []
        current_row = 0

        global_row = 0  # Track absolute row position
        selected_rows = []  # Store selected rows

        for batch in dataset.scanner().to_batches():
            batch_size = batch.num_rows
            batch_df = batch.to_pandas()  # Convert batch to Pandas DataFrame

            # Compute global row index range for this batch
            batch_start = global_row
            batch_end = global_row + batch_size - 1

            # Find which indices belong to this batch
            batch_indices = [idx for idx in indices if batch_start <= idx <= batch_end]
            if batch_indices:
                relative_indices = [idx - batch_start for idx in batch_indices]  # Adjust for batch indexing
                selected_rows.append(batch_df.iloc[relative_indices])

            global_row += batch_size  # Update global row counter

        # Concatenate all selected rows into a single DataFrame
        sampled_df = pd.concat(selected_rows, ignore_index=True)

        return sampled_df

    def _sample_csv(self, file_path: Path, sample_size: int) -> pd.DataFrame:
        file_size = file_path.stat().st_size

        if file_size <= self.config.max_file_size:
            df = pd.read_csv(file_path)
            if len(df) <= sample_size:
                return df

            # Ensure we have room for the first and last rows
            if sample_size < 2:
                sample_size = 2

            # Sample the data, excluding the first and last rows
            sampled_df = df.iloc[1:-1].sample(n=sample_size - 2, random_state=self.config.random_seed)

            # Add the first and last rows
            return pd.concat([df.iloc[[0]], sampled_df, df.iloc[[-1]]])

        # For large files, estimate total rows first
        with open(file_path, 'rb') as f:
            chunk = pd.read_csv(f, nrows=1000)
            avg_row_size = file_size / len(chunk)
            estimated_total_rows = int(file_size / avg_row_size)

        final_sample_size = min(
            sample_size,
            self.config.max_rows,
            int(estimated_total_rows * self.config.sampling_ratio)
        )

        # Ensure we have room for the first and last rows
        if final_sample_size < 2:
            final_sample_size = 2

        # Skip random rows for efficiency, excluding the first and last rows
        skip_rows = sorted(random.sample(
            range(1, estimated_total_rows - 1),
            estimated_total_rows - final_sample_size
        ))

        # Add the first and last rows
        skip_rows = [row for row in skip_rows if row != 0 and row != estimated_total_rows - 1]

        return pd.read_csv(file_path, skiprows=skip_rows)


class SystematicSamplingStrategy(BaseSamplingStrategy):
    """Implements systematic sampling of data"""

    def sample_data(self, data_source: Union[str, Path], sample_size: int) -> pd.DataFrame:
        path = self._handle_file_input(data_source)

        if path.suffix=='.parquet':
            return self._sample_parquet(path, sample_size)
        elif path.suffix=='.csv':
            return self._sample_csv(path, sample_size)
        else:
            raise ValueError(f"Unsupported file type: {path.suffix}")

    def _sample_parquet(self, file_path: Path, sample_size: int) -> pd.DataFrame:
        dataset = ds.dataset(file_path, format='parquet')
        total_rows = sum(1 for _ in dataset.scanner().to_batches())

        if total_rows <= sample_size:
            return dataset.to_table().to_pandas()

        final_sample_size = min(
            sample_size,
            self.config.max_rows,
            int(total_rows * self.config.sampling_ratio)
        )

        # Systematic sampling - take evenly spaced rows
        step = total_rows // final_sample_size
        scanner = dataset.scanner(columns=dataset.schema.names)
        return scanner.take(range(0, total_rows, step)[:final_sample_size]).to_pandas()

    def _sample_csv(self, file_path: Path, sample_size: int) -> pd.DataFrame:
        file_size = file_path.stat().st_size

        if file_size <= self.config.max_file_size:
            df = pd.read_csv(file_path)
            if len(df) <= sample_size:
                return df
            step = len(df) // sample_size
            return df.iloc[::step]

        # For large files, use efficient skiprows
        with open(file_path, 'rb') as f:
            chunk = pd.read_csv(f, nrows=1000)
            avg_row_size = file_size / len(chunk)
            estimated_total_rows = int(file_size / avg_row_size)

        final_sample_size = min(
            sample_size,
            self.config.max_rows,
            int(estimated_total_rows * self.config.sampling_ratio)
        )

        n = max(estimated_total_rows // final_sample_size, 1)
        return pd.read_csv(file_path, skiprows=lambda x: x % n!=0)


class StratifiedSamplingStrategy(BaseSamplingStrategy):
    """Implements stratified sampling based on column values"""

    def __init__(self, strata_column: str, config: SamplingConfig = None):
        super().__init__(config)
        self.strata_column = strata_column

    def sample_data(self, data_source: Union[str, Path], sample_size: int) -> pd.DataFrame:
        path = self._handle_file_input(data_source)

        if path.suffix=='.parquet':
            return self._sample_parquet(path, sample_size)
        elif path.suffix=='.csv':
            return self._sample_csv(path, sample_size)
        else:
            raise ValueError(f"Unsupported file type: {path.suffix}")

    def _sample_parquet(self, file_path: Path, sample_size: int) -> pd.DataFrame:
        dataset = ds.dataset(file_path, format='parquet')
        scanner = dataset.scanner()
        batches = list(scanner.to_batches())

        if not batches:
            return pd.DataFrame()

        total_rows = sum(len(batch) for batch in batches)
        if total_rows <= sample_size:
            return dataset.to_table().to_pandas()

        final_sample_size = min(
            sample_size,
            self.config.max_rows,
            int(total_rows * self.config.sampling_ratio)
        )

        # Sample from each batch proportionally
        sampled_batches = []
        rows_per_batch = final_sample_size // len(batches)

        for batch in batches:
            if len(batch) > rows_per_batch:
                indices = random.sample(range(len(batch)), rows_per_batch)
                sampled_batches.append(batch.take(indices))
            else:
                sampled_batches.append(batch)

        return pa.Table.from_batches(sampled_batches).to_pandas()

    def _sample_csv(self, file_path: Path, sample_size: int) -> pd.DataFrame:
        file_size = file_path.stat().st_size

        if file_size <= self.config.max_file_size:
            df = pd.read_csv(file_path)
            if len(df) <= sample_size:
                return df

            strata = df[self.strata_column].unique()
            samples_per_stratum = max(sample_size // len(strata), 1)

            sampled_data = []
            for stratum in strata:
                stratum_data = df[df[self.strata_column]==stratum]
                sampled_data.append(
                    stratum_data.sample(
                        n=min(samples_per_stratum, len(stratum_data)),
                        random_state=self.config.random_seed
                    )
                )
            return pd.concat(sampled_data)

        # For large files, read in chunks
        chunks = []
        chunk_size = self.config.max_file_size // 1024  # Smaller chunks

        final_sample_size = min(
            sample_size,
            self.config.max_rows,
            int(file_size / (chunk_size * self.config.sampling_ratio))
        )

        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            size = min(len(chunk) // 10, final_sample_size // 10)
            strata = chunk[self.strata_column].unique()
            samples_per_stratum = max(size // len(strata), 1)

            sampled_chunk = []
            for stratum in strata:
                stratum_data = chunk[chunk[self.strata_column]==stratum]
                if len(stratum_data) > samples_per_stratum:
                    sampled_chunk.append(
                        stratum_data.sample(
                            n=samples_per_stratum,
                            random_state=self.config.random_seed
                        )
                    )
                else:
                    sampled_chunk.append(stratum_data)

            chunks.append(pd.concat(sampled_chunk))

        return pd.concat(chunks, ignore_index=True)