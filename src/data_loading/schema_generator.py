from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple
import logging
import re
from datetime import datetime
import random
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import numpy as np

# # Configure logging with a consistent format
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )
# logger = logging.getLogger(__name__)


class FileType(Enum):
    """Supported file types for schema generation"""
    CSV = "csv"
    PARQUET = "parquet"


class SQLType(Enum):
    """
    PostgreSQL data types with their corresponding constraints.
    Includes comprehensive type coverage for various data scenarios.
    """
    SMALLINT = "SMALLINT"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    REAL = "REAL"
    DOUBLE_PRECISION = "DOUBLE PRECISION"
    BOOLEAN = "BOOLEAN"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMPTZ = "TIMESTAMP WITH TIME ZONE"
    DATE = "DATE"
    TIME = "TIME"
    TEXT = "TEXT"
    VARCHAR = "VARCHAR"
    CHAR = "CHAR"
    BYTEA = "BYTEA"
    JSON = "JSON"
    JSONB = "JSONB"
    UUID = "UUID"
    NUMERIC = "NUMERIC"


class TypeMapper:
    """
    Handles mapping between different type systems.
    Provides comprehensive mapping from Apache Arrow to PostgreSQL types.
    """

    # Detailed mapping from Apache Arrow to PostgreSQL types
    ARROW_TO_POSTGRESQL = {
        # Integer types
        pa.int8(): SQLType.SMALLINT.value,
        pa.int16(): SQLType.SMALLINT.value,
        pa.int32(): SQLType.INTEGER.value,
        pa.int64(): SQLType.BIGINT.value,
        pa.uint8(): SQLType.SMALLINT.value,
        pa.uint16(): SQLType.INTEGER.value,
        pa.uint32(): SQLType.BIGINT.value,
        pa.uint64(): SQLType.NUMERIC.value,  # Use NUMERIC for very large integers

        # Floating point types
        pa.float16(): SQLType.REAL.value,
        pa.float32(): SQLType.REAL.value,
        pa.float64(): SQLType.DOUBLE_PRECISION.value,

        # String types
        pa.string(): SQLType.TEXT.value,
        pa.large_string(): SQLType.TEXT.value,

        # Boolean type
        pa.bool_(): SQLType.BOOLEAN.value,

        # Date and time types
        pa.date32(): SQLType.DATE.value,
        pa.date64(): SQLType.DATE.value,
        pa.time32('s'): SQLType.TIME.value,
        pa.time64('us'): SQLType.TIME.value,
        pa.timestamp('s'): SQLType.TIMESTAMP.value,
        pa.timestamp('ms'): SQLType.TIMESTAMP.value,
        pa.timestamp('us'): SQLType.TIMESTAMP.value,
        pa.timestamp('ns'): SQLType.TIMESTAMP.value,

        # Binary types
        pa.binary(): SQLType.BYTEA.value,
        pa.large_binary(): SQLType.BYTEA.value,

        # Special types
        pa.decimal128(34, 4): SQLType.NUMERIC.value,
        pa.null(): SQLType.TEXT.value,  # Default to TEXT for null type
    }

    # Add timezone-aware timestamp mappings
    for unit in ['s', 'ms', 'us', 'ns']:
        ARROW_TO_POSTGRESQL[pa.timestamp(unit, tz='UTC')] = SQLType.TIMESTAMPTZ.value

    @classmethod
    def get_postgresql_type(cls, arrow_type: pa.DataType,
                            sample_data: Optional[pd.Series] = None) -> str:
        """
        Map an Arrow type to its PostgreSQL equivalent with smart type inference.

        Args:
            arrow_type: PyArrow data type to map
            sample_data: Optional sample data for better type inference

        Returns:
            PostgreSQL data type as string
        """
        # Handle special cases and complex types
        if pa.types.is_decimal(arrow_type):
            precision = arrow_type.precision
            scale = arrow_type.scale
            return f"{SQLType.NUMERIC.value}({precision},{scale})"

        if pa.types.is_fixed_size_binary(arrow_type):
            return f"{SQLType.CHAR.value}({arrow_type.byte_width})"

        if pa.types.is_string(arrow_type) and sample_data is not None:
            # Analyze string length for VARCHAR optimization
            max_length = sample_data.astype(str).str.len().max()
            if max_length <= 255:
                return f"{SQLType.VARCHAR.value}({max_length})"

        # Use the standard mapping for other types
        return cls.ARROW_TO_POSTGRESQL.get(arrow_type, SQLType.TEXT.value)


@dataclass
class SamplingStrategy:
    """Configuration for data sampling strategy"""
    method: str = "random"  # Options: "random", "systematic", "stratified"
    max_rows: int = 100000  # Maximum number of rows to sample
    max_file_size: int = 1024 * 1024 * 100  # 100MB chunk size for reading
    sampling_ratio: float = 0.01  # 1% sampling ratio for large files
    random_seed: int = 42  # For reproducibility


class SmartSampler:
    """Handles intelligent sampling of large datasets"""

    def __init__(self, strategy: SamplingStrategy):
        self.strategy = strategy
        random.seed(strategy.random_seed)
        np.random.seed(strategy.random_seed)

    def sample_parquet(self, file_path: Path) -> Tuple[pd.DataFrame, pa.Schema]:
        """
        Intelligently sample a Parquet file.

        Uses PyArrow's dataset API to efficiently read only needed portions.
        """
        dataset = ds.dataset(file_path, format='parquet')
        total_rows = sum(1 for _ in dataset.scanner().to_batches())

        if total_rows <= self.strategy.max_rows:
            # If file is small enough, read it entirely
            table = dataset.to_table()
            return table.to_pandas(), table.schema

        # Calculate number of rows to sample
        sample_size = min(
            self.strategy.max_rows,
            int(total_rows * self.strategy.sampling_ratio)
        )

        if self.strategy.method=="random":
            # Generate random row indices
            indices = sorted(random.sample(range(total_rows), sample_size))

            # Read only the selected rows
            scanner = dataset.scanner(
                columns=dataset.schema.names,
                filter=ds.field('__index_level_0').isin(indices)
            )
            table = scanner.to_table()
            return table.to_pandas(), dataset.schema

        elif self.strategy.method=="systematic":
            # Systematic sampling - take evenly spaced rows
            step = total_rows // sample_size
            scanner = dataset.scanner(
                columns=dataset.schema.names
            )
            table = scanner.take(range(0, total_rows, step)[:sample_size])
            return table.to_pandas(), dataset.schema

        else:  # stratified - simplified version
            scanner = dataset.scanner()
            batches = list(scanner.to_batches())
            sampled_batches = []

            # Take some rows from each batch
            rows_per_batch = sample_size // len(batches)
            for batch in batches:
                if len(batch) > rows_per_batch:
                    indices = random.sample(range(len(batch)), rows_per_batch)
                    sampled_batches.append(batch.take(indices))
                else:
                    sampled_batches.append(batch)

            table = pa.Table.from_batches(sampled_batches)
            return table.to_pandas(), dataset.schema

    def sample_csv(self, file_path: Path) -> pd.DataFrame:
        """
        Intelligently sample a CSV file.

        Uses different strategies depending on file size and available memory.
        """
        file_size = file_path.stat().st_size

        if file_size <= self.strategy.max_file_size:
            # For small files, read everything
            return pd.read_csv(file_path)

        # For large files, estimate total rows first
        with open(file_path, 'rb') as f:
            # Read first chunk to get average row size
            chunk = pd.read_csv(f, nrows=1000)
            avg_row_size = file_size / len(chunk)
            estimated_total_rows = int(file_size / avg_row_size)

        sample_size = min(
            self.strategy.max_rows,
            int(estimated_total_rows * self.strategy.sampling_ratio)
        )

        if self.strategy.method=="random":
            # Skip random rows
            skip_rows = sorted(random.sample(
                range(1, estimated_total_rows),
                estimated_total_rows - sample_size
            ))
            return pd.read_csv(file_path, skiprows=skip_rows)

        elif self.strategy.method=="systematic":
            # Read every nth row
            n = max(estimated_total_rows // sample_size, 1)
            return pd.read_csv(file_path, skiprows=lambda x: x % n!=0)

        else:  # stratified
            # Read in chunks and sample from each
            chunks = []
            chunk_size = self.strategy.max_file_size // 1024  # Smaller chunks

            for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                size = min(len(chunk) // 10, sample_size // 10)
                chunks.append(chunk.sample(n=size))

            return pd.concat(chunks, ignore_index=True)

@dataclass
class ColumnInfo:
    """
    Stores analyzed information about a database column.
    Includes metadata useful for schema generation and optimization.
    """
    name: str
    sql_type: str
    nullable: bool
    stats: Dict  # Statistical information about the column
    original_type: Union[str, pa.DataType]  # Original data type
    sample_values: Optional[List] = None  # Sample of distinct values
    unique_ratio: Optional[float] = None  # Ratio of unique values
    comment: Optional[str] = None  # Column documentation


class SchemaAnalyzer:
    """
    Analyzes data files and determines appropriate schema information.
    Handles both CSV and Parquet files with intelligent sampling.
    """

    def __init__(self,
                 file_path: Union[str, Path],
                 sampling_strategy: Optional[SamplingStrategy] = None):
        """Initialize the schema analyzer with optional sampling strategy."""
        self.file_path = Path(file_path)
        self.file_type = self._determine_file_type()
        self.sampler = SmartSampler(
            sampling_strategy or SamplingStrategy()
        )

    def _determine_file_type(self) -> FileType:
        """Determine the type of input file based on extension."""
        extension = self.file_path.suffix.lower()
        if extension=='.csv':
            return FileType.CSV
        elif extension in ('.parquet', '.pq'):
            return FileType.PARQUET
        else:
            raise ValueError(f"Unsupported file type: {extension}")

    def _analyze_column_from_arrow(self,
                                   field: pa.Field,
                                   sample_data: Optional[pd.Series] = None) -> ColumnInfo:
        """
        Analyze a column based on its Arrow schema field and optional sample data.

        Args:
            field: PyArrow field from schema
            sample_data: Optional sample data for additional analysis

        Returns:
            ColumnInfo object with analyzed information
        """
        clean_name = self._clean_identifier(field.name)

        # Get SQL type using TypeMapper
        sql_type = TypeMapper.get_postgresql_type(field.type, sample_data)

        # Calculate statistics if sample data is available
        stats = {
            "nullable": field.nullable,
            "original_type": str(field.type)
        }

        if sample_data is not None:
            non_null_count = sample_data.count()
            unique_count = sample_data.nunique()

            stats.update({
                "sample_size": len(sample_data),
                "non_null_count": non_null_count,
                "unique_count": unique_count,
                "memory_usage": sample_data.memory_usage(deep=True)
            })

        # Create column info
        column_info = ColumnInfo(
            name=clean_name,
            sql_type=sql_type,
            nullable=field.nullable,
            stats=stats,
            original_type=field.type
        )

        # Add additional metadata if sample data exists
        if sample_data is not None:
            column_info.sample_values = (
                sample_data.dropna()
                .unique()
                .tolist()[:5]  # Store up to 5 sample values
            )
            column_info.unique_ratio = (
                unique_count / non_null_count if non_null_count > 0 else 0
            )

        return column_info

    def _analyze_column_from_pandas(self,
                                    series: pd.Series,
                                    name: str) -> ColumnInfo:
        """
        Analyze a column based on pandas Series data.

        Args:
            series: Pandas series containing column data
            name: Column name

        Returns:
            ColumnInfo object with analyzed information
        """
        clean_name = self._clean_identifier(name)
        non_null_data = series.dropna()

        # Calculate basic statistics
        stats = {
            "null_count": len(series) - len(non_null_data),
            "unique_count": len(series.unique()),
            "sample_size": len(series),
            "memory_usage": series.memory_usage(deep=True)
        }

        # Determine SQL type
        sql_type = self._determine_sql_type(series)

        # Create column info
        column_info = ColumnInfo(
            name=clean_name,
            sql_type=sql_type,
            nullable=stats["null_count"] > 0,
            stats=stats,
            original_type=str(series.dtype),
            sample_values=series.dropna().unique().tolist()[:5],
            unique_ratio=(
                stats["unique_count"] /
                (len(series) - stats["null_count"])
                if len(series) - stats["null_count"] > 0
                else 0
            )
        )

        # Add comments for special cases
        if column_info.unique_ratio==1.0:
            column_info.comment = "Unique values - consider as potential key"
        elif column_info.unique_ratio < 0.01:
            column_info.comment = "Low cardinality - consider indexing"

        return column_info

    def _determine_sql_type(self, series: pd.Series) -> str:
        """
        Determine the appropriate SQL type for a pandas Series.

        Args:
            series: Pandas series to analyze

        Returns:
            PostgreSQL data type as string
        """
        if len(series.dropna())==0:
            return SQLType.TEXT.value

        dtype = str(series.dtype)

        # Numeric types
        if pd.api.types.is_numeric_dtype(series):
            non_null = series.dropna()
            if all(non_null.apply(lambda x: float(x).is_integer())):
                min_val, max_val = non_null.min(), non_null.max()

                if min_val >= -32768 and max_val <= 32767:
                    return SQLType.SMALLINT.value
                elif min_val >= -2147483648 and max_val <= 2147483647:
                    return SQLType.INTEGER.value
                return SQLType.BIGINT.value

            # Check if decimal precision is needed
            decimal_places = non_null.apply(
                lambda x: len(str(x).split('.')[-1])
                if '.' in str(x) else 0
            ).max()

            if decimal_places > 0:
                total_digits = non_null.apply(
                    lambda x: len(str(x).replace('.', ''))
                ).max()
                return f"{SQLType.NUMERIC.value}({total_digits},{decimal_places})"

            return SQLType.DOUBLE_PRECISION.value

        # Boolean type
        if dtype=='bool':
            return SQLType.BOOLEAN.value

        # Datetime types
        if pd.api.types.is_datetime64_any_dtype(series):
            if any(series.dt.tz is not None):
                return SQLType.TIMESTAMPTZ.value
            return SQLType.TIMESTAMP.value

        # String/Object types
        max_length = series.astype(str).str.len().max()
        if max_length <= 255:
            return f"{SQLType.VARCHAR.value}({max_length})"
        return SQLType.TEXT.value

    @staticmethod
    def _clean_identifier(name: str) -> str:
        """Clean a column name to be a valid SQL identifier."""
        clean = re.sub(r'[^a-zA-Z0-9_]', '_', name.lower())
        return f"col_{clean}" if clean[0].isdigit() else clean

    def analyze(self) -> List[ColumnInfo]:
        """
        Perform the schema analysis.

        Returns:
            List of ColumnInfo objects containing column analyses
        """
        sample_data, arrow_schema = self._read_data_sample()

        if self.file_type==FileType.PARQUET and arrow_schema:
            # Use Arrow schema for Parquet files
            return [
                self._analyze_column_from_arrow(
                    field,
                    sample_data.get(field.name)
                )
                for field in arrow_schema
            ]
        else:
            # Analyze data for CSV files
            return [
                self._analyze_column_from_pandas(sample_data[col], col)
                for col in sample_data.columns
            ]

    def _read_data_sample(self) -> Tuple[pd.DataFrame, Optional[pa.Schema]]:
        """Read a sample of data using the configured sampling strategy."""
        try:
            if self.file_type==FileType.CSV:
                return self.sampler.sample_csv(self.file_path), None
            else:  # Parquet
                return self.sampler.sample_parquet(self.file_path)
        except Exception as e:
            raise ValueError(f"Error reading data sample: {str(e)}")


class SQLSchemaGenerator:
    """Generates SQL schema definitions from analyzed column information"""

    def __init__(self, table_name: Optional[str] = None):
        """
        Initialize the schema generator.

        Args:
            table_name: Optional name for the table
        """
        self.table_name = table_name

    def generate_schema(self, columns: List[ColumnInfo],
                        source_file: Union[str, Path]) -> str:
        """Generate the SQL schema definition"""
        source_file = Path(source_file)
        if not self.table_name:
            self.table_name = self._derive_table_name(source_file)

        sql_lines = self._generate_header(source_file, len(columns))
        sql_lines.extend(self._generate_table_definition(columns))
        sql_lines.extend(self._generate_footer(columns))

        return '\n'.join(sql_lines)

    @staticmethod
    def _derive_table_name(file_path: Path) -> str:
        """Derive a table name from the file path"""
        base_name = file_path.stem.lower()
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', base_name)
        return f"tbl_{clean_name}" if clean_name[0].isdigit() else clean_name

    def _generate_header(self, source_file: Path, column_count: int) -> List[str]:
        """Generate the SQL header comments"""
        return [
            f"-- Schema generated for {source_file.name}",
            f"-- Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"-- Number of columns: {column_count}",
            "",
            "-- Table Definition"
        ]

    def _generate_table_definition(self, columns: List[ColumnInfo]) -> List[str]:
        """Generate the table creation SQL"""
        lines = [f'CREATE TABLE IF NOT EXISTS "{self.table_name}" (']

        column_defs = []
        for col in columns:
            definition = f'    "{col.name}" {col.sql_type}'
            if not col.nullable:
                definition += " NOT NULL"
            column_defs.append(definition)

        lines.extend([',\n'.join(column_defs)])
        lines.append(');')
        return lines

    def _generate_footer(self, columns: List[ColumnInfo]) -> List[str]:
        """Generate helpful footer comments including type mapping information"""
        footer = [
            "",
            "-- Notes:",
            "-- 1. Review and adjust data types and constraints as needed",
            "-- 2. Consider adding appropriate primary key constraint",
            "-- 3. Consider adding foreign key constraints if applicable",
            "-- 4. Add indexes based on your query patterns",
            "",
            "-- Original Type Mappings:"
        ]

        for col in columns:
            footer.append(f"-- {col.name}: {col.original_type} -> {col.sql_type}")

        return footer

