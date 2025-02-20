from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Union
import pandas as pd

from .base_scema_analyzer import BaseSchemaAnalyzer, BaseColumnInfo
from .sampling_strategies import BaseSamplingStrategy, RandomSamplingStrategy
from .type_inference_engine import PostgreSQLTypeInference


class PostgreSQLSchemaAnalyzer(BaseSchemaAnalyzer):
    """
    PostgreSQL-specific schema analyzer implementation.
    """

    def __init__(
            self,
            sampling_strategy: Optional[BaseSamplingStrategy] = None,
            type_inference: Optional[PostgreSQLTypeInference] = None
    ):
        self.sampling_strategy = sampling_strategy or RandomSamplingStrategy()
        self.type_inference = type_inference or PostgreSQLTypeInference()

    def analyze_schema(self, data_source: Union[str, Path]) -> List[BaseColumnInfo]:
        """
        Analyze the schema of a data source.

        Args:
            data_source: Path to the data file

        Returns:
            List of column information objects
        """
        self.validate_source(data_source)
        sample_data = self.get_sample_data(data_source, sample_size=10000)

        columns = []
        for column_name in sample_data.columns:
            column_data = sample_data[column_name]

            column_info = BaseColumnInfo(
                name=self._clean_column_name(column_name),
                data_type=self.type_inference.infer_type(column_data, column_name),
                nullable=column_data.isnull().any(),
                original_type=str(column_data.dtype),
                stats=self._compute_column_stats(column_data),
                sample_values=column_data.dropna().unique()[:5].tolist(),
                metadata=self._get_column_metadata(column_data)
            )

            columns.append(column_info)

        return columns

    def validate_source(self, data_source: Union[str, Path]) -> bool:
        """Validate the data source"""
        path = Path(data_source)
        if not path.exists():
            raise FileNotFoundError(f"Data source not found: {data_source}")
        if not path.suffix.lower() in ['.csv', '.parquet']:
            raise ValueError(f"Unsupported file type: {path.suffix}")
        return True

    def get_sample_data(self, data_source: Union[str, Path], sample_size: int) -> pd.DataFrame:
        """Get a sample of data for analysis"""
        return self.sampling_strategy.sample_data(data_source, sample_size)

    def _clean_column_name(self, name: str) -> str:
        """Clean column name for PostgreSQL"""
        import re
        clean = re.sub(r'[^a-zA-Z0-9_]', '_', name.lower())
        return f"col_{clean}" if clean[0].isdigit() else clean

    def _compute_column_stats(self, data: pd.Series) -> Dict:
        """Compute statistical information about a column"""
        return {
            "count": len(data),
            "null_count": data.isnull().sum(),
            "unique_count": data.nunique(),
            "memory_usage": data.memory_usage(deep=True),
            "unique_ratio": data.nunique() / len(data) if len(data) > 0 else 0
        }

    def _get_column_metadata(self, data: pd.Series) -> Dict:
        """Get additional metadata about a column"""
        metadata = {
            "is_unique": data.nunique()==len(data),
            "is_monotonic": data.is_monotonic_increasing or data.is_monotonic_decreasing,
            "contains_nulls": data.isnull().any()
        }

        # Add recommendations based on column characteristics
        recommendations = []
        if metadata["is_unique"]:
            recommendations.append("Consider as primary key candidate")
        if metadata["is_monotonic"]:
            recommendations.append("Consider adding an index")
        if data.nunique() / len(data) < 0.1:
            recommendations.append("Low cardinality - consider using as categorical")

        metadata["recommendations"] = recommendations
        return metadata


class SQLSchemaGenerator:
    """Generates SQL schema definitions from analyzed column information."""

    def __init__(self, table_name: Optional[str] = None):
        """Initialize the SQL schema generator.

        Args:
            table_name: Optional name for the table. If not provided, will be derived from the source file.
        """
        self.table_name = table_name

    def generate_schema(
            self,
            columns: List[BaseColumnInfo],
            source_file: Union[str, Path]
    ) -> str:
        """Generate SQL schema definition.

        Args:
            columns: List of column information objects
            source_file: Path to the source data file

        Returns:
            Complete SQL schema as a string
        """
        source_file_path = Path(source_file)
        table_name = self.table_name or self._derive_table_name(source_file_path)

        sql_parts = []
        sql_parts.extend(self._generate_header(source_file_path, len(columns)))
        sql_parts.append(f'CREATE TABLE IF NOT EXISTS "{table_name}" (')

        column_definitions = self._generate_column_definitions(columns)
        sql_parts.append(',\n'.join(column_definitions))
        sql_parts.append(');')

        sql_parts.extend(self._generate_footer(columns))

        return '\n'.join(sql_parts)

    def _generate_column_definitions(
            self,
            columns: List[BaseColumnInfo],
            primary_key: Optional[str] = None
    ) -> List[str]:
        """Generate SQL column definitions with optional primary key constraint.

        Args:
            columns: List of column information objects
            primary_key: Optional column name to use as primary key

        Returns:
            List of formatted column definition strings
        """
        definitions = []

        # Generate individual column definitions
        for column in columns:
            definition = f'    "{column.name}" {column.data_type}'
            if not column.nullable:
                definition += " NOT NULL"
            definitions.append(definition)

        # Add primary key constraint as a separate definition if provided
        if primary_key:
            definitions.append(f"    PRIMARY KEY ({primary_key})")

        return definitions

    @staticmethod
    def _derive_table_name(file_path: Path) -> str:
        """Derive table name from file path.

        Args:
            file_path: Path object for the source file

        Returns:
            Derived table name as string
        """
        import re
        base_name = file_path.stem.lower()
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', base_name)
        return f"tbl_{clean_name}" if clean_name[0].isdigit() else clean_name

    @staticmethod
    def _generate_header(source_file: Path, column_count: int) -> List[str]:
        """Generate SQL header comments.

        Args:
            source_file: Path object for the source file
            column_count: Number of columns in the schema

        Returns:
            List of header comment strings
        """
        return [
            f"-- Schema generated for {source_file.name}",
            f"-- Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"-- Number of columns: {column_count}",
            "",
            "-- Table Definition"
        ]

    @staticmethod
    def _generate_footer(columns: List[BaseColumnInfo]) -> List[str]:
        """Generate footer with additional column information.

        Args:
            columns: List of column information objects

        Returns:
            List of footer comment strings
        """
        footer = [
            "",
            "-- Column Information:"
        ]

        for column in columns:
            footer.extend([
                f"-- {column.name}:",
                f"--   Type: {column.original_type} -> {column.data_type}",
                f"--   Nullable: {column.nullable}",
                f"--   Unique Values: {column.stats['unique_count']}",
                "--   Recommendations:"
            ])

            if column.metadata and column.metadata.get('recommendations'):
                for recommendation in column.metadata['recommendations']:
                    footer.append(f"--     * {recommendation}")

            footer.append("--")

        return footer

    def generate_schema_from_columns(self, table_name: str, columns: List[Dict[str, str]]) -> str:
        """Generate a SQL CREATE TABLE statement from column definitions."""
        try:
            column_definitions = []
            for col in columns:
                definition = f"{col['name']} {col['type']}"
                if col.get('nullable') is False:
                    definition += " NOT NULL"
                if col.get('default'):
                    definition += f" DEFAULT {col['default']}"
                column_definitions.append(definition)
            return f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {', '.join(column_definitions)}
                );
            """
        except Exception as e:
            self._logger.error(f"Schema generation failed: {str(e)}")
            raise