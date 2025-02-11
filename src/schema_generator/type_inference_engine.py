from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import pandas as pd
from enum import Enum


class SQLType(Enum):
    """Supported SQL data types"""
    SMALLINT = "SMALLINT"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    NUMERIC = "NUMERIC"
    REAL = "REAL"
    DOUBLE_PRECISION = "DOUBLE PRECISION"
    TEXT = "TEXT"
    VARCHAR = "VARCHAR"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMPTZ = "TIMESTAMP WITH TIME ZONE"
    JSON = "JSON"
    JSONB = "JSONB"


class BaseTypeInference(ABC):
    """Abstract base class for type inference engines"""

    @abstractmethod
    def infer_type(self, data: Any, column_name: str) -> str:
        """Infer the SQL type from the data"""
        pass


class PostgreSQLTypeInference(BaseTypeInference):
    """PostgreSQL-specific type inference"""

    def infer_type(self, data: pd.Series, column_name: str) -> str:
        if data.empty:
            return SQLType.TEXT.value

        # Numeric type inference
        if pd.api.types.is_numeric_dtype(data):
            non_null = data.dropna()
            if all(non_null.apply(lambda x: float(x).is_integer())):
                max_val = non_null.max()
                min_val = non_null.min()

                if min_val >= -32768 and max_val <= 32767:
                    return SQLType.SMALLINT.value
                elif min_val >= -2147483648 and max_val <= 2147483647:
                    return SQLType.INTEGER.value
                return SQLType.BIGINT.value

            # Handle decimals
            decimal_places = non_null.apply(
                lambda x: len(str(x).split('.')[-1]) if '.' in str(x) else 0
            ).max()

            if decimal_places > 0:
                total_digits = non_null.apply(
                    lambda x: len(str(x).replace('.', ''))
                ).max()
                return f"{SQLType.NUMERIC.value}({total_digits},{decimal_places})"

            return SQLType.DOUBLE_PRECISION.value

        # Boolean type
        if pd.api.types.is_bool_dtype(data):
            return SQLType.BOOLEAN.value

        # DateTime types
        if pd.api.types.is_datetime64_any_dtype(data):
            return (SQLType.TIMESTAMPTZ.value
                    if data.dt.tz is not None
                    else SQLType.TIMESTAMP.value)

        # String types
        max_length = data.astype(str).str.len().max()
        return (f"{SQLType.VARCHAR.value}({max_length})"
                if max_length <= 255
                else SQLType.TEXT.value)