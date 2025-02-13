from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import pandas as pd
from enum import Enum
import numpy as np


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

    # Known column patterns and their types
    COLUMN_TYPE_PATTERNS = {
        'lat': 'NUMERIC(10,6)',
        'latitude': 'NUMERIC(10,6)',
        'lng': 'NUMERIC(10,6)',
        'longitude': 'NUMERIC(10,6)',
        'price': 'NUMERIC(15,2)',
        'amount': 'NUMERIC(15,2)',
        'cost': 'NUMERIC(15,2)',
        'zip': 'VARCHAR(10)',
        'zipcode': 'VARCHAR(10)',
        'postal': 'VARCHAR(10)',
        'email': 'VARCHAR(255)',
        'phone': 'VARCHAR(20)',
    }

    def __init__(self, sample_size: int = 10000):
        self.sample_size = sample_size

    def _get_predefined_type(self, column_name: str) -> Optional[str]:
        """Check if column name matches known patterns"""
        column_lower = column_name.lower()

        # Direct matches
        if column_lower in self.COLUMN_TYPE_PATTERNS:
            return self.COLUMN_TYPE_PATTERNS[column_lower]

        # Pattern matches
        for pattern, sql_type in self.COLUMN_TYPE_PATTERNS.items():
            if pattern in column_lower:
                return sql_type

        return None

    def infer_type(self, data: pd.Series, column_name: str) -> str:
        """Infer the SQL type with enhanced handling for numeric types"""
        # Check for predefined types first
        predefined_type = self._get_predefined_type(column_name)
        if predefined_type:
            return predefined_type

        if data.empty:
            return SQLType.TEXT.value

        # Drop null values for better analysis
        non_null = data.dropna()

        # Numeric type inference
        if pd.api.types.is_numeric_dtype(data):
            # Check if all values are integers
            is_integer = non_null.apply(
                lambda x: isinstance(x, (int, np.integer)) or
                          (isinstance(x, float) and x.is_integer())
            ).all()

            if is_integer:
                max_val = non_null.max()
                min_val = non_null.min()

                if min_val >= -32768 and max_val <= 32767:
                    return SQLType.SMALLINT.value
                elif min_val >= -2147483648 and max_val <= 2147483647:
                    return SQLType.INTEGER.value
                return SQLType.BIGINT.value

            # Handle floating-point numbers more conservatively
            decimal_places = non_null.apply(
                lambda x: len(str(float(x)).split('.')[-1])
                if '.' in str(float(x)) and not str(float(x)).endswith('.0')
                else 0
            ).max()

            if decimal_places==0:
                return SQLType.INTEGER.value

            # Use DOUBLE PRECISION for most floating-point numbers
            if 'price' in column_name.lower() or 'amount' in column_name.lower():
                return 'NUMERIC(15,2)'
            elif decimal_places <= 6:
                return 'NUMERIC(12,6)'  # Good for coordinates and most measurements
            else:
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