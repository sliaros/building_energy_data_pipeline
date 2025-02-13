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
        'id': 'BIGINT',
        '_id': 'BIGINT',
        'zip': 'VARCHAR(10)',
        'zipcode': 'VARCHAR(10)',
        'postal': 'VARCHAR(10)',
        'email': 'VARCHAR(255)',
        'phone': 'VARCHAR(20)',
    }

    def infer_type(self, data: pd.Series, column_name: str) -> str:
        if data.empty:
            return SQLType.TEXT.value

        # Drop null values for better analysis
        non_null = data.dropna()

        # Numeric type inference
        if pd.api.types.is_numeric_dtype(data):
            # Check if all values are integers
            is_integer = non_null.apply(lambda x: isinstance(x, (int, np.integer)) or float(x).is_integer()).all()

            if is_integer:

                max_val = non_null.max()
                min_val = non_null.min()

                if min_val >= -32768 and max_val <= 32767:
                    return SQLType.SMALLINT.value
                elif min_val >= -2147483648 and max_val <= 2147483647:
                    return SQLType.INTEGER.value
                return SQLType.BIGINT.value

            # Handle floating-point numbers
            decimal_places = non_null.apply(
                lambda x: len(str(x).split('.')[-1]) if '.' in str(x) and not str(x).endswith(".0") else 0
            ).max()

            if decimal_places==0:
                return SQLType.INTEGER.value  # Edge case where numbers look like floats but are whole numbers

            # If precision is limited, use REAL or DOUBLE PRECISION
            if decimal_places <= 6:
                return SQLType.REAL.value
            if decimal_places <= 15:
                return SQLType.DOUBLE_PRECISION.value

            # For very precise decimals, use NUMERIC
            total_digits = non_null.apply(lambda x: len(str(x).replace('.', ''))).max()
            return f"{SQLType.NUMERIC.value}({total_digits},{decimal_places})"

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