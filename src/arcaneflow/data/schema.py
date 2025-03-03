"""
Data validation utilities for pandas DataFrames using Pydantic models.

Classes:
    DataFrameSchema: Validates column presence and data types in pandas DataFrames.
"""

from pydantic import BaseModel
from typing import Dict
import pandas as pd


class DataFrameSchema(BaseModel):
    """
    Schema definition for validating pandas DataFrame structure and data types.

    Attributes:
        columns (Dict[str, str]): Dictionary mapping column names to expected data types.
            Supported types: 'integer', 'float', 'string', 'boolean', 'datetime', 'unknown'.

    Methods:
        from_dataframe: Create schema from an existing DataFrame's structure
        validate: Check if a DataFrame matches the schema
    """

    columns: Dict[str, str]

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> "DataFrameSchema":
        """
        Generate a DataFrameSchema from an existing DataFrame's structure.

        Maps pandas dtypes to simplified type names:
        - int64 -> 'integer'
        - float64 -> 'float'
        - object -> 'string'
        - bool -> 'boolean'
        - datetime64[ns] -> 'datetime'
        - Other types -> 'unknown'

        Args:
            df (pd.DataFrame): DataFrame to analyze for schema creation

        Returns:
            DataFrameSchema: Schema representing the DataFrame's structure
        """
        # Map pandas data types to simplified type names
        type_mapping = {
            "int64": "integer",
            "float64": "float",
            "object": "string",
            "bool": "boolean",
            "datetime64[ns]": "datetime",
        }

        return cls(
            columns={
                col: type_mapping.get(str(dtype), "unknown")
                for col, dtype in df.dtypes.items()
            }
        )

    def validate(self, df: pd.DataFrame) -> None:
        """
        Validate a DataFrame against the schema.

        Checks for:
        - Missing required columns
        - Extra unexpected columns
        - Data type mismatches in existing columns

        Args:
            df (pd.DataFrame): DataFrame to validate against the schema

        Raises:
            ValueError: If any schema violations are found, with detailed error message

        Example:
            >>> schema.validate(df)
            ValueError: Schema validation failed: Missing columns: {'age'};
            Extra columns: {'temp_column'}; Type mismatches: {'price': ('float', 'integer')}
        """
        # Generate schema from the current DataFrame
        current_schema = self.from_dataframe(df)

        # Check for schema mismatches
        if current_schema != self:
            # Identify missing columns (in schema but not in DataFrame)
            missing = set(self.columns) - set(current_schema.columns)

            # Identify extra columns (in DataFrame but not in schema)
            extra = set(current_schema.columns) - set(self.columns)

            # Find columns with type mismatches
            type_mismatch = {
                col: (current_schema.columns[col], expected)
                for col, expected in self.columns.items()
                if col in current_schema.columns
                and current_schema.columns[col] != expected
            }

            # Build error messages
            errors = []
            if missing:
                errors.append(f"Missing columns: {missing}")
            if extra:
                errors.append(f"Extra columns: {extra}")
            if type_mismatch:
                errors.append(f"Type mismatches: {type_mismatch}")

            # Raise comprehensive error if any issues found
            raise ValueError(f"Schema validation failed: {'; '.join(errors)}")
