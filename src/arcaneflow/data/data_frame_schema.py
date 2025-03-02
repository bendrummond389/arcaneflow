from pydantic import BaseModel, create_model
from typing import Dict, Any, Type, Optional
import pandas as pd
import numpy as np


class DataFrameSchema(BaseModel):
    columns: Dict[str, str]

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> "DataFrameSchema":
        type_mapping = {
            'int64': 'integer',
            'float64': 'float',
            'object': 'string',
            'bool': 'boolean',
            'datetime64[ns]': 'datetime'
        }
        return cls(columns={
            col: type_mapping.get(str(dtype), 'unknown')
            for col, dtype in df.dtypes.items()
        })


    def validate(self, df: pd.DataFrame) -> None:
        current_schema = self.from_dataframe(df)
        if current_schema != self:
            missing = set(self.columns) - set(current_schema.columns)
            extra = set(current_schema.columns) - set(self.columns)
            type_mismatch = {
                col: (current_schema.columns[col], expected)
                for col, expected in self.columns.items()
                if col in current_schema.columns and current_schema.columns[col] != expected
            }
            
            errors = []
            if missing: errors.append(f"Missing columns: {missing}")
            if extra: errors.append(f"Extra columns: {extra}")
            if type_mismatch: errors.append(f"Type mismatches: {type_mismatch}")
            
            raise ValueError(f"Schema validation failed: {'; '.join(errors)}")