from typing import Dict

import pandas as pd
from arcaneflow.data.base import BaseTransformation
from arcaneflow.data.schema import DataFrameSchema


class RenameColumns(BaseTransformation):
    """Transformation that renames DataFrame columns"""
    def __init__(self, column_mapping: Dict[str, str]):
        self.column_mapping = column_mapping
        self.input_schema = None  # No schema requirements for input
        self._build_output_schema()
        
    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.column_mapping)
    
    def _build_output_schema(self):
        if self.input_schema:
            self.output_schema = DataFrameSchema(columns={
                new_name: self.input_schema.columns[old_name]
                for old_name, new_name in self.column_mapping.items()
            })