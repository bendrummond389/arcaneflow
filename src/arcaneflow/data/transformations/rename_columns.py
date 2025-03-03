from typing import Dict, Optional

import pandas as pd
from arcaneflow.data.transformations.base import BaseTransformation
from arcaneflow.data.schema import DataFrameSchema

class RenameColumns(BaseTransformation):
    """Transformation that renames DataFrame columns.
    
    When columns are renamed, the transformation maintains the data types
    from the input schema in the output schema.
    
    Attributes:
        column_mapping: Dictionary mapping original column names to new column names
    """
    def __init__(self, 
                 column_mapping: Dict[str, str], 
                 input_schema: Optional[DataFrameSchema] = None):
        """Initialize the column renaming transformation.
        
        Args:
            column_mapping: Dictionary mapping original column names to new column names
            input_schema: Optional schema for validating input data before transformation
        """
        self.column_mapping = column_mapping
        
        # Initialize with input schema if provided
        super().__init__(input_schema=input_schema)
        
        # Build the output schema based on input schema and column mapping
        self._build_output_schema()

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename columns according to the specified mapping.
        
        Args:
            df: Input DataFrame with columns to rename
            
        Returns:
            DataFrame with renamed columns
        """
        return df.rename(columns=self.column_mapping)
    
    def _build_output_schema(self) -> None:
        """Build output schema based on input schema and column mapping.
        
        If input_schema is provided, derives the output schema by applying
        the column renaming while preserving column data types.
        """
        if self.input_schema:
            # Create a new schema with columns renamed according to column_mapping
            new_columns = {}
            
            # Copy columns that aren't being renamed
            for col_name, col_type in self.input_schema.columns.items():
                if col_name in self.column_mapping:
                    # Use the new name for columns in the mapping
                    new_name = self.column_mapping[col_name]
                    new_columns[new_name] = col_type
                else:
                    # Keep original name for columns not in the mapping
                    new_columns[col_name] = col_type
                    
            self.output_schema = DataFrameSchema(columns=new_columns)