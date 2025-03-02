from abc import ABC, abstractmethod
from typing import Optional, Protocol
import pandas as pd

from arcaneflow.data.schema import DataFrameSchema

class DataTransformer(Protocol):
    """Protocol defining the interface for data transformers"""
    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        ...

class BaseTransformation(ABC):
    """Base class for all transformations"""
    def __init__(self, 
                 input_schema: Optional[DataFrameSchema] = None,
                 output_schema: Optional[DataFrameSchema] = None):
        self.input_schema = input_schema
        self.output_schema = output_schema

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.input_schema:
            self.input_schema.validate(df)
        
        transformed = self._transform(df)

        if self.output_schema:
            self.output_schema.validate(transformed)
            
        return transformed

    @abstractmethod
    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        pass
