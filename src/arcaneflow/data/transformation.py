from abc import ABC, abstractmethod
from typing import Optional
import pandas as pd

from arcaneflow.data.data_frame_schema import DataFrameSchema
from arcaneflow.data.transformation_chain import TransformationChain

class BaseTransformation(ABC):
    input_schema: Optional[DataFrameSchema]
    output_schema: Optional[DataFrameSchema]

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

    def chain(self, next_transformation: 'BaseTransformation') -> 'TransformationChain':
        return TransformationChain([self, next_transformation])