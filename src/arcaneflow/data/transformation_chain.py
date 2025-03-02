from typing import List
from arcaneflow.data.transformation import BaseTransformation
import pandas as pd


class TransformationChain:
    def __init__(self, transformations: List[BaseTransformation]):
        self.transformations = transformations
        self._validate_chain()

    def _validate_chain(self):
        for i in range(1, len(self.transformations)):
            prev_output = self.transformations[i-1].output_schema
            curr_input = self.transformations[i].input_schema
            
            if prev_output and curr_input and prev_output != curr_input:
                raise ValueError(
                    f"Schema mismatch between step {i-1} and {i}:\n"
                    f"Step {i-1} output: {prev_output}\n"
                    f"Step {i} input: {curr_input}"
                )
            
    def append(self, transformation: BaseTransformation):
        new_chain = self.transformations + [transformation]
        return TransformationChain(new_chain)
    
    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        for transformation in self.transformations:
            df = transformation(df)
        return df