from typing import List, Union
import pandas as pd

from .transformations.base import BaseTransformation


class TransformationChain:
    """Chain multiple transformations together"""

    def __init__(
        self, transformations: List[Union[BaseTransformation, "TransformationChain"]]
    ):
        self.transformations = transformations
        self._validate_chain()

    def _validate_chain(self):
        # Iterate over the transformations in the chain (excluding the first one).
        for i in range(1, len(self.transformations)):

            # Get the output schema of the previous transformation and input schema of the current transformation.
            prev = self.transformations[i - 1]
            curr = self.transformations[i]

            prev_output = getattr(prev, "output_schema", None)
            curr_input = getattr(curr, "input_schema", None)

            # If both schemas are defined (i.e., not None), check if they are equal.
            if prev_output and curr_input and prev_output != curr_input:

                # Raise a ValueError with an error message describing the schema mismatch.
                raise ValueError(
                    f"Schema mismatch between step {i-1} and {i}:\n"
                    f"Step {i-1} output: {prev_output}\n"
                    f"Step {i} input: {curr_input}"
                )

    def append(
        self, transformation: Union[BaseTransformation, "TransformationChain"]
    ) -> "TransformationChain":
        new_chain = self.transformations + [transformation]
        return TransformationChain(new_chain)

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        for transformation in self.transformations:
            result = transformation(result)
        return result
