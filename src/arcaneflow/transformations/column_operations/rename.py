from typing import Dict

from ...core.interfaces.transformation_signature import TransformationSignature
from ...core.interfaces.etl_node import ETLNode
from ...core.interfaces.context import PipelineContext
import pandas as pd

class ColumnRenamer(ETLNode):
    def __init__(self, column_mapping: Dict[str, str]):
        self.column_mapping = column_mapping
        self._validate_mapping()

    @property
    def node_id(self) -> str:
        return f"ColumnRenamer_{hash(frozenset(self.column_mapping.items()))}"

    def execute(self, context: PipelineContext) -> PipelineContext:
        if not isinstance(context.data, pd.DataFrame):
            raise TypeError("ColumnRenamer requires pandas DataFrame input")
            
        context.data = context.data.rename(columns=self.column_mapping)
        context.metadata['columns'] = list(context.data.columns)
        return context
        
    def _validate_mapping(self):
        if not isinstance(self.column_mapping, dict):
            raise TypeError("Column mapping must be a dictionary")
        if len(self.column_mapping) == 0:
            raise ValueError("Column mapping cannot be empty")

    def get_transformation_signature(self) -> TransformationSignature:
        input_schema = set(self.column_mapping.keys())
        output_schema = set(self.column_mapping.values())
        
        return TransformationSignature(
            input_schema=input_schema,
            output_schema=output_schema,
            transformation_type="column_rename",
            properties={"mapping": self.column_mapping}
        )