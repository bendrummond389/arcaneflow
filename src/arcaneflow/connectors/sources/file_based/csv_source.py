import pandas as pd

from arcaneflow.core.interfaces.context import PipelineContext
from ....core.interfaces.etl_node import ETLNode


class CSVSource(ETLNode):
    def __init__(self, file_path: str, **read_csv_kwargs) -> str:
        self.file_path = file_path
        self.read_csv_kwargs = read_csv_kwargs

    @property
    def node_id(self) -> str:
        return f"CSVSource_{self.file_path}"

    def execute(self, context: PipelineContext):
        context.data = pd.read_csv(self.file_path, **self.read_csv_kwargs)
        context.metadata["source"] = self.file_path
        return context
