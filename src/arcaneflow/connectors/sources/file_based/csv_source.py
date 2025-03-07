import pandas as pd
from ....core.interfaces.etl_node import ETLNode

class CSVSource(ETLNode):
    def node_id(self, file_path: str, **read_csv_kwargs) -> str:
        self.file_path = file_path
        self.read_csv_kwargs = read_csv_kwargs
    