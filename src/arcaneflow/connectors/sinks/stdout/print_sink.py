from typing import Optional
from ....core.interfaces.context import PipelineContext
from ....core.interfaces.etl_node import ETLNode

class PrintSink(ETLNode):
    def __init__(self, limit: Optional[int] = 5) -> None:
        """
        Initialize a simple PrintSink that outputs data to console for testing.
        
        Args:
            limit (Optional[int]): Number of rows to print using df.head(). Default is 5.
        """
        self.limit = limit
    
    @property
    def node_id(self) -> str:
        return "PrintSink"
    
    def execute(self, context: PipelineContext) -> PipelineContext:
        """Print the data for testing purposes using pandas' built-in head() method"""
        data = context.data
        row_count = len(data)
        
        print(f"\n===== PrintSink: {row_count} total rows =====")
        print(data.head(self.limit))
        print(f"===== End of PrintSink output =====\n")
        
        # Add metadata about the operation
        context.metadata['printed_rows'] = min(row_count, self.limit)
        context.metadata['total_rows'] = row_count
            
        return context