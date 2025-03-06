# src/arcaneflow/core/pipeline/builder.py
from typing import List, Optional
from ..interfaces.etl_node import ETLNode
from ..interfaces.context import PipelineContext

class PipelineBuilder:
    """Constructs linear pipelines with optional branching"""
    
    def __init__(self):
        self.nodes: List[ETLNode] = []
        self.source: Optional[ETLNode] = None
        self.sink: Optional[ETLNode] = None
        
    def set_source(self, source: ETLNode) -> 'PipelineBuilder':
        self.source = source
        return self
        
    def add_node(self, node: ETLNode) -> 'PipelineBuilder':
        self.nodes.append(node)
        return self
        
    def set_sink(self, sink: ETLNode) -> 'PipelineBuilder':
        self.sink = sink
        return self
        
    def build(self) -> 'Pipeline':
        """Construct executable pipeline"""
        if not self.source:
            raise ValueError("Pipeline source not defined")
        return Pipeline(self.source, self.nodes, self.sink)

class Pipeline:
    """Executable pipeline instance"""
    
    def __init__(self, source, nodes, sink):
        self.source = source
        self.nodes = nodes
        self.sink = sink
        
    def execute(self):
        """Run the entire pipeline"""
        context = PipelineContext()
        
        # Execute source
        context = self.source.execute(context)
        
        # Process transformations
        for node in self.nodes:
            context = node.execute(context)
            
        # Final sink
        if self.sink:
            context = self.sink.execute(context)
            
        return context