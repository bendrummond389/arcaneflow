from typing import List, Optional
from ..interfaces.etl_node import ETLNode
from .context import PipelineContext

class Pipeline:
    def __init__(self, source, nodes, sink):
        self.source = source
        self.nodes = nodes
        self.sink = sink

    def execute(self):
        context = PipelineContext()
        context = self.source.execute(context)

        for node in self.nodes:
            context = node.execute(context)

        if self.sink:
            context = self.sink.execute(context)

        return context

class PipelineBuilder:
    def __init__(self):
        self.source: Optional[ETLNode] = None
        self.nodes: List[ETLNode] = []
        self.sink: Optional[ETLNode] = None
        

    def set_source(self, source_node: ETLNode) -> 'PipelineBuilder': 
        self.source = source_node
        return self

    def add_node(self, node: ETLNode) -> 'PipelineBuilder':
        self.nodes.append(node)
        return self

    def set_sink(self, sink: ETLNode) -> 'PipelineBuilder':
        self.sink = sink
        return sink
    
    def build(self) -> Pipeline:
        if not self.source:
            raise ValueError("Pipeline source not defined")
        return Pipeline(self.source, self.nodes, self.sink)


