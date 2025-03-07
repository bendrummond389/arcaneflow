from typing import Dict, FrozenSet, List, Set
import networkx as nx

from .graph.transformation_graph_builder import TransformationGraphBuilder

from .graph.schema_state_manager import SchemaStateManager

from .strategies import BaseOptimizer, CycleOptimizer, RedundancyOptimizer

from ..pipeline.builder import Pipeline


class CayleyGraphOptimizer:
    """
    Main optimizer that composes various optimization strategies to improve ETL pipelines.
    Uses a graph-based approach to identify and eliminate redundant transformations.
    """
    
    def __init__(self, optimizers: List[BaseOptimizer] = None):
        """
        Initialize the optimizer with strategies.
        
        Args:
            optimizers: List of optimization strategies to apply. If None, default strategies are used.
        """
        self.schema_manager = SchemaStateManager()
        self.graph_builder = TransformationGraphBuilder(self.schema_manager)
        
        if optimizers is None:
            self.optimizers = [
                RedundancyOptimizer(),
                CycleOptimizer()
            ]
        else:
            self.optimizers = optimizers
    
    def optimize_pipeline(self, pipeline: "Pipeline") -> "Pipeline":

        # Build the transformation graph
        graph = self.graph_builder.build_graph(pipeline)
        
        # Collect redundancies from all optimization strategies
        redundant_nodes = set()
        for optimizer in self.optimizers:
            redundant_nodes.update(optimizer.identify_redundancies(graph))
        
        if not redundant_nodes:
            # No optimizations found
            return pipeline
            
        # Create optimized pipeline with non-redundant nodes
        optimized_nodes = [
            node for node in pipeline.nodes if node.node_id not in redundant_nodes
        ]
        
        return Pipeline(pipeline.source, optimized_nodes, pipeline.sink)
        
    def get_redundant_nodes(self, pipeline: "Pipeline") -> List[str]:

        # Build the transformation graph
        graph = self.graph_builder.build_graph(pipeline)
        
        # Collect redundancies from all optimization strategies
        redundant_nodes = set()
        for optimizer in self.optimizers:
            redundant_nodes.update(optimizer.identify_redundancies(graph))
            
        return list(redundant_nodes)