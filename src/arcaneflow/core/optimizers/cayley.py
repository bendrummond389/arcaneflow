from typing import List
import networkx as nx

from ..pipeline.builder import Pipeline

class CayleyGraphOptimizer:
    def __init__(self):
        self.graph = nx.DiGraph()

    def build_transformation_graph(self, pipeline: 'Pipeline') -> None:
        all_nodes = []
        if pipeline.source:
            all_nodes.append(pipeline.source)
        all_nodes.extend(pipeline.nodes)
        if pipeline.sink:
            all_nodes.append(pipeline.sink)

        schema_state = set()
        schema_to_node_name = {}

        initial_schema = frozenset(schema_state)
        initial_state = f"schema_{hash(initial_schema)}"
        self.graph.add_node(initial_state, schema=initial_schema)
        schema_to_node_name[initial_schema] = initial_state
        current_state = initial_state

        for node in all_nodes:
            if not hasattr(node, 'get_transformation_signature'):
                continue

            try:
                signature = node.get_transformation_signature()
            except NotImplementedError:
                continue

            node_id = node.node_id

       
            unchanged_columns = schema_state - signature.input_schema
            next_schema_state = unchanged_columns.union(signature.output_schema)
            next_schema_frozen = frozenset(next_schema_state)

           
            if next_schema_frozen in schema_to_node_name:
                next_state = schema_to_node_name[next_schema_frozen]
            else:
                next_state = f"schema_{hash(next_schema_frozen)}"
                self.graph.add_node(next_state, schema=next_schema_frozen)
                schema_to_node_name[next_schema_frozen] = next_state

            self.graph.add_edge(current_state, next_state, node_id=node_id, signature=signature)

            # Move to next node
            current_state = next_state
            schema_state = next_schema_state


    def identify_redundancies(self) -> List[str]:
        """Identify redundant transformations in the pipeline."""
        redundant_nodes = set()

        for u, v, data in self.graph.edges(data=True):
            node_id = data.get('node_id')
            signature = data.get('signature')

            if self.graph.nodes[u]['schema'] == self.graph.nodes[v]['schema']:
                redundant_nodes.add(node_id)

        cycles = nx.simple_cycles(self.graph)

        for cycle in cycles:
            for i in range(len(cycle)):
                u = cycle[i]
                v = cycle[(i + 1) % len(cycle)]
                edge_data = self.graph.get_edge_data(u, v)
                if edge_data:
                    redundant_nodes.add(edge_data['node_id'])


        return list(redundant_nodes)
    
    def optimize_pipeline(self, pipeline: 'Pipeline') -> 'Pipeline':
        """Create an optimized version of the given pipeline."""
        # Build the transformation graph
        self.build_transformation_graph(pipeline)
        
        # Identify redundancies
        redundant_nodes = set(self.identify_redundancies())
        
        if not redundant_nodes:
            # No optimizations found
            return pipeline
            
        # Filter out redundant nodes
        optimized_nodes = []
        for node in pipeline.nodes:
            if node.node_id not in redundant_nodes:
                optimized_nodes.append(node)
        
        # Create optimized pipeline
        return Pipeline(pipeline.source, optimized_nodes, pipeline.sink)


