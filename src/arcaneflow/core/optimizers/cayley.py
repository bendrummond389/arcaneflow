from typing import Dict, FrozenSet, List, Set
import networkx as nx

from ..pipeline.builder import Pipeline


class CayleyGraphOptimizer:
    def __init__(self):
        self.graph = nx.DiGraph()
        self.schema_to_node_name: Dict[FrozenSet, str] = {}

    def build_transformation_graph(self, pipeline: "Pipeline") -> None:
        all_nodes = self._collect_pipeline_nodes(pipeline)

        schema_state: Set = set()
        initial_schema = frozenset(schema_state)
        initial_state = self._get_or_create_schema_node(initial_schema)
        current_state = initial_state

        for node in all_nodes:
            signature = self._get_node_signature(node)
            if not signature:
                continue

            node_id = node.node_id
            next_state = self._process_transformation(
                schema_state, signature, current_state, node_id
            )

            current_state = next_state
            schema_state = self._calculate_schema_state(schema_state, signature)

    def _collect_pipeline_nodes(self, pipeline: "Pipeline") -> List:
        all_nodes = []
        if pipeline.source:
            all_nodes.append(pipeline.source)
        all_nodes.extend(pipeline.nodes)
        if pipeline.sink:
            all_nodes.append(pipeline.sink)
        return all_nodes

    def _get_node_signature(self, node) -> object:
        """Gets transformation signature from a node if available."""
        if not hasattr(node, "get_transformation_signature"):
            return None

        try:
            return node.get_transformation_signature()
        except NotImplementedError:
            return None

    def _get_or_create_schema_node(self, schema: FrozenSet) -> str:
        """Gets existing schema node or creates a new one."""
        if schema in self.schema_to_node_name:
            return self.schema_to_node_name[schema]

        node_name = f"schema_{hash(schema)}"
        self.graph.add_node(node_name, schema=schema)
        self.schema_to_node_name[schema] = node_name
        return node_name

    def _calculate_schema_state(self, current_schema: Set, signature) -> Set:
        """Calculates the new schema state after applying a transformation."""
        unchanged_columns = current_schema - signature.input_schema
        return unchanged_columns.union(signature.output_schema)

    def _process_transformation(
        self, schema_state: Set, signature, current_state: str, node_id: str
    ) -> str:
        """Processes a transformation and adds it to the graph."""
        next_schema_state = self._calculate_schema_state(schema_state, signature)
        next_schema_frozen = frozenset(next_schema_state)
        next_state = self._get_or_create_schema_node(next_schema_frozen)

        self.graph.add_edge(
            current_state, next_state, node_id=node_id, signature=signature
        )
        return next_state

    def identify_redundancies(self) -> List[str]:
        """Identify redundant transformations in the pipeline."""
        redundant_nodes = set()

        # Find nodes that don't change the schema
        self._find_no_change_redundancies(redundant_nodes)

        # Find cycles in the graph
        self._find_cycle_redundancies(redundant_nodes)

        return list(redundant_nodes)

    def _find_no_change_redundancies(self, redundant_nodes: Set[str]) -> None:
        """Find transformations that don't change the schema."""
        for u, v, data in self.graph.edges(data=True):
            if self.graph.nodes[u]["schema"] == self.graph.nodes[v]["schema"]:
                redundant_nodes.add(data.get("node_id"))

    def _find_cycle_redundancies(self, redundant_nodes: Set[str]) -> None:
        """Find transformations that are part of cycles."""
        cycles = nx.simple_cycles(self.graph)

        for cycle in cycles:
            for i in range(len(cycle)):
                u = cycle[i]
                v = cycle[(i + 1) % len(cycle)]
                edge_data = self.graph.get_edge_data(u, v)
                if edge_data:
                    redundant_nodes.add(edge_data["node_id"])

    def optimize_pipeline(self, pipeline: "Pipeline") -> "Pipeline":
        """Create an optimized version of the given pipeline."""
        # Reset the graph before building a new one
        self.graph = nx.DiGraph()
        self.schema_to_node_name = {}

        # Build the transformation graph
        self.build_transformation_graph(pipeline)

        # Identify redundancies
        redundant_nodes = set(self.identify_redundancies())

        if not redundant_nodes:
            # No optimizations found
            return pipeline

        # Create optimized pipeline with non-redundant nodes
        optimized_nodes = [
            node for node in pipeline.nodes if node.node_id not in redundant_nodes
        ]

        return Pipeline(pipeline.source, optimized_nodes, pipeline.sink)
