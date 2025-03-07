from typing import Dict, FrozenSet, List, Set, Any, Optional
import networkx as nx


from ...pipeline.builder import Pipeline


class TransformationGraphBuilder:
    def __init__(self, schema_manager):
        self.graph = nx.DiGraph()
        self.schema_manager = schema_manager

    def build_graph(self, pipeline: "Pipeline") -> nx.DiGraph:
        all_nodes = self._collect_pipeline_nodes(pipeline)

        schema_state: Set = set()
        initial_schema = frozenset(schema_state)
        initial_state = self._add_schema_node(initial_schema)
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
            schema_state = self.schema_manager.calculate_next_state(
                schema_state, signature
            )

        return self.graph

    def _add_schema_node(self, schema: FrozenSet) -> str:
        node_name = self.schema_manager.get_or_create_node(schema)
        self.graph.add_node(node_name, schema=schema)
        return node_name

    def _collect_pipeline_nodes(self, pipeline: "Pipeline") -> List:
        all_nodes = []
        if pipeline.source:
            all_nodes.append(pipeline.source)
        all_nodes.extend(pipeline.nodes)
        if pipeline.sink:
            all_nodes.append(pipeline.sink)
        return all_nodes

    def _get_node_signature(self, node) -> Optional[Any]:

        if not hasattr(node, "get_transformation_signature"):
            return None

        try:
            return node.get_transformation_signature()
        except NotImplementedError:
            return None

    def _process_transformation(
        self, schema_state: Set, signature, current_state: str, node_id: str
    ) -> str:

        next_schema_state = self.schema_manager.calculate_next_state(
            schema_state, signature
        )
        next_schema_frozen = frozenset(next_schema_state)
        next_state = self._add_schema_node(next_schema_frozen)

        self.graph.add_edge(
            current_state, next_state, node_id=node_id, signature=signature
        )
        return next_state
