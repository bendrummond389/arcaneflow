import networkx as nx
from typing import Set

from .base_optimizer import BaseOptimizer


class RedundancyOptimizer(BaseOptimizer):
    """Optimizes graph by identifying redundant edges between nodes with matching schemas."""

    def identify_redundancies(self, graph: nx.DiGraph) -> Set[str]:
        """Detects redundant edges where connected nodes have identical schemas.

        Args:
            graph: Input directed graph with node schemas and edge node_ids

        Returns:
            Set of node_ids from edges connecting nodes with duplicate schemas
        """
        redundant_nodes = set()

        for source, target, edge_data in graph.edges(data=True):
            if graph.nodes[source]["schema"] == graph.nodes[target]["schema"]:
                redundant_nodes.add(edge_data.get("node_id"))

        return redundant_nodes
