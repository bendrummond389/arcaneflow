import networkx as nx
from typing import Set

from .base_optimizer import BaseOptimizer


class CycleOptimizer(BaseOptimizer):
    """Optimizer identifying redundant nodes in cycles within a directed graph."""

    def identify_redundancies(self, graph: nx.DiGraph) -> Set[str]:
        """Identify redundant nodes by examining cycles in the graph.

        For each cycle detected, extracts 'node_id' from edges and marks them as redundant.

        Args:
            graph: The directed graph to analyze for cyclical redundancies.

        Returns:
            A set of redundant node IDs found in cycles.
        """
        redundant_nodes = set()
        try:
            for cycle in nx.simple_cycles(graph):
                # Generate consecutive node pairs in the cycle
                next_nodes = cycle[1:] + cycle[:1]
                for u, v in zip(cycle, next_nodes):
                    edge_data = graph.get_edge_data(u, v)
                    if edge_data:
                        redundant_nodes.add(edge_data["node_id"])
        except nx.NetworkXNoCycle:
            pass  # No cycles present

        return redundant_nodes
