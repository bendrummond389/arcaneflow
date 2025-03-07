import networkx as nx
from typing import Set

from .base_optimizer import BaseOptimizer


class CycleOptimizer(BaseOptimizer):

    
    def identify_redundancies(self, graph: nx.DiGraph) -> Set[str]:

        redundant_nodes = set()
        
        try:
            cycles = nx.simple_cycles(graph)
            
            for cycle in cycles:
                for i in range(len(cycle)):
                    u = cycle[i]
                    v = cycle[(i + 1) % len(cycle)]
                    edge_data = graph.get_edge_data(u, v)
                    if edge_data:
                        redundant_nodes.add(edge_data["node_id"])
        except nx.NetworkXNoCycle:
            # No cycles found
            pass
                
        return redundant_nodes
