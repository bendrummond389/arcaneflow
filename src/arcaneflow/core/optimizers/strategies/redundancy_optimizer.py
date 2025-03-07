import networkx as nx
from typing import Set

from .base_optimizer import BaseOptimizer


class RedundancyOptimizer(BaseOptimizer):
    
    def identify_redundancies(self, graph: nx.DiGraph) -> Set[str]:
        redundant_nodes = set()
        
        for u, v, data in graph.edges(data=True):
            if graph.nodes[u]["schema"] == graph.nodes[v]["schema"]:
                redundant_nodes.add(data.get("node_id"))
                
        return redundant_nodes