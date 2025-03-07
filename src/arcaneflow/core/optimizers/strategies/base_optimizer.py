from abc import ABC, abstractmethod
import networkx as nx
from typing import Set, List

class BaseOptimizer(ABC):
    @abstractmethod
    def identify_redundancies(self, graph: nx.DiGraph) -> Set[str]:
        """
        Identify redundant nodes in the transformation graph.
        
        Args:
            graph: The transformation graph to analyze
            
        Returns:
            A set of node_ids that can be safely removed
        """
        pass