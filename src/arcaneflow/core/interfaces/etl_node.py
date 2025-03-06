from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from pydantic import BaseModel

from arcaneflow.core.interfaces.context import PipelineContext


class ETLNode(ABC):
    @property
    @abstractmethod
    def node_id(self) -> str:
        """Unique identifier for this node"""

    @abstractmethod
    def execute(self, context: PipelineContext) -> PipelineContext:
        """Execute node's operation and return modified context"""

    def validate(self, context: PipelineContext):
        """Optional validation hook"""
        return True
