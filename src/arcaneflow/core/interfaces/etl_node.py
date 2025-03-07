from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from pydantic import BaseModel

from .context import PipelineContext
from .transformation_signature import TransformationSignature


class ETLNode(ABC):
    @property
    @abstractmethod
    def node_id(self) -> str:
        """Unique identifier for this node"""

    @abstractmethod
    def execute(self, context: PipelineContext) -> PipelineContext:
        """Execute node's operation and return modified context"""

    def validate(self, context: PipelineContext) -> bool:
        """Optional validation hook"""
        return True

    def get_transformation_signature(self) -> TransformationSignature:
        """Return a signature describing the transformation effect."""
        raise NotImplementedError(
            "Subclasses must implement get_transformation_signature"
        )
