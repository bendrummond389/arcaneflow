from dataclasses import dataclass
from typing import Dict, Set, Any


@dataclass
class TransformationSignature:
    """Signature representing the effect of a transformation."""

    input_schema: Set[str]
    output_schema: Set[str]
    transformation_type: str
    properties: Dict[str, Any] = None

    def __post_init__(self):
        if self.properties is None:
            self.properties = {}

    def __hash__(self):
        prop_items = frozenset((k, str(v)) for k, v in self.properties.items())
        return hash(
            (
                frozenset(self.input_schema),
                frozenset(self.output_schema),
                self.transformation_type,
                prop_items,
            )
        )
