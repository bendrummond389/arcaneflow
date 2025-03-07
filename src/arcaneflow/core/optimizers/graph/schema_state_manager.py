from typing import Dict, FrozenSet, Set, Optional, Any


class SchemaStateManager:
    def __init__(self):
        self.schema_to_node_name: Dict[FrozenSet, str] = {}

    def get_or_create_node(self, schema: FrozenSet) -> str:
        if schema in self.schema_to_node_name:
            return self.schema_to_node_name[schema]

        node_name = f"schema_{hash(schema)}"
        self.schema_to_node_name[schema] = node_name
        return node_name

    def calculate_next_state(self, current_schema: Set, signature: Any) -> Set:
        unchanged_columns = current_schema - signature.input_schema
        return unchanged_columns.union(signature.output_schema)
