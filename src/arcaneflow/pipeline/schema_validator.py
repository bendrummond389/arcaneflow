from typing import Any
import pandas as pd
from sqlalchemy import Boolean, DateTime, Float, Integer, String
from sqlalchemy.orm import DeclarativeMeta
import logging

from arcaneflow.data.schema import DataFrameSchema

# Mapping of SQLAlchemy types to schema type strings
_TYPE_MAPPING = {
    Integer: "integer",
    String: "string",
    Float: "float",
    DateTime: "datetime",
    Boolean: "boolean",
}


class SchemaValidator:
    """
    Validates data against a target schema derived from SQLAlchemy models.
    """

    def __init__(self, db_model: DeclarativeMeta):
        self.db_model = db_model
        self.logger = logging.logger(__name__)

    def validate(self, data: pd.DataFrame):
        self.logger.info("Validating data against schema")
        schema = self._get_target_dataframe_schema()
        schema.validate(data)
        self.logger.info("Data validation successful")

    def _get_target_dataframe_schema(self) -> DataFrameSchema:
        """Derive DataFrame schema from SQLAlchemy model."""
        return DataFrameSchema(
            columns={
                col.name: self._map_column_type(col.type)
                for col in self.db_model.__table__.columns
            }
        )

    def _map_column_type(self, column_type: Any) -> str:
        """Map SQLAlchemy type to schema type string."""
        return _TYPE_MAPPING.get(type(column_type), "unknown")
