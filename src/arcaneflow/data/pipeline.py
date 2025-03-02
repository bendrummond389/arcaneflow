from typing import Any

from sqlalchemy import Boolean, DateTime, Float, Integer, String
from arcaneflow.data.data_frame_schema import DataFrameSchema
from arcaneflow.data.data_source import DataSource
from arcaneflow.data.transformation_chain import TransformationChain
from sqlalchemy.orm import DeclarativeMeta

class ArcanePipeline:
    def __init__(
        self,
        data_source: DataSource,
        transformations: TransformationChain,
        schema_model: DeclarativeMeta,
        engine: Any
    ):
        self.data_source = data_source
        self.transformations = transformations
        self.schema_model = schema_model
        self.engine = engine
        
    def run(self):
        raw_data = self.data_source.load_data()
        
        transformed_data = self.transformations(raw_data)
        
        expected_schema = self._get_expected_schema()
        expected_schema.validate(transformed_data)
        
        transformed_data.to_sql(
            self.schema_model.__tablename__,
            self.engine,
            if_exists='append',
            index=False
        )
    
    def _get_expected_schema(self) -> DataFrameSchema:
        type_map = {
            Integer: 'integer',
            String: 'string',
            Float: 'float',
            DateTime: 'datetime',
            Boolean: 'boolean'
        }
        return DataFrameSchema(columns={
            col.name: type_map.get(type(col.type), 'unknown')
            for col in self.schema_model.__table__.columns
        })