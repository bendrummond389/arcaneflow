import logging
from typing import Any, Union

import pandas as pd
from arcaneflow.data.chain import TransformationChain
from arcaneflow.data.data_source import DataSource
from arcaneflow.data.transformations.base import BaseTransformation, DataTransformer
from sqlalchemy.orm import DeclarativeMeta

from arcaneflow.db.manager import DatabaseManager
from arcaneflow.pipeline.data_processor import DataProcessor
from arcaneflow.pipeline.schema_validator import SchemaValidator
from arcaneflow.pipeline.stats import PipelineStats


class ArcanePipeline:

    def __init__(
        self,
        data_source: DataSource,
        transformations: Union[
            TransformationChain, BaseTransformation, DataTransformer
        ],
        db_model: DeclarativeMeta,
        engine: Any,
        config_path: str = None,
    ):
        self.data_source = data_source
        self.db_manager = DatabaseManager(config_path)
        self.data_processor = DataProcessor(transformations)
        self.validator = SchemaValidator(db_model)
        self.stats_generator = PipelineStats()
        self.logger = logging.getLogger(__name__)
        self.db_model = db_model

    def run(self, batch_size: int = 1000):
        self.db_manager.create_tables([self.db_model])
        raw_data = self._load_raw_data()
        transformed_data = self.data_processor.transform_data(raw_data)
        self.validator.validate(transformed_data)
        records = self.db_manager.convert_to_orm_records(transformed_data)
        inserted_count = self.db_manager.persist_records(records, model=self.db_model, batch_size=batch_size)

        # Generate statistics
        return self.stats_generator.generate_stats(
            raw_record_count=len(raw_data),
            transformed_record_count=len(transformed_data),
            inserted_count=inserted_count,
        )

    def _load_raw_data(self) -> pd.DataFrame:
        """Load data from the configured data source."""
        self.logger.info(f"Loading data from source '{self.data_source.name}'")
        data = self.data_source.load_data()
        self.logger.info(f"Loaded {len(data)} records from source")
        return data
