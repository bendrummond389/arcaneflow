from typing import Any, Iterable, List, Optional, Union, Dict
from sqlalchemy import Boolean, DateTime, Float, Integer, String
from sqlalchemy.orm import DeclarativeMeta, Session
import pandas as pd

from arcaneflow.data.schema import DataFrameSchema
from arcaneflow.data.data_source import DataSource
from arcaneflow.data.chain import TransformationChain
from arcaneflow.data.transformations.base import DataTransformer, BaseTransformation
import logging

_TYPE_MAPPING = {
    Integer: "integer",
    String: "string",
    Float: "float",
    DateTime: "datetime",
    Boolean: "boolean",
}


class ArcanePipeline:
    """
    Pipeline for loading, transforming, and storing data according to a predefined workflow.

    Manages the flow of data from a source, through transformations, validation against
    a target schema, and finally persistence to a database table defined by an ORM model.

    Attributes:
        data_source: Configuration for loading source data
        transformations: Transformation logic to apply to the data
        db_model: SQLAlchemy model defining the target database table structure
        engine: SQLAlchemy engine for database operations
    """

    def __init__(
        self,
        data_source: DataSource,
        transformations: Union[
            TransformationChain, BaseTransformation, DataTransformer
        ],
        db_model: DeclarativeMeta,
        engine: Any,
        create_tables: bool = True,
    ):
        """
        Initialize a data pipeline with source, transformations, and target configuration.

        Args:
            data_source: Configuration for loading source data
            transformations: Transformation logic to apply to the data
            db_model: SQLAlchemy model defining the target database table
            engine: SQLAlchemy engine for database operations
            create_tables: Whether to create tables if they don't exist (default: True)
        """
        self.data_source = data_source
        self.transformations = transformations
        self.db_model = db_model
        self.engine = engine
        self.create_tables = create_tables
        self.logger = logging.getLogger(__name__)


    def run(self, batch_size: int = 1000) -> Dict[str, Any]:
        """
        Execute the pipeline to load, transform, validate, and store data.

        Steps:
        1. Create tables if they don't exist (optional)
        2. Load raw data from configured data source
        3. Apply transformation chain to the data
        4. Validate transformed data against target table schema
        5. Store validated data to the database using SQLAlchemy ORM

        Args:
            batch_size: Number of records to insert in each batch
                        (helps manage memory for large datasets)

        Returns:
            Dictionary with pipeline execution statistics

        Raises:
            ValueError: If schema validation fails
        """
        # Create tables if they don't exist
        self._create_tables_if_needed()
        raw_data = self._load_raw_data()
        transformed_data = self._transform_data(raw_data)
        self._validate_data(transformed_data)
        records = self._convert_to_orm_records(transformed_data)
        inserted_count = self._persist_records(records, batch_size)

        return self._generate_stats(
            raw_record_count=len(raw_data),
            transformed_record_count=len(transformed_data),
            inserted_count=inserted_count,
        )


    # Data loading and processing
    def _load_raw_data(self) -> pd.DataFrame:
        """Load data from the configured data source."""
        self.logger.info(f"Loading data from source '{self.data_source.name}'")
        data = self.data_source.load_data()
        self.logger.info(f"Loaded {len(data)} records from source")
        return data

    def _transform_data(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Apply transformations to the raw data."""
        self.logger.info("Applying transformations")
        transformed = self.transformations(raw_data)
        self.logger.info(f"Transformed {len(transformed)} records")
        return transformed

    def _validate_data(self, data: pd.DataFrame) -> None:
        """Validate data against target schema."""
        self.logger.info("Validating data against schema")
        schema = self._get_target_dataframe_schema()
        schema.validate(data)
        self.logger.info("Data validation successful")

    # Persistence and storage
    def _persist_records(self, records: List[Dict], batch_size: int) -> int:
        """Persist records to database in batches."""
        self.logger.info(f"Inserting {len(records)} records in batches of {batch_size}")

        inserted_count = 0
        with Session(self.engine) as session:
            for batch in self._batch_generator(records, batch_size):
                inserted_count += self._process_batch(session, batch)

        self.logger.info(f"Inserted {inserted_count} records total")
        return inserted_count

    def _process_batch(self, session: Session, batch: List[Dict]) -> int:
        """Process a single batch of records."""
        batch_size = len(batch)
        self.logger.debug(f"Processing batch of {batch_size} records")

        orm_objects = [self.db_model(**record) for record in batch]
        session.add_all(orm_objects)
        session.commit()
        return batch_size

    def _batch_generator(self, records: List, batch_size: int) -> Iterable[List]:
        """Generate batches of records."""
        for i in range(0, len(records), batch_size):
            yield records[i : i + batch_size]

    # Database-related methods
    def _convert_to_orm_records(self, data: pd.DataFrame) -> List[Dict]:
        """Convert DataFrame to ORM-compatible records."""
        self.logger.debug("Converting DataFrame to ORM records")
        return data.to_dict(orient="records")

    def _create_tables(self):
        """Create the database tables if they don't already exist."""
        base_class = self.db_model.__base__
        base_class.metadata.create_all(self.engine, checkfirst=True)

    def _create_tables_if_needed(self) -> None:
        """Create database tables if configured to do so."""
        if self.create_tables:
            self.logger.info(
                f"Creating table '{self.db_model.__tablename__}' if needed"
            )
            base_class = self.db_model.__base__
            base_class.metadata.create_all(self.engine, checkfirst=True)

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

    # Statistics generation
    def _generate_stats(self, **kwargs) -> Dict[str, Any]:
        """Generate pipeline execution statistics."""
        return {
            "source_records": kwargs.get("raw_record_count", 0),
            "transformed_records": kwargs.get("transformed_record_count", 0),
            "inserted_records": kwargs.get("inserted_count", 0),
        }
