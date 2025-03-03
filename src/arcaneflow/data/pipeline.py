from typing import Any, Optional, Union, Dict
from sqlalchemy import Boolean, DateTime, Float, Integer, String
from sqlalchemy.orm import DeclarativeMeta, Session

from arcaneflow.data.schema import DataFrameSchema
from arcaneflow.data.data_source import DataSource
from arcaneflow.data.chain import TransformationChain
from arcaneflow.data.transformations.base import DataTransformer, BaseTransformation
import logging

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
        transformations: Union[TransformationChain, BaseTransformation, DataTransformer],
        db_model: DeclarativeMeta,
        engine: Any,
        create_tables: bool = True
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
        # Create tables if they don't exist (and if create_tables is True)
        if self.create_tables:
            self.logger.info(f"Creating table '{self.db_model.__tablename__}' if it doesn't exist")
            self._create_tables()
        
        # Load raw data from source
        self.logger.info(f"Loading data from source '{self.data_source.name}'")
        raw_data = self.data_source.load_data()
        raw_record_count = len(raw_data)
        self.logger.info(f"Loaded {raw_record_count} records from source")
        
        # Apply transformations
        self.logger.info("Applying transformations")
        transformed_data = self.transformations(raw_data)
        transformed_record_count = len(transformed_data)
        self.logger.info(f"Transformation complete: {transformed_record_count} records")
        
        # Derive expected DataFrame schema from database model
        target_df_schema = self._get_target_dataframe_schema()
        
        # Validate that transformed data matches expected structure
        self.logger.info("Validating data against schema")
        target_df_schema.validate(transformed_data)
        self.logger.info("Data validation successful")
        
        # Convert DataFrame to list of dictionaries
        records = transformed_data.to_dict(orient='records')
        
        # Use SQLAlchemy ORM to insert data in batches
        self.logger.info(f"Inserting {len(records)} records into table '{self.db_model.__tablename__}'")
        inserted_count = 0
        
        with Session(self.engine) as session:
            # Process in batches to prevent memory issues with large datasets
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                batch_size_actual = len(batch)
                self.logger.debug(f"Processing batch of {batch_size_actual} records")
                
                # Create ORM objects from dictionaries
                orm_objects = [self.db_model(**record) for record in batch]
                
                # Add all objects to session
                session.add_all(orm_objects)
                
                # Commit the batch
                session.commit()
                inserted_count += batch_size_actual
                self.logger.debug(f"Committed {batch_size_actual} records. Total: {inserted_count}/{len(records)}")
        
        self.logger.info(f"Pipeline execution complete. {inserted_count} records inserted.")
        
        # Return execution statistics
        return {
            "source_records": raw_record_count,
            "transformed_records": transformed_record_count,
            "inserted_records": inserted_count
        }
    
    def _create_tables(self):
        """Create the database tables if they don't already exist."""
        # Get the base class of the model (should be a declarative base)
        base_class = self.db_model.__base__
        # Create only the tables for models that inherit from this base
        base_class.metadata.create_all(self.engine, checkfirst=True)
    
    def _get_target_dataframe_schema(self) -> DataFrameSchema:
        """
        Convert the SQLAlchemy model structure to a DataFrameSchema for validation.
        
        Maps SQLAlchemy column types to simplified type names used in DataFrameSchema:
        - Integer -> 'integer'
        - String -> 'string'
        - Float -> 'float'
        - DateTime -> 'datetime'
        - Boolean -> 'boolean'
        - Other types -> 'unknown'
        
        Returns:
            DataFrameSchema representing the expected structure of the DataFrame
                before storing to the database
        """
        # Map SQLAlchemy types to DataFrame schema type names
        type_map = {
            Integer: 'integer',
            String: 'string',
            Float: 'float', 
            DateTime: 'datetime',
            Boolean: 'boolean'
        }
        
        # Build schema from SQLAlchemy model columns
        return DataFrameSchema(columns={
            col.name: type_map.get(type(col.type), 'unknown')
            for col in self.db_model.__table__.columns
        })