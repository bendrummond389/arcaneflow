


import logging
from typing import Any, Dict, List, Optional

import pandas as pd
from arcaneflow.core import load_config
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeMeta, Session

class DatabaseManager:

    def __init__(self, config_path: Optional[str] = None, config_dict: Optional[Dict] = None):
        self.logger = logging.getLogger(__name__)
        self.config = load_config(config_path)
        self.engine = self._create_engine()


    def _create_engine(self):
        db_config = self.config.get("database", {})
        connection_string = db_config.get("default_connection", "sqlite:///:memory:")
        pool_size = db_config.get("connection_pool_size", 5)

        self.logger.info(f"Creating database engine with pool size {pool_size}")
        create_engine(
            connection_string,
            pool_size=pool_size,
            pool_pre_ping=True
        )

    def create_tables(self, db_models: List[DeclarativeMeta]):
        if not db_models:
            self.logger.warning("No models provided for table creation")
            return
        
        self.logger.info(f"Creating tables for {len(db_models)} models if needed")
        base_class = db_models[0].__base__
        base_class.metadata.create_all(self.engine, checkfirst=True)

        for model in db_models:
            self.logger.debug(f"Ensured table exists: {model.__tablename__}")

    def convert_to_orm_records(self, data: pd.DataFrame) -> List[Dict]:
        """Convert DataFrame to ORM-compatible records."""
        self.logger.debug("Converting DataFrame to ORM records")
        return data.to_dict(orient="records")
    
    def persist_records(self, records: List[Dict], model: DeclarativeMeta, batch_size: int = 1000) -> int:
        self.logger.info(f"Inserting {len(records)} records in batches of {batch_size}")
        inserted_count = 0

        with Session(self.engine) as session:
            for batch in self._batch_generator(records, batch_size):
                inserted_count += self._process_batch(session, batch, model)
        self.logger.info(f"Inserted {inserted_count} records total")
        return inserted_count
    
    def _process_batch(self, session: Session, batch: List[Dict], model: DeclarativeMeta) -> int:
        """Process a single batch of records."""
        batch_size = len(batch)
        self.logger.debug(f"Processing batch of {batch_size} records")

        orm_objects = [model(**record) for record in batch]
        session.add_all(orm_objects)
        session.commit()
        return batch_size
        
    def _batch_generator(self, records: List, batch_size: int) -> Any:
        """Generate batches of records."""
        for i in range(0, len(records), batch_size):
            yield records[i : i + batch_size]


    def get_engine(self):
        return self.engine
