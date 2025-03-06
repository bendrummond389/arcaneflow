import logging
from typing import Dict, Generator, List, Optional, Type

import pandas as pd
from arcaneflow.core import load_config
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeMeta, Session


class DatabaseManager:
    """Manages database connections, schema creation, and data persistence operations.

    Attributes:
        logger: Logger instance for tracking operations.
        config: Loaded configuration dictionary.
        engine: SQLAlchemy engine instance for database interactions.
    """

    def __init__(
        self, config_path: Optional[str] = None, config_dict: Optional[Dict] = None
    ):
        """Initializes the DatabaseManager with configuration.

        Args:
            config_path: Path to configuration file. Takes precedence over config_dict.
            config_dict: Configuration dictionary as an alternative to config_path.
        """
        self.logger = logging.getLogger(__name__)
        self.config = config_dict if config_dict else load_config(config_path)
        self.engine = self._create_engine()

    def _create_engine(self) -> Type[create_engine]:
        """Creates and configures the SQLAlchemy database engine.

        Returns:
            Configured SQLAlchemy engine instance.

        Note:
            Uses SQLite in-memory database as default if no configuration provided.
        """
        db_config = self.config.get("database", {})
        connection_string = db_config.get("default_connection", "sqlite:///:memory:")
        pool_size = db_config.get("connection_pool_size", 5)

        self.logger.info(f"Creating database engine with pool size {pool_size}")
        return create_engine(connection_string, pool_size=pool_size, pool_pre_ping=True)

    def create_tables(self, db_models: List[Type[DeclarativeMeta]]) -> None:
        """Creates database tables for specified ORM models if they don't exist.

        Args:
            db_models: List of SQLAlchemy declarative model classes.

        Note:
            Uses metadata from the first model's base class for table creation.
        """
        if not db_models:
            self.logger.warning("No models provided for table creation")
            return

        self.logger.info(f"Creating tables for {len(db_models)} models if needed")
        base_class = db_models[0].__base__
        base_class.metadata.create_all(self.engine, checkfirst=True)

        for model in db_models:
            self.logger.debug(f"Ensured table exists: {model.__tablename__}")

    def convert_to_orm_records(self, data: pd.DataFrame) -> List[Dict]:
        """Converts DataFrame to ORM-compatible dictionary records.

        Args:
            data: Input DataFrame containing data to convert.

        Returns:
            List of dictionaries suitable for ORM model instantiation.
        """
        self.logger.debug("Converting DataFrame to ORM records")
        return data.to_dict(orient="records")

    def persist_records(
        self, records: List[Dict], model: Type[DeclarativeMeta], batch_size: int = 1000
    ) -> int:
        """Persists records to database using batched insert operations.

        Args:
            records: List of dictionary records to persist
            model: SQLAlchemy ORM model class for record conversion
            batch_size: Number of records per batch (default: 1000)

        Returns:
            Total number of successfully inserted records.
        """
        self.logger.info(f"Inserting {len(records)} records in batches of {batch_size}")
        inserted_count = 0

        with Session(self.engine) as session:
            for batch in self._batch_generator(records, batch_size):
                inserted_count += self._process_batch(session, batch, model)

        self.logger.info(f"Inserted {inserted_count} records total")
        return inserted_count

    def _process_batch(
        self, session: Session, batch: List[Dict], model: Type[DeclarativeMeta]
    ) -> int:
        """Processes a single batch of records through SQLAlchemy ORM.

        Args:
            session: Active database session
            batch: Batch of records to process
            model: ORM model class for instantiation

        Returns:
            Number of records processed in this batch
        """
        batch_size = len(batch)
        self.logger.debug(f"Processing batch of {batch_size} records")

        orm_objects = [model(**record) for record in batch]
        session.add_all(orm_objects)
        session.commit()
        return batch_size

    def _batch_generator(
        self, records: List, batch_size: int
    ) -> Generator[List[Dict], None, None]:
        """Generates batches of records from the complete dataset.

        Args:
            records: Complete list of records to batch
            batch_size: Number of records per batch

        Yields:
            Lists of dictionary records for each batch
        """
        for i in range(0, len(records), batch_size):
            yield records[i : i + batch_size]

    def get_engine(self) -> Type[create_engine]:
        """Provides access to the SQLAlchemy engine instance.

        Returns:
            Configured SQLAlchemy engine instance.
        """
        return self.engine
