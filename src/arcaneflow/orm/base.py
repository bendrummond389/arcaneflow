from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


class SQLAManager:
    """Manages SQLAlchemy connections and sessions"""

    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
        self.session = sessionmaker(bind=self.engine)
        self.base = declarative_base()

    def get_session(self):
        """Return a new SQLAlchemy session"""
        return self.Session()

    class managed_session:
        """Context manager for database sessions"""
        def __init__(self, manager):
            self.manager = manager
            
        def __enter__(self):
            self.session = self.manager.get_session()
            return self.session
            
        def __exit__(self, exc_type, exc_val, exc_tb):
            self.session.close()
