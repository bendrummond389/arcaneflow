import logging
from abc import ABC, abstractmethod
from typing import Generator, Optional, Dict, Any
from pydantic import BaseModel
from sqlalchemy.orm import Session
from contextlib import contextmanager

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class PipelineContext(BaseModel):
    data: Optional[Any] = None
    metadata: Optional[Dict[str, Any]] = {}
    _session: Optional[Any] = None 

    @property
    def session(self) -> Session:
        logger.debug("Session property accessed, _session is %s", "available" if self._session else "None")
        if self._session is None:
            logger.error("No active database session when trying to access session property")
            raise ValueError("No active database session")
        return self._session

    @contextmanager
    def with_session(self, session: Session) -> Generator[None, None, None]:
        """Context manager for session management"""
        logger.debug("Setting session in context")
        self._session = session
        try:
            yield
            logger.debug("Session context manager exiting normally")
        except Exception as e:
            logger.error("Exception in session context: %s", str(e))
            raise
        finally:
            logger.debug("Clearing session reference in context")
            self._session = None  