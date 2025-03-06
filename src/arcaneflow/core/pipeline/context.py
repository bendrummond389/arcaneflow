from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from pydantic import BaseModel


class PipelineContext(BaseModel):
    data: Optional[Any] = None
    metadata: Optional[Dict[str, Any]] = {}
    sql_session: Optional[Any] = None
