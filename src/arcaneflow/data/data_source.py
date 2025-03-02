from typing import Any, Dict, Literal
from pydantic import BaseModel, Field



class DataSource(BaseModel):
    name: str
    type: Literal["csv", "excel", "json", "api"]
    location: str  # File path or URL
    options: Dict[str, Any] = Field(default_factory=dict)

