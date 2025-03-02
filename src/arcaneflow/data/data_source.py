import logging
from typing import Any, Dict, Literal
from pydantic import BaseModel, Field
import pandas as pd


class DataSource(BaseModel):
    name: str
    type: Literal["csv", "excel", "json", "api"]
    location: str  # File path or URL
    options: Dict[str, Any] = Field(default_factory=dict)

    def load_data(self):

        logging.info(f"Loading data from source '{self.name}' "
                     f"of type '{self.type}' at '{self.location}'")
        
        if self.type == "csv":
            return self._load_csv()
        elif self.type == "excel":
            return self._load_excel()
        elif self.type == "json":
            return self._load_json()
        elif self.type == "api":
            return self._load_api()
        else:
            raise ValueError(f"Unsupported data source type: {self.type}")

    def _load_csv(self):
        logging.debug(f"Using CSV options: {self.options}")
        return pd.read_csv(self.location, **self.options)
    
    def _load_excel(self):
        logging.debug(f"Using Excel options: {self.options}")
        return pd.read_excel(self.location, **self.options)
    
    def _load_json(self) -> pd.DataFrame:
        logging.debug(f"Using JSON options: {self.options}")
        return pd.read_json(self.location, **self.options)
    
    def _load_api(self) -> pd.DataFrame:
        """
        Placeholder method for API-based data loading.
        This might involve requests or an HTTP library.
        """
        raise NotImplementedError("API loading not yet implemented.")
