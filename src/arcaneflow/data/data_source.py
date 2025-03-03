"""
Defines a DataSource model and data loading utilities using Pydantic and pandas.

Classes:
    DataSource: Represents a configuration for loading data from different sources.
"""

import logging
from typing import Any, Dict, Literal
from pydantic import BaseModel, Field
import pandas as pd


class DataSource(BaseModel):
    """
    A Pydantic model representing a configured data source for data loading operations.

    Attributes:
        name (str): Descriptive name for the data source.
        type (Literal["csv", "excel", "json", "api"]): Type of data source format/interface.
        location (str): Path or URL to the data source (file path or API endpoint).
        options (Dict[str, Any]): Source-specific loading options for pandas read methods.
            Defaults to empty dictionary.

    Methods:
        load_data: Loads data based on source type using appropriate pandas reader.
        _load_csv: Internal method to load CSV data.
        _load_excel: Internal method to load Excel data.
        _load_json: Internal method to load JSON data.
        _load_api: Placeholder for API data loading (not implemented).
    """

    name: str
    type: Literal["csv", "excel", "json"]
    location: str  # File path or URL
    options: Dict[str, Any] = Field(default_factory=dict)

    def load_data(self) -> pd.DataFrame:
        """
        Load data from the configured source using appropriate pandas loader.

        Returns:
            pd.DataFrame: Loaded data as a pandas DataFrame.

        Raises:
            ValueError: If unsupported data source type is specified.
        """
        logging.info(
            f"Loading data from source '{self.name}' "
            f"of type '{self.type}' at '{self.location}'"
        )

        # Dispatch to appropriate loader based on source type
        if self.type == "csv":
            return self._load_csv()
        elif self.type == "excel":
            return self._load_excel()
        elif self.type == "json":
            return self._load_json()
        else:
            raise ValueError(f"Unsupported data source type: {self.type}")

    def _load_csv(self) -> pd.DataFrame:
        """Load CSV data using pandas.read_csv with configured options."""
        logging.debug(f"Using CSV options: {self.options}")
        return pd.read_csv(self.location, **self.options)

    def _load_excel(self) -> pd.DataFrame:
        """Load Excel data using pandas.read_excel with configured options."""
        logging.debug(f"Using Excel options: {self.options}")
        return pd.read_excel(self.location, **self.options)

    def _load_json(self) -> pd.DataFrame:
        """Load JSON data using pandas.read_json with configured options."""
        logging.debug(f"Using JSON options: {self.options}")
        return pd.read_json(self.location, **self.options)
