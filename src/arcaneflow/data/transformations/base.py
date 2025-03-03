"""
Defines core interfaces and base classes for data transformation pipelines.

Contains:
- DataTransformer protocol for transformation functions
- BaseTransformation ABC for validated transformation classes
"""

from abc import ABC, abstractmethod
from typing import Optional, Protocol
import pandas as pd

from arcaneflow.data.schema import DataFrameSchema


class DataTransformer(Protocol):
    """Protocol defining a callable interface for data transformation operations.

    Implementations should take a DataFrame as input and return a transformed DataFrame.
    Can be used for both simple functions and stateful transformer classes.
    """

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform a pandas DataFrame.

        Args:
            df: Input data to be transformed

        Returns:
            Transformed version of the input data
        """
        ...


class BaseTransformation(ABC):
    """Abstract base class for validated data transformations with schema checking.

    Provides a framework for transformations that:
    1. Validate input structure before transformation
    2. Perform actual data transformation
    3. Validate output structure after transformation

    Attributes:
        input_schema (Optional[DataFrameSchema]): Expected schema of input DataFrames.
            If provided, validation occurs before transformation.
        output_schema (Optional[DataFrameSchema]): Expected schema of output DataFrames.
            If provided, validation occurs after transformation.
    """

    def __init__(
        self,
        input_schema: Optional[DataFrameSchema] = None,
        output_schema: Optional[DataFrameSchema] = None,
    ):
        """Initialize transformation with optional input/output validation schemas.

        Args:
            input_schema: Schema for validating input DataFrames
            output_schema: Schema for validating transformation results
        """
        self.input_schema = input_schema
        self.output_schema = output_schema

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the full transformation pipeline with schema validation.

        1. Validate input schema if configured
        2. Perform the concrete transformation
        3. Validate output schema if configured

        Args:
            df: Input DataFrame to transform

        Returns:
            Transformed DataFrame

        Raises:
            ValueError: If input or output schema validation fails
        """
        # Validate input structure if schema provided
        if self.input_schema:
            self.input_schema.validate(df)

        # Perform concrete transformation implemented in subclass
        transformed = self._transform(df)

        # Validate output structure if schema provided
        if self.output_schema:
            self.output_schema.validate(transformed)

        return transformed

    @abstractmethod
    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Abstract method containing the core transformation logic.

        Must be implemented by concrete subclasses.

        Args:
            df: Validated input DataFrame to transform

        Returns:
            Transformed DataFrame that should match output_schema
        """
        pass
