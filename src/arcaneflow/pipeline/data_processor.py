import logging
from typing import Union

import pandas as pd

from arcaneflow.data.chain import TransformationChain
from arcaneflow.data.transformations.base import BaseTransformation, DataTransformer


class DataProcessor:
    """
    Processes data by applying transformations to raw data.
    """

    def __init__(
        self,
        transformations: Union[
            TransformationChain, BaseTransformation, DataTransformer
        ],
    ):
        self.transformations = transformations
        self.logger = logging.getLogger(__name__)

    def transform_data(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Apply transformations to the raw data."""
        self.logger.info("Applying transformations")
        transformed = self.transformations(raw_data)
        self.logger.info(f"Transformed {len(transformed)} records")
        return transformed
