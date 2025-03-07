from typing import Type
from sqlalchemy.orm import DeclarativeMeta

from ....core.interfaces.context import PipelineContext
from ....core.interfaces.etl_node import ETLNode


class SQLAlchemySink(ETLNode):
    def __init__(self, model: Type[DeclarativeMeta], batch_size: int = 1000) -> None:
        self.model = model
        self.batch_size = batch_size

    @property
    def node_id(self) -> str:
        return f"SQLAlchemySink_{self.model.__tablename__}"

    def execute(self, context: PipelineContext) -> PipelineContext:
        """Execute the SQL insertion using the current session in context"""
        try:
            session = context.session
        except ValueError as e:
            raise ValueError(
                "SQL session not available in context. Make sure to call pipeline.execute(session=session)"
            )

        data = context.data.to_dict("records")

        for i in range(0, len(data), self.batch_size):
            batch = data[i : i + self.batch_size]
            session.bulk_insert_mappings(self.model, batch)

        context.metadata["processed_rows"] = len(data)

        return context
