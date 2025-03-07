from typing import List, Optional
from contextlib import contextmanager
from sqlalchemy.orm import Session

from ..interfaces.etl_node import ETLNode
from ..interfaces.context import PipelineContext


class PipelineBuilder:
    """Constructs linear pipelines with optional branching.

    Attributes:
        nodes: List of intermediate processing nodes
        source: Entry point data source
        sink: Final output destination
    """

    def __init__(self) -> None:
        self.nodes: List[ETLNode] = []
        self.source: Optional[ETLNode] = None
        self.sink: Optional[ETLNode] = None

    def set_source(self, source: ETLNode) -> "PipelineBuilder":
        """Set the pipeline's data source.

        Args:
            source: Source node implementing ETLNode interface

        Returns:
            PipelineBuilder instance for method chaining
        """
        self.source = source
        return self

    def add_node(self, node: ETLNode) -> "PipelineBuilder":
        """Add a processing node to the pipeline.

        Args:
            node: Transformation or processing node

        Returns:
            PipelineBuilder instance for method chaining
        """
        self.nodes.append(node)
        return self

    def set_sink(self, sink: ETLNode) -> "PipelineBuilder":
        """Set the pipeline's final destination.

        Args:
            sink: Sink node for data output

        Returns:
            PipelineBuilder instance for method chaining
        """
        self.sink = sink
        return self

    def build(self) -> "Pipeline":
        """Construct the configured pipeline.

        Returns:
            Fully configured Pipeline instance

        Raises:
            ValueError: If no source is configured
        """
        if not self.source:
            raise ValueError("Pipeline source must be defined before building")
        return Pipeline(self.source, self.nodes, self.sink)


class Pipeline:
    """Executable pipeline with context management.

    Attributes:
        source: Initial data source node
        nodes: Sequence of processing nodes
        sink: Final output node (optional)
    """

    def __init__(
        self, source: ETLNode, nodes: List[ETLNode], sink: Optional[ETLNode]
    ) -> None:
        self.source = source
        self.nodes = nodes
        self.sink = sink
        self._session: Optional[Session] = None

    def execute(self, session: Optional[Session] = None) -> PipelineContext:
        """Execute the pipeline with optional database session.

        Args:
            session: SQLAlchemy session for database operations

        Returns:
            PipelineContext containing execution results/metadata

        Raises:
            Exception: Rolls back transaction on error if session provided
        """
        context = PipelineContext()

        try:
            if session:
                with context.with_session(session):
                    return self._execute_pipeline(context)
            return self._execute_pipeline(context)
        except Exception as e:
            if session:
                session.rollback()
            raise

    def _execute_pipeline(self, context: PipelineContext) -> PipelineContext:
        """Internal pipeline execution workflow.

        Args:
            context: Initial pipeline context

        Returns:
            Updated context containing processed data and metadata
        """
        context = self.source.execute(context)

        for node in self.nodes:
            context = node.execute(context)

        if self.sink:
            context = self.sink.execute(context)

        return context
