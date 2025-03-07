from .builder import Pipeline, PipelineBuilder
from ..optimizers.cayley import CayleyGraphOptimizer


class OptimizingPipelineBuilder(PipelineBuilder):

    def __init__(self) -> None:
        super().__init__()
        self.optimization_enabled = True

    def disable_optimization(self) -> "OptimizingPipelineBuilder":
        self.optimization_enabled = False
        return self

    def enable_optimization(self) -> "OptimizingPipelineBuilder":
        self.optimization_enabled = True
        return self

    def build(self) -> "Pipeline":
        """Construct the configured pipeline with optional optimization.

        Returns:
            Fully configured Pipeline instance, potentially optimized

        Raises:
            ValueError: If no source is configured
        """
        if not self.source:
            raise ValueError("Pipeline source must be defined before building")

        if not self.optimization_enabled:
            return Pipeline(self.source, self.nodes, self.sink)

        optimizer = CayleyGraphOptimizer()
        return optimizer.optimize_pipeline(Pipeline(self.source, self.nodes, self.sink))
