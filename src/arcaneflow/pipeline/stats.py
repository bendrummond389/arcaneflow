from typing import Any, Dict


class PipelineStats:
    """
    Generates and holds statistics for pipeline execution.
    """

    def generate_stats(
        self, raw_record_count: int, transformed_record_count: int, inserted_count: int
    ) -> Dict[str, Any]:
        """Generate pipeline execution statistics."""
        return {
            "source_records": raw_record_count,
            "transformed_records": transformed_record_count,
            "inserted_records": inserted_count,
        }
