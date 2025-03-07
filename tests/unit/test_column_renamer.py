import pandas as pd
from arcaneflow.transformations.column_operations.rename import ColumnRenamer
from arcaneflow.core.interfaces.etl_node import PipelineContext


def test_column_renamer():
    df = pd.DataFrame({"old_name": [1, 2, 3]})
    renamer = ColumnRenamer({"old_name": "new_name"})
    context = PipelineContext(data=df)

    result = renamer.execute(context)

    assert "new_name" in result.data.columns
    assert "old_name" not in result.data.columns
