import pytest
from unittest.mock import MagicMock, call
import pandas as pd
from typing import Type
from sqlalchemy.orm import DeclarativeMeta

# Import the classes to be tested
from arcaneflow.connectors.sinks.database.sql_sink import SQLAlchemySink
from arcaneflow.core.interfaces.context import PipelineContext


# Mock model class for testing
class MockModel(DeclarativeMeta):
    __tablename__ = "test_table"


def test_node_id_property():
    """Test that node_id is correctly formatted with model's table name."""
    model = MockModel
    sink = SQLAlchemySink(model=model)
    assert sink.node_id == f"SQLAlchemySink_{model.__tablename__}"


def test_execute_raises_error_without_session():
    """Test execute raises ValueError when context has no session."""
    sink = SQLAlchemySink(model=MockModel)
    context = PipelineContext(data=pd.DataFrame())  # No session attached

    with pytest.raises(ValueError) as exc_info:
        sink.execute(context)

    assert "SQL session not available" in str(exc_info.value)


def test_execute_processes_batches_correctly():
    """Test data is inserted in correct batch sizes and updates metadata."""
    # Setup test data and mocks
    batch_size = 1000
    total_rows = 2500
    sink = SQLAlchemySink(model=MockModel, batch_size=batch_size)
    mock_session = MagicMock()

    # Create DataFrame with test data
    test_data = pd.DataFrame([{"id": i} for i in range(total_rows)])
    context = PipelineContext(data=test_data)
    context._session = mock_session  # Inject mock session

    # Execute sink
    result_context = sink.execute(context)

    # Verify batch processing
    calls = mock_session.bulk_insert_mappings.call_args_list
    assert len(calls) == 3  # 2500 / 1000 = 3 batches

    # Check batch sizes
    assert len(calls[0][0][1]) == batch_size  # First batch
    assert len(calls[1][0][1]) == batch_size  # Second batch
    assert len(calls[2][0][1]) == total_rows % batch_size  # Third batch (500)

    # Verify metadata
    assert result_context.metadata["processed_rows"] == total_rows


def test_execute_with_empty_data():
    """Test no insertion occurs and processed_rows is 0 when data is empty."""
    sink = SQLAlchemySink(model=MockModel)
    mock_session = MagicMock()

    # Create empty DataFrame
    context = PipelineContext(data=pd.DataFrame())
    context._session = mock_session

    result_context = sink.execute(context)

    # Verify no insertion attempted
    mock_session.bulk_insert_mappings.assert_not_called()
    assert result_context.metadata["processed_rows"] == 0


def test_execute_with_smaller_than_batch_data():
    """Test single batch insertion when data size is smaller than batch size."""
    batch_size = 10
    total_rows = 5
    sink = SQLAlchemySink(model=MockModel, batch_size=batch_size)
    mock_session = MagicMock()

    test_data = pd.DataFrame([{"id": i} for i in range(total_rows)])
    context = PipelineContext(data=test_data)
    context._session = mock_session

    sink.execute(context)

    # Verify single batch insertion
    mock_session.bulk_insert_mappings.assert_called_once()
    call_args = mock_session.bulk_insert_mappings.call_args
    assert len(call_args[0][1]) == total_rows
    assert context.metadata["processed_rows"] == total_rows
