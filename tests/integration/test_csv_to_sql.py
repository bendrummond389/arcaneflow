import pytest
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from arcaneflow.connectors.sinks.database.sql_sink import SQLAlchemySink
from arcaneflow.connectors.sources.file_based.csv_source import CSVSource
from arcaneflow.core.pipeline.builder import PipelineBuilder
from arcaneflow.transformations.column_operations.rename import ColumnRenamer


@pytest.fixture
def test_db():
    """Fixture providing an in-memory SQLite database with session."""
    engine = create_engine("sqlite:///:memory:")
    Session = sessionmaker(bind=engine)
    return engine, Session


def test_full_pipeline(test_db, tmp_path):
    """Test the complete pipeline from CSV source through transformation to DB sink."""
    engine, Session = test_db

    # Define test database model
    Base = declarative_base()

    class TestModel(Base):
        __tablename__ = "test_table"
        id = Column(Integer, primary_key=True)
        new_col = Column(String)

    Base.metadata.create_all(engine)

    # Create test CSV file
    csv_path = tmp_path / "test.csv"
    csv_content = """old_col
                    value1
                    value2
                    value3
                    """
    csv_path.write_text(csv_content)

    # Build pipeline
    pipeline = (
        PipelineBuilder()
        .set_source(CSVSource(str(csv_path)))
        .add_node(ColumnRenamer({"old_col": "new_col"}))
        .set_sink(SQLAlchemySink(TestModel))
        .build()
    )

    # Execute pipeline and verify results
    with Session() as session:
        result = pipeline.execute(session=session)
        session.commit()

        # Verify database insertion
        inserted_rows = session.query(TestModel).count()
        assert inserted_rows == 3, "Should insert 3 rows from CSV"
        assert result.metadata["processed_rows"] == 3, "Should process 3 rows"
