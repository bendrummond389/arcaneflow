# ArcaneFlow 🌞

**Streamlined ETL Pipelines with Chainable Transformations**

ArcaneFlow is a Python framework designed to simplify building robust ETL pipelines with a focus on data validation and modular transformations. Inspired by LangChain’s approach to chaining, it provides a structured way to create and manage data processing workflows.

## Key Features

- **🧩 Chainable Transformations** - Compose complex workflows from simple, reusable components
- **🔒 Automatic Schema Validation** - Ensures data consistency at every stage of the pipeline
- **🛆 Built-in Data Source Support** - Supports loading data from CSV, Excel, JSON, and APIs
- **⚙️ SQLAlchemy Integration** - Provides seamless database operations with automatic type mapping
- **🧐 Extensible Architecture** - Easily create custom transformations and data sources
- **✨ Logging & Monitoring** - Integrated logging and error handling for better pipeline debugging

## Installation

```bash
poetry add arcaneflow
# or
pip install arcaneflow
```

## Quick Start

```python
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from arcaneflow import DataSource, TransformationChain, ArcanePipeline
from arcaneflow.transformations import RenameColumns

# Define SQLAlchemy model
Base = declarative_base()

class User(Base):
    """Example SQLAlchemy model mapping to the `users` table"""
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    population = Column(Integer)
    country_code = Column(String(2))

# Define data source
source = DataSource(
    name="city_data",
    type="csv",
    location="data/cities.csv",
    options={"delimiter": ";"}
)

# Create transformation chain
transformations = TransformationChain([
    RenameColumns({"pop": "population"}),
    # Add other transformations here
])

# Set up database connection
engine = create_engine("sqlite:///cities.db")
Base.metadata.create_all(engine)

# Configure pipeline
pipeline = ArcanePipeline(
    data_source=source,
    transformations=transformations,
    schema_model=User,  # Uses SQLAlchemy model
    engine=engine
)

# Execute full ETL process
pipeline.run()
```

## Usage
### Data Sources
```python
# Load CSV data
csv_source = DataSource(
    name="weather",
    type="csv",
    location="data/raw/weather.csv",
)
```

### Schema Validation
```python
from arcaneflow import DataFrameSchema

# Define expected schema
schema = DataFrameSchema(columns={
    "customer_id": "integer",
    "order_total": "float",
    "order_date": "datetime"
})

# Validate DataFrame
schema.validate(df)
```

### Transformations
Create custom transformations by inheriting from BaseTransformation:
```python
class CleanAddresses(BaseTransformation):
    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df["address"] = df["address"].str.strip().str.title()
        return df
```

### Transformation Chains
```python
# Create complex workflows
chain = TransformationChain([
    DataCleaner(),
    FeatureGenerator(),
    TransformationChain([AnotherTransformation(), MoreTransformations()]) # Nested chain
])

# Execute chain
processed_data = chain(raw_data)
```

### Pipelines
```python
# Full ETL pipeline with automatic validation
pipeline = ArcanePipeline(
    data_source=DataSource(...),
    transformations=TransformationChain([...]),
    schema_model=SalesRecord,
    engine=create_engine(...)
)

pipeline.run()
```

## Logging and Error Handling
ArcaneFlow includes built-in logging to help debug pipelines:
```python
import logging
from arcaneflow import ArcanePipeline

logging.basicConfig(level=logging.INFO)
pipeline = ArcanePipeline(...)
try:
    pipeline.run()
except Exception as e:
    logging.error(f"Pipeline execution failed: {e}")
```

## License
ArcaneFlow is licensed under the MIT License.

