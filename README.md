# AirPipe

A task-based ETL framework using imperative Python code with decorators.

## Features

- **Task-Based Architecture**: Write ETL as simple Python functions with `@pipeline.task()` decorator
- **Automatic Dependency Resolution**: Dependencies inferred from function parameters
- **Parallel Execution**: Independent tasks run in parallel automatically
- **Data Artifacts**: Flexible data container supporting DataFrames, dicts, lists
- **No Configuration**: Pure Python code, no YAML/JSON configs needed
- **Simple and Direct**: Write extraction, transformation, and loading logic inline

## Installation

```bash
pip install -r requirements.txt
pip install -e .
```

## Quick Start

Write ETL logic as decorated Python functions:

```python
# workflows/data_workflow.py
from airpipe.core.task import TaskPipeline
import pandas as pd
import logging

LOG = logging.getLogger(__name__)

# Create pipeline
pipeline = TaskPipeline("data_processing")

@pipeline.task()
def extractor():
    """Extract data from source"""
    LOG.info("databathing_workflow: extractor")
    df = pd.read_csv("data.csv")
    return pipeline.create_artifact(df, "raw_data")

@pipeline.task()
def transformer(raw_data):
    """Transform the data"""
    df = raw_data.as_dataframe()
    filtered = df[df['value'] > 100]
    return pipeline.create_artifact(filtered, "filtered_data")

@pipeline.task()
def loader(filtered_data):
    """Load to destination"""
    df = filtered_data.as_dataframe()
    df.to_csv("output.csv")
    LOG.info(f"Saved {len(df)} records")

# Define the execution flow
def run():
    data = extractor()
    transformed = transformer(data)
    loader(transformed)
    return pipeline.execute()

if __name__ == "__main__":
    run()
```

## Running Workflows

```bash
# List all workflows
python run_workflow.py --list

# Run specific workflows
python run_workflow.py employee_task_workflow
python run_workflow.py simple_task_workflow
python run_workflow.py advanced_task_workflow

# Or run directly
python workflows/employee_task_workflow.py
```

## How It Works

1. **Define Tasks**: Use `@pipeline.task()` decorator on functions
2. **Automatic Dependencies**: Function parameters define dependencies
3. **Write Flow**: Call tasks in order in your `run()` function
4. **Execute**: `pipeline.execute()` runs all tasks with automatic parallelization

## Task Types

Tasks are automatically classified based on their signature:

- **Extractor**: No parameters (or all optional) - extracts data from sources
- **Transformer**: Takes artifact(s) as input, returns artifact - transforms data
- **Loader**: Takes artifact, returns None - loads data to destinations

## Data Artifacts

Data flows through the pipeline as `DataArtifact` objects:

```python
# Create artifact
artifact = pipeline.create_artifact(data, "name")

# Access data
df = artifact.as_dataframe()
dict_data = artifact.as_dict()
list_data = artifact.as_list()

# Metadata
print(artifact.metadata.row_count)
print(artifact.metadata.checksum)
```

## Parallel Execution

Tasks automatically run in parallel when possible:

```python
@pipeline.task()
def extract():
    return pipeline.create_artifact(data, "raw")

@pipeline.task()
def transform_a(raw):
    # Runs in parallel with transform_b
    return pipeline.create_artifact(processed_a, "result_a")

@pipeline.task()
def transform_b(raw):
    # Runs in parallel with transform_a
    return pipeline.create_artifact(processed_b, "result_b")

def run():
    data = extract()
    # These run in parallel
    result_a = transform_a(data)
    result_b = transform_b(data)
    return pipeline.execute(parallel=True)
```

## Example Workflows

### Simple ETL
See `workflows/simple_task_workflow.py` for a minimal example.

### Employee Analysis
See `workflows/employee_task_workflow.py` for filtering and aggregation.

### Advanced Analytics
See `workflows/advanced_task_workflow.py` for complex multi-source processing.

## Project Structure

```
airpipe/
├── core/
│   └── task.py              # Task decorator system
├── artifacts/
│   └── data_artifact.py     # Data container
└── utils/
    └── logger.py            # Logging utilities

workflows/                   # Your ETL workflows
├── employee_task_workflow.py
├── simple_task_workflow.py
└── advanced_task_workflow.py
```

## License

MIT