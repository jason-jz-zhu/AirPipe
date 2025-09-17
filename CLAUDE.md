# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

AirPipe is a task-based ETL framework using imperative Python code with decorators. Developers write ETL logic as simple Python functions decorated with `@pipeline.task()`. The framework automatically handles dependency resolution and parallel execution.

## Development Commands

### Installation
```bash
pip install -r requirements.txt
pip install -e .
```

### Testing
```bash
python -m pytest tests/
```

### Running Workflows
```bash
python run_workflow.py --list                    # List all workflows
python run_workflow.py employee_task_workflow    # Run specific workflow
python pipelines/workflows/employee_task_workflow.py  # Run directly

# DAG Visualization
python run_workflow.py employee_enhanced_workflow --visualize        # Visualize DAG without running
python run_workflow.py employee_enhanced_workflow --visualize --format mermaid  # Generate Mermaid diagram
python pipelines/workflows/employee_enhanced_workflow.py --visualize  # Direct visualization
```

## Code Architecture

### Core Structure
```
airpipe/                    # Framework code only
├── core/                   # Core framework functionality
│   ├── task.py            # Task pipeline engine
│   ├── streaming/         # Streaming framework
│   └── ascii_dag_visualizer.py  # DAG visualization
├── artifacts/              # Artifact management framework
│   └── data_artifact.py   # DataArtifact and ArtifactStore classes
└── utils/                  # Generic framework utilities
    ├── extractors/         # Generic extraction utilities (CSV, JSON, API)
    ├── transformers/       # Generic transformation utilities (filtering, aggregation)
    └── loaders/           # Generic loading utilities (file operations)

pipelines/                  # All business logic organized by domain
├── workflows/             # All workflow definitions centralized
│   ├── employee_task_workflow.py        # Employee data processing workflow
│   ├── employee_enhanced_workflow.py    # Enhanced employee workflow with dependencies
│   ├── advanced_task_workflow.py        # Complex sales analytics workflow
│   ├── simple_task_workflow.py          # Simple ETL example
│   └── streaming_example_workflow.py    # Streaming processing example
├── employee/              # Employee domain components
│   ├── extractors/        # Employee-specific extractors
│   │   └── csv_extractor.py
│   ├── transformers/      # Employee-specific transformers
│   │   ├── salary_transformer.py
│   │   └── department_transformer.py
│   └── loaders/           # Employee-specific loaders
│       └── report_loader.py
├── sales/                 # Sales domain components
│   ├── extractors/        # Sales-specific extractors
│   │   └── sample_extractor.py
│   ├── transformers/      # Sales-specific transformers
│   │   ├── regional_transformer.py
│   │   ├── product_transformer.py
│   │   ├── customer_transformer.py
│   │   └── insights_transformer.py
│   └── loaders/           # Sales-specific loaders
│       └── report_loader.py
└── examples/              # Example domain components
    ├── simple/            # Simple ETL example components
    │   ├── extractors/
    │   ├── transformers/
    │   └── loaders/
    └── streaming/         # Streaming example components (if needed)
```

### Key Components

1. **TaskPipeline**: Main class for creating task-based pipelines with enhanced dependency management
2. **@pipeline.task()**: Enhanced decorator supporting:
   - `depends_on`: Explicit task dependencies
   - `produces`: Named artifact output
   - `consumes`: Named artifact input(s)
3. **DataArtifact**: Container for data flowing through pipeline
4. **Dependency Resolution**: Both implicit (parameters) and explicit (depends_on) supported
5. **Reusable Utils**: Generic operations in utils/ for use across pipelines
6. **Pipeline Components**: Business logic organized by pipeline in extractors/, transformers/, loaders/

### Creating New Workflows

1. Create workflow file in `pipelines/workflows/` directory (centralized location)
2. Choose or create appropriate domain directory under `pipelines/` for components (e.g., `pipelines/mydomain/`)
3. Create corresponding component files in `extractors/`, `transformers/`, `loaders/` subdirectories within the domain
4. Import `TaskPipeline` from `airpipe.core.task`
5. Import components from domain paths (e.g., `from mydomain.extractors.my_extractor import MyExtractor`)
6. Create pipeline instance: `pipeline = TaskPipeline("name")`
7. Define functions with `@pipeline.task()` decorator, using component methods for business logic
8. Write execution flow in `run()` function
9. Call `pipeline.execute()` to run with automatic parallelization

### Example Patterns

#### Basic Pattern (Implicit Dependencies)
```python
from airpipe.core.task import TaskPipeline
import pandas as pd

pipeline = TaskPipeline("my_pipeline")

@pipeline.task()
def extract():
    df = pd.read_csv("data.csv")
    return pipeline.create_artifact(df, "raw_data")

@pipeline.task()
def transform(raw_data):
    df = raw_data.as_dataframe()
    # Transform logic
    return pipeline.create_artifact(df, "transformed")

def run():
    data = extract()
    transformed = transform(data)
    return pipeline.execute()
```

#### Enhanced Pattern (Explicit Dependencies)
```python
from pathlib import Path
import sys
# Add both the project root and the pipelines directory to path
sys.path.append(str(Path(__file__).parent.parent.parent.parent))
sys.path.append(str(Path(__file__).parent.parent.parent))

from airpipe.core.task import TaskPipeline
from employee.extractors.csv_extractor import EmployeeCSVExtractor
from employee.transformers.salary_transformer import SalaryTransformer

pipeline = TaskPipeline("enhanced_pipeline")
extractor = EmployeeCSVExtractor()
transformer = SalaryTransformer()

@pipeline.task(produces="raw_data")
def extract():
    df = extractor.extract_current_employees()
    return pipeline.create_artifact(df, "raw_data")

@pipeline.task(
    depends_on=["extract"],
    consumes="raw_data",
    produces="high_earners"
)
def filter_high_earners():
    raw_data = pipeline.get_artifact("raw_data")
    df = raw_data.as_dataframe()
    filtered = transformer.filter_high_earners(df)
    return pipeline.create_artifact(filtered, "high_earners")

@pipeline.task(
    depends_on=["filter_high_earners"],
    consumes="high_earners"
)
def save_report():
    data = pipeline.get_artifact("high_earners")
    df = data.as_dataframe()
    df.to_csv("output/report.csv")

def run():
    # No manual orchestration needed!
    return pipeline.execute(parallel=True)
```

### Important Notes

- Tasks are automatically classified as extractors, transformers, or loaders based on their signatures
- Dependencies can be:
  - **Implicit**: Inferred from function parameters (backward compatible)
  - **Explicit**: Defined using `depends_on` parameter (recommended for complex workflows)
- Artifacts can be named using `produces` and retrieved using `consumes` or `pipeline.get_artifact()`
- Independent tasks automatically run in parallel
- All data flows through DataArtifact objects
- No configuration files needed - everything is Python code

### New Architecture Benefits

1. **Clean Separation**: Framework code (`airpipe/`) completely separated from business logic (`pipelines/`)
2. **Domain Organization**: Business logic organized by domain (employee, sales, examples) rather than component type
3. **Framework Reusability**: AirPipe framework can be reused across different projects with different business domains
4. **Reusable Utilities**: Generic framework utilities in `airpipe/utils/` can be shared across all pipelines
5. **Explicit Dependencies**: Clear DAG definition with `depends_on`, `produces`, `consumes`
6. **Multiple Extractors**: Support for multiple data sources per pipeline
7. **Business Logic Isolation**: Domain-specific logic completely separated from framework utilities
8. **Easy Scalability**: Easy to add new business domains under `pipelines/` without touching framework code
9. **DAG Visualization**: Built-in visualization of task dependencies and execution flow
10. **Centralized Workflows**: All workflows in one location for easy discovery and management
11. **Clear Separation**: Workflows (orchestration) separated from components (implementation)

### DAG Visualization Features

#### Visualization Formats
- **ASCII**: Text-based tree visualization for terminal display
- **Mermaid**: Markdown-compatible diagrams for documentation

#### Available Methods
```python
# Get DAG structure
dag_structure = pipeline.get_dag_structure()

# Visualize DAG
ascii_dag = pipeline.visualize_dag(format='ascii')
mermaid_dag = pipeline.visualize_dag(format='mermaid', output_file='dag.md')

# Validate DAG (check for cycles)
pipeline.validate_dag()

# Get pipeline statistics
stats = pipeline.get_task_statistics()
# Returns: total_tasks, task_types, critical_path_length, etc.
```

#### Example ASCII Visualization
```
[E] extract_employees
    ├── [T] filter_high_earners ─────────┐
    ├── [T] calculate_salary_stats ──────┼─> [T] generate_report -> [L] save_report
    └── [T] analyze_departments ─────────┘

Legend: [E] Extractor  [T] Transformer  [L] Loader
```