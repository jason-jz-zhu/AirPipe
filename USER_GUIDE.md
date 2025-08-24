# AirPipe Complete User Guide

Welcome to AirPipe - a modern, task-based ETL framework that makes data pipeline development simple, intuitive, and powerful.

## Table of Contents

1. [Getting Started](#part-1-getting-started)
2. [Using create-airpipe-app CLI](#part-2-using-create-airpipe-app-cli)
3. [Core Concepts](#part-3-core-concepts)
4. [Writing Workflows](#part-4-writing-workflows)
5. [Advanced Features](#part-5-advanced-features)
6. [Architecture & Organization](#part-6-architecture--organization)
7. [Examples & Tutorials](#part-7-examples--tutorials)
8. [Reference](#part-8-reference)
9. [Deployment](#part-9-deployment)

---

## Part 1: Getting Started

### 1.1 Installation

#### Installing AirPipe from Source

```bash
# Clone the repository
git clone https://github.com/yourusername/airpipe.git
cd airpipe

# Install dependencies
pip install -r requirements.txt

# Install AirPipe in development mode
pip install -e .
```

#### System Requirements

- Python 3.8 or higher
- pip package manager
- Optional: Apache Spark 3.0+ (for Spark features)
- Optional: Git (for version control)

#### Verifying Installation

```bash
# Test AirPipe import
python -c "from airpipe.core.task import TaskPipeline; print('✓ AirPipe installed successfully')"

# Test CLI tool
create-airpipe-app --help
```

### 1.2 Quick Start with create-airpipe-app

#### Creating Your First Project

```bash
# Interactive template selection
create-airpipe-app my-first-pipeline

# Or specify a template directly
create-airpipe-app my-first-pipeline --template simple
```

#### Understanding the Project Structure

```
my-first-pipeline/
├── README.md                 # Project documentation
├── requirements.txt          # Python dependencies
├── run_workflow.py          # Workflow runner CLI
├── pipelines/               # Your ETL logic
│   ├── workflows/           # Workflow definitions (orchestration)
│   └── [domain]/            # Domain-specific components
│       ├── extractors/      # Data extraction logic
│       ├── transformers/    # Data transformation logic
│       └── loaders/         # Data loading logic
├── data/                    # Input data files
└── output/                  # Output files
```

#### Running Example Workflows

```bash
cd my-first-pipeline

# List available workflows
python run_workflow.py --list

# Run a specific workflow
python run_workflow.py simple_task_workflow

# Or run directly
python pipelines/workflows/simple_task_workflow.py
```

---

## Part 2: Using create-airpipe-app CLI

### 2.1 Available Templates

The CLI provides several templates to jumpstart your project:

| Template | Description | Use Case |
|----------|-------------|----------|
| **blank** | Clean project structure with no examples | Starting from scratch |
| **simple** | Basic ETL workflow example | Learning the basics |
| **streaming** | Streaming data processing with micro-batches | Real-time processing |
| **spark** | Big data processing with Apache Spark | Large-scale data |
| **full** | Complete example project with all pipeline types | Reference implementation |

### 2.2 CLI Options

```bash
create-airpipe-app [project_name] [options]

Options:
  --template, -t     Template to use (blank|simple|streaming|spark|full)
  --output-dir, -o   Directory to create project in (default: current directory)
  --no-git          Skip git repository initialization
  --no-venv         Skip virtual environment setup
  --no-install      Skip dependency installation
```

#### Examples

```bash
# Create project with specific template
create-airpipe-app analytics --template simple

# Create in specific directory
create-airpipe-app my-project --output-dir ~/projects

# Skip Git and virtual environment setup
create-airpipe-app quick-test --no-git --no-venv

# Full control
create-airpipe-app production-etl \
  --template spark \
  --output-dir /opt/pipelines \
  --no-venv
```

### 2.3 Project Structure Deep Dive

#### Framework vs Business Logic Separation

```
project/
├── pipelines/                    # ALL your business logic
│   ├── workflows/               # Centralized workflow definitions
│   │   └── my_workflow.py      # Your ETL orchestration
│   └── mydomain/                # Domain-specific components
│       ├── extractors/          # Data sources
│       ├── transformers/        # Business rules
│       └── loaders/            # Data destinations
└── [AirPipe framework]          # Installed as package (not in project)
```

**Important**: The `airpipe/` folder is NOT included in your project. It's installed as a Python package, ensuring clean separation between framework and business logic.

---

## Part 3: Core Concepts

### 3.1 TaskPipeline

The heart of AirPipe - orchestrates your ETL tasks.

```python
from airpipe.core.task import TaskPipeline

# Create a pipeline
pipeline = TaskPipeline("my_pipeline")

# Define tasks with decorator
@pipeline.task()
def extract_data():
    # Your extraction logic
    return pipeline.create_artifact(data, "raw_data")
```

#### Key Methods

- `create_artifact(data, name)` - Wrap data for pipeline flow
- `get_artifact(name)` - Retrieve named artifact
- `execute()` - Run the pipeline
- `visualize_dag()` - Visualize task dependencies

### 3.2 DataArtifacts

Data containers that flow through your pipeline.

```python
# Creating artifacts
artifact = pipeline.create_artifact(
    data=df,                    # Your data
    name="processed_data"       # Artifact name
)

# Accessing data in different formats
df = artifact.as_dataframe()        # As Pandas DataFrame
spark_df = artifact.as_spark_dataframe()  # As Spark DataFrame
dict_data = artifact.as_dict()      # As dictionary
list_data = artifact.as_list()      # As list

# Metadata access
print(f"Rows: {artifact.metadata.row_count}")
print(f"Columns: {artifact.metadata.column_count}")
print(f"Created: {artifact.metadata.created_at}")
```

#### Supported Data Formats

- Pandas DataFrames
- Spark DataFrames
- Python dictionaries
- Python lists
- JSON data
- Raw bytes

### 3.3 Task Types

Tasks are automatically classified based on their function signature:

#### Extractors (No input parameters)
```python
@pipeline.task()
def extract():
    """Gets data from source"""
    df = pd.read_csv("data.csv")
    return pipeline.create_artifact(df, "raw_data")
```

#### Transformers (Takes artifacts, returns artifact)
```python
@pipeline.task()
def transform(raw_data):
    """Processes data"""
    df = raw_data.as_dataframe()
    processed = df[df['value'] > 100]
    return pipeline.create_artifact(processed, "filtered_data")
```

#### Loaders (Takes artifact, returns None)
```python
@pipeline.task()
def load(filtered_data):
    """Saves data to destination"""
    df = filtered_data.as_dataframe()
    df.to_csv("output.csv")
    print(f"Saved {len(df)} records")
```

---

## Part 4: Writing Workflows

### 4.1 Basic Workflow Pattern

#### Simple ETL Example

```python
from airpipe.core.task import TaskPipeline
import pandas as pd

# Create pipeline
pipeline = TaskPipeline("basic_etl")

@pipeline.task()
def extract():
    df = pd.read_csv("input.csv")
    return pipeline.create_artifact(df, "raw")

@pipeline.task()
def transform(raw):
    df = raw.as_dataframe()
    # Apply business logic
    df['new_column'] = df['value'] * 2
    return pipeline.create_artifact(df, "transformed")

@pipeline.task()
def load(transformed):
    df = transformed.as_dataframe()
    df.to_csv("output.csv", index=False)

def run():
    # Define execution flow
    raw_data = extract()
    transformed_data = transform(raw_data)
    load(transformed_data)
    
    # Execute pipeline
    return pipeline.execute()

if __name__ == "__main__":
    run()
```

### 4.2 Advanced Patterns

#### Explicit Dependencies

```python
@pipeline.task(
    depends_on=["extract"],      # Explicit dependency
    consumes="raw_data",          # Named input artifact
    produces="clean_data"         # Named output artifact
)
def clean():
    raw = pipeline.get_artifact("raw_data")
    df = raw.as_dataframe()
    # Cleaning logic
    return pipeline.create_artifact(df, "clean_data")
```

#### Parallel Task Execution

```python
@pipeline.task()
def extract():
    return pipeline.create_artifact(data, "raw")

@pipeline.task()
def transform_a(raw):
    # This runs in parallel with transform_b
    return pipeline.create_artifact(result_a, "result_a")

@pipeline.task()
def transform_b(raw):
    # This runs in parallel with transform_a
    return pipeline.create_artifact(result_b, "result_b")

@pipeline.task()
def combine(result_a, result_b):
    # Waits for both transforms to complete
    return pipeline.create_artifact(combined, "final")

def run():
    raw = extract()
    a = transform_a(raw)
    b = transform_b(raw)
    final = combine(a, b)
    return pipeline.execute(parallel=True)  # Enable parallelization
```

#### Error Handling

```python
@pipeline.task()
def risky_extraction():
    try:
        df = pd.read_csv("maybe_missing.csv")
        return pipeline.create_artifact(df, "data")
    except FileNotFoundError:
        # Return empty DataFrame as fallback
        df = pd.DataFrame()
        return pipeline.create_artifact(df, "data")
    except Exception as e:
        LOG.error(f"Extraction failed: {e}")
        raise
```

### 4.3 Best Practices

#### Organizing Business Logic

```python
# pipelines/mydomain/extractors/database_extractor.py
class DatabaseExtractor:
    def __init__(self, connection_string):
        self.conn_str = connection_string
    
    def extract_customers(self):
        # Complex extraction logic
        return df

# pipelines/workflows/customer_workflow.py
from mydomain.extractors.database_extractor import DatabaseExtractor

extractor = DatabaseExtractor("postgresql://...")

@pipeline.task()
def extract():
    df = extractor.extract_customers()
    return pipeline.create_artifact(df, "customers")
```

#### Component Reusability

```python
# Create reusable transformers
class DataCleaner:
    def remove_duplicates(self, df):
        return df.drop_duplicates()
    
    def handle_missing(self, df, strategy='drop'):
        if strategy == 'drop':
            return df.dropna()
        elif strategy == 'fill':
            return df.fillna(0)

# Use in multiple workflows
cleaner = DataCleaner()

@pipeline.task()
def clean(raw_data):
    df = raw_data.as_dataframe()
    df = cleaner.remove_duplicates(df)
    df = cleaner.handle_missing(df, strategy='fill')
    return pipeline.create_artifact(df, "clean")
```

---

## Part 5: Advanced Features

### 5.1 Streaming Processing

#### Basic Streaming Setup

```python
from airpipe.core.streaming import MicroBatchProcessor, StreamConfig
from airpipe.core.streaming import SimulatedDataSource

# Configure streaming
config = StreamConfig(
    batch_size=100,
    batch_interval=5.0,  # Process every 5 seconds
    max_batches=None,    # Run indefinitely
)

# Create data source
source = SimulatedDataSource(
    schema={'value': 'float', 'timestamp': 'datetime'},
    rate=10.0  # 10 records per second
)

# Process stream
processor = MicroBatchProcessor(pipeline, config)
processor.process_stream(source=source)
```

#### Real-time Monitoring

```python
from airpipe.core.streaming import StreamMonitor, AlertRule

# Set up monitoring
monitor = StreamMonitor()
monitor.add_rule(AlertRule(
    name="high_value",
    condition=lambda batch: (batch['value'] > 1000).any(),
    action=lambda batch: LOG.warning(f"High value detected!")
))

# Process with monitoring
processor.process_stream(source=source, monitor=monitor)
```

### 5.2 Apache Spark Integration

#### Setting Up Spark

```python
from airpipe.utils.spark import SparkSessionManager

# Initialize Spark session
spark = SparkSessionManager.get_or_create({
    'app_name': 'My ETL Pipeline',
    'master': 'local[*]',  # Use all cores
    'config': {
        'spark.driver.memory': '4g',
        'spark.sql.adaptive.enabled': 'true'
    }
})
```

#### Processing Large Files

```python
from airpipe.utils.spark import read_csv, write_parquet

@pipeline.task(produces="big_data")
def extract_large_file():
    # Read large CSV with Spark
    spark_df = read_csv(
        "huge_file.csv",
        header=True,
        inferSchema=True
    )
    
    # Return as artifact (stays as Spark DataFrame)
    return pipeline.create_artifact(spark_df, "big_data")

@pipeline.task(consumes="big_data", produces="aggregated")
def aggregate():
    spark_df = pipeline.get_artifact("big_data").as_spark_dataframe()
    
    # Use Spark SQL
    from airpipe.utils.spark import create_temp_view, execute_sql
    
    create_temp_view(spark_df, "data")
    result = execute_sql("""
        SELECT 
            category,
            COUNT(*) as count,
            AVG(value) as avg_value
        FROM data
        GROUP BY category
    """)
    
    return pipeline.create_artifact(result, "aggregated")
```

#### Converting Between Pandas and Spark

```python
from airpipe.utils.spark import pandas_to_spark, spark_to_pandas

@pipeline.task()
def convert_example(pandas_artifact):
    # Get Pandas DataFrame
    df = pandas_artifact.as_dataframe()
    
    # Convert to Spark for large-scale processing
    spark_df = pandas_to_spark(df)
    
    # Process with Spark
    processed = spark_df.filter("value > 100")
    
    # Convert back to Pandas if needed
    result_df = spark_to_pandas(processed)
    
    return pipeline.create_artifact(result_df, "result")
```

### 5.3 DAG Visualization

#### Visualizing Task Dependencies

```bash
# Command line visualization
python run_workflow.py my_workflow --visualize

# Generate Mermaid diagram
python run_workflow.py my_workflow --visualize --format mermaid
```

#### Programmatic Visualization

```python
# In your workflow
def visualize():
    # ASCII visualization
    ascii_dag = pipeline.visualize_dag(format='ascii')
    print(ascii_dag)
    
    # Mermaid diagram (for documentation)
    mermaid_dag = pipeline.visualize_dag(format='mermaid')
    with open('workflow_diagram.md', 'w') as f:
        f.write(mermaid_dag)
    
    # Get DAG statistics
    stats = pipeline.get_task_statistics()
    print(f"Total tasks: {stats['total_tasks']}")
    print(f"Critical path: {stats['critical_path_length']}")
```

Example ASCII output:
```
[E] extract_data
    ├── [T] clean_data
    │   └── [T] transform_data
    │       ├── [T] aggregate ────┐
    │       └── [T] filter ───────┼─> [L] save_results
    └── [T] validate_data ────────┘

Legend: [E] Extractor  [T] Transformer  [L] Loader
```

### 5.4 MCP Server & Agent System

AirPipe now supports **MCP (Model Context Protocol)** and an **Agent-based architecture** for AI-powered ETL operations.

#### Natural Language Pipeline Creation

Create pipelines using natural language descriptions:

```python
from airpipe.mcp import AirPipeMCPServer
from airpipe.agents import OrchestratorAgent

# Using MCP Server
server = AirPipeMCPServer()

# Using Orchestrator Agent directly
orchestrator = OrchestratorAgent()
pipeline = await orchestrator.create_pipeline_from_description(
    description="Extract sales data from CSV, filter Q4 2024, aggregate by region, save to database",
    name="sales_analysis"
)
```

#### Starting the MCP Server

```python
import asyncio
from airpipe.mcp import AirPipeMCPServer

# Create and start server
server = AirPipeMCPServer()
await server.start_server(host="localhost", port=8765)
```

#### Connecting with MCP Client

```python
import websockets
import json

# Connect to server
ws = await websockets.connect("ws://localhost:8765")

# Create pipeline from natural language
request = {
    "tool": "create_pipeline",
    "parameters": {
        "description": "Extract sales data from S3, filter Q4 2024 where revenue > 1000, aggregate by region, save to Redshift",
        "name": "sales_q4_analysis",
        "auto_execute": True
    }
}

await ws.send(json.dumps(request))
response = json.loads(await ws.recv())
```

#### Available MCP Tools

1. **create_pipeline** - Create pipelines from natural language
2. **execute_pipeline** - Run existing pipelines
3. **extract_data** - Extract from various sources
4. **transform_data** - Apply transformations
5. **load_data** - Load to destinations
6. **query_artifact** - Query pipeline data
7. **monitor_pipeline** - Get pipeline metrics
8. **optimize_pipeline** - AI-driven optimization
9. **visualize_pipeline** - Generate DAG visualizations
10. **list_pipelines** - List all pipelines

#### Agent System Architecture

```python
from airpipe.agents import (
    OrchestratorAgent,
    ExtractorAgent,
    TransformerAgent,
    LoaderAgent
)

# Create specialized agents
orchestrator = OrchestratorAgent()
extractor = ExtractorAgent()
transformer = TransformerAgent()
loader = LoaderAgent()

# Register agents for collaboration
orchestrator.register_agent(extractor)
orchestrator.register_agent(transformer)
orchestrator.register_agent(loader)

# Agents work together automatically
pipeline = await orchestrator.create_pipeline_from_description(
    "Read customer data, segment by behavior, export to JSON",
    "customer_segmentation"
)
```

#### Natural Language Examples

**Sales Analysis:**
```
"Extract sales data from S3, 
filter for Q4 2024 where revenue > 1000,
aggregate by region and product,
save results to Redshift"
```

**Data Quality Pipeline:**
```
"Read data from database,
detect and fix quality issues,
validate against business rules,
generate quality report"
```

**Real-time Processing:**
```
"Stream data from Kafka,
transform with business logic,
aggregate in 5-minute windows,
load to S3 and trigger alerts"
```

#### Agent Communication

Agents communicate through structured messages:

```python
from airpipe.agents import AgentMessage, MessageType

# Send task to agent
message = AgentMessage(
    sender="orchestrator",
    recipient="extractor_001",
    type=MessageType.TASK,
    content={
        "source_type": "csv",
        "source_config": {"file_path": "data.csv"}
    },
    requires_response=True
)

response = await extractor.receive_message(message)
```

#### Self-Adapting Pipelines

Pipelines learn and optimize based on execution patterns:

```python
# Pipeline adapts automatically
orchestrator = OrchestratorAgent()

# Create adaptive pipeline
pipeline = await orchestrator.create_pipeline_from_description(
    "Extract data, auto-detect quality issues, apply appropriate fixes, optimize for performance",
    "adaptive_pipeline"
)

# Pipeline learns from each execution
await orchestrator.learn_from_execution(
    task={'type': 'pipeline_execution'},
    result={'status': 'success'},
    performance={'execution_time': 45.2, 'throughput': 10000}
)
```

#### Integration with AI Models

**With OpenAI GPTs:**
```python
# GPT can use MCP tools
tools = server.get_tool_definitions()
# Use tools with OpenAI function calling
```

**With Claude:**
```python
# Claude can orchestrate pipelines
# Tools are available through MCP protocol
```

**With LangChain:**
```python
from langchain.tools import Tool

# Wrap as LangChain tool
airpipe_tool = Tool(
    name="AirPipe",
    func=server.handle_request,
    description="Create and run data pipelines"
)
```

#### Complete Example

```python
import asyncio
from airpipe.mcp import AirPipeMCPServer
from airpipe.agents import OrchestratorAgent

async def example():
    # Start MCP server
    server = AirPipeMCPServer()
    server_task = asyncio.create_task(server.start_server())
    
    # Create orchestrator
    orchestrator = OrchestratorAgent()
    
    # Natural language pipeline
    pipeline = await orchestrator.create_pipeline_from_description(
        description="""
        Extract customer data from database,
        clean missing values and duplicates,
        calculate customer lifetime value,
        segment by value tiers,
        export to CSV and send summary email
        """,
        name="customer_ltv_analysis"
    )
    
    # Execute pipeline
    results = pipeline.execute(parallel=True)
    print(f"Pipeline completed: {results}")
    
    # Optimize based on performance
    optimizations = await orchestrator.optimize_pipeline(
        pipeline=pipeline,
        goal="speed",
        constraints={"max_memory": "8GB"}
    )
    print(f"Applied optimizations: {optimizations}")

# Run example
asyncio.run(example())
```

### 5.5 AWS Glue Integration

AirPipe can deploy and run workflows on AWS Glue for enterprise-scale processing.

#### Setting Up Glue Integration

```python
from airpipe.integrations.aws import GlueAdapter

# Initialize adapter
adapter = GlueAdapter(region='us-east-1')

# Deploy pipeline to Glue
workflow_name = adapter.create_workflow(
    pipeline=my_pipeline,
    role_arn="arn:aws:iam::123456789:role/GlueServiceRole",
    script_bucket="my-glue-scripts"
)

# Run in AWS Glue
run_id = adapter.run_workflow(workflow_name)
adapter.wait_for_completion(workflow_name, run_id)
```

#### Using Glue Data Catalog

```python
from airpipe.integrations.aws import GlueCatalogExtractor

extractor = GlueCatalogExtractor()

@pipeline.task(produces="catalog_data")
def extract_from_catalog():
    return extractor.extract_from_catalog(
        database="my_database",
        table="my_table",
        pipeline=pipeline
    )
```

#### Loading to S3

```python
from airpipe.integrations.aws import S3DataLoader

loader = S3DataLoader(bucket="my-bucket", prefix="output")

@pipeline.task(consumes="processed_data")
def save_to_s3():
    data = pipeline.get_artifact("processed_data")
    path = loader.load_to_s3(
        artifact=data,
        format="parquet",
        partition_by=["year", "month"]
    )
    print(f"Saved to: {path}")
```

---

## Part 6: Architecture & Organization

### 6.1 Framework vs Business Logic

#### Clean Separation Principles

```
AirPipe Framework (installed package)    Your Project
┌─────────────────────────┐             ┌──────────────────────┐
│ airpipe/                │             │ my-project/          │
│ ├── core/               │             │ ├── pipelines/       │
│ │   └── task.py         │ <────uses── │ │   ├── workflows/  │
│ ├── artifacts/          │             │ │   └── mydomain/   │
│ └── utils/              │             │ └── run_workflow.py  │
└─────────────────────────┘             └──────────────────────┘
```

**Key Points:**
- Framework provides infrastructure
- You provide business logic
- No framework code in your project
- Business logic organized by domain

### 6.2 Creating Custom Components

#### Building an Extractor

```python
# pipelines/mydomain/extractors/api_extractor.py
import requests
import pandas as pd
from typing import Dict, Optional

class APIExtractor:
    """Extract data from REST API."""
    
    def __init__(self, base_url: str, api_key: Optional[str] = None):
        self.base_url = base_url
        self.headers = {'Authorization': f'Bearer {api_key}'} if api_key else {}
    
    def extract_data(self, endpoint: str, params: Dict = None) -> pd.DataFrame:
        """Fetch data from API endpoint."""
        response = requests.get(
            f"{self.base_url}/{endpoint}",
            params=params,
            headers=self.headers
        )
        response.raise_for_status()
        
        data = response.json()
        return pd.DataFrame(data)
    
    def extract_paginated(self, endpoint: str, page_size: int = 100) -> pd.DataFrame:
        """Extract paginated data."""
        all_data = []
        page = 1
        
        while True:
            df = self.extract_data(endpoint, {'page': page, 'size': page_size})
            if df.empty:
                break
            all_data.append(df)
            page += 1
        
        return pd.concat(all_data, ignore_index=True)
```

#### Building a Transformer

```python
# pipelines/mydomain/transformers/business_rules.py
import pandas as pd
import numpy as np

class BusinessRuleTransformer:
    """Apply business rules to data."""
    
    def categorize_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Categorize customers based on purchase history."""
        df = df.copy()
        
        # Calculate metrics
        df['total_purchases'] = df.groupby('customer_id')['amount'].transform('sum')
        df['purchase_count'] = df.groupby('customer_id')['amount'].transform('count')
        
        # Apply categorization rules
        conditions = [
            (df['total_purchases'] >= 10000),
            (df['total_purchases'] >= 5000),
            (df['total_purchases'] >= 1000)
        ]
        categories = ['Platinum', 'Gold', 'Silver']
        
        df['customer_tier'] = np.select(conditions, categories, default='Bronze')
        
        return df
    
    def calculate_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate business metrics."""
        metrics = df.groupby('customer_tier').agg({
            'amount': ['sum', 'mean', 'count'],
            'customer_id': 'nunique'
        })
        
        metrics.columns = ['total_revenue', 'avg_purchase', 'transaction_count', 'customer_count']
        return metrics.reset_index()
```

#### Building a Loader

```python
# pipelines/mydomain/loaders/database_loader.py
import pandas as pd
from sqlalchemy import create_engine
from typing import Optional

class DatabaseLoader:
    """Load data to database."""
    
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
    
    def load_to_table(
        self, 
        df: pd.DataFrame, 
        table_name: str,
        schema: Optional[str] = None,
        if_exists: str = 'append'
    ):
        """Load DataFrame to database table."""
        df.to_sql(
            name=table_name,
            con=self.engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            method='multi',  # Faster for large datasets
            chunksize=1000
        )
        
        return len(df)
    
    def upsert(self, df: pd.DataFrame, table_name: str, key_columns: list):
        """Upsert (insert or update) records."""
        # Implementation depends on database type
        # This is PostgreSQL example
        from sqlalchemy.dialects.postgresql import insert
        
        meta = pd.io.sql.SQLDatabase(self.engine).meta
        table = meta.tables[table_name]
        
        stmt = insert(table).values(df.to_dict('records'))
        stmt = stmt.on_duplicate_key_update(
            **{col: stmt.inserted[col] for col in df.columns if col not in key_columns}
        )
        
        self.engine.execute(stmt)
```

---

## Part 7: Examples & Tutorials

### 7.1 Tutorial: Building a Sales Analytics Pipeline

Let's build a complete pipeline from scratch.

#### Step 1: Create Project

```bash
create-airpipe-app sales-analytics --template blank
cd sales-analytics
```

#### Step 2: Create Domain Structure

```bash
mkdir -p pipelines/sales/{extractors,transformers,loaders}
```

#### Step 3: Create Extractor

```python
# pipelines/sales/extractors/sales_extractor.py
import pandas as pd
from datetime import datetime, timedelta

class SalesDataExtractor:
    def extract_recent_sales(self, days=30):
        """Extract recent sales data."""
        # In production, this would query a database
        # For demo, we'll generate sample data
        
        dates = pd.date_range(
            end=datetime.now(),
            periods=days*100,
            freq='H'
        )
        
        df = pd.DataFrame({
            'transaction_id': range(len(dates)),
            'timestamp': dates,
            'product_id': np.random.choice(['P001', 'P002', 'P003'], len(dates)),
            'customer_id': np.random.choice(range(1, 101), len(dates)),
            'quantity': np.random.randint(1, 10, len(dates)),
            'price': np.random.uniform(10, 1000, len(dates)),
            'region': np.random.choice(['North', 'South', 'East', 'West'], len(dates))
        })
        
        df['total'] = df['quantity'] * df['price']
        return df
```

#### Step 4: Create Transformer

```python
# pipelines/sales/transformers/sales_transformer.py
import pandas as pd

class SalesAnalyzer:
    def analyze_by_region(self, df):
        """Analyze sales by region."""
        return df.groupby('region').agg({
            'total': ['sum', 'mean', 'count'],
            'quantity': 'sum',
            'customer_id': 'nunique'
        }).round(2)
    
    def analyze_by_product(self, df):
        """Analyze sales by product."""
        return df.groupby('product_id').agg({
            'total': 'sum',
            'quantity': 'sum',
            'transaction_id': 'count'
        }).sort_values('total', ascending=False)
    
    def identify_top_customers(self, df, top_n=10):
        """Identify top customers by revenue."""
        customer_sales = df.groupby('customer_id')['total'].sum()
        return customer_sales.nlargest(top_n).reset_index()
```

#### Step 5: Create Workflow

```python
# pipelines/workflows/sales_analytics_workflow.py
from airpipe.core.task import TaskPipeline
from sales.extractors.sales_extractor import SalesDataExtractor
from sales.transformers.sales_transformer import SalesAnalyzer
import logging

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Initialize components
pipeline = TaskPipeline("sales_analytics")
extractor = SalesDataExtractor()
analyzer = SalesAnalyzer()

@pipeline.task(produces="raw_sales")
def extract_sales():
    """Extract recent sales data."""
    LOG.info("Extracting sales data...")
    df = extractor.extract_recent_sales(days=30)
    LOG.info(f"Extracted {len(df)} transactions")
    return pipeline.create_artifact(df, "raw_sales")

@pipeline.task(
    depends_on=["extract_sales"],
    consumes="raw_sales",
    produces="regional_analysis"
)
def analyze_regions():
    """Analyze sales by region."""
    LOG.info("Analyzing regional sales...")
    sales = pipeline.get_artifact("raw_sales")
    df = sales.as_dataframe()
    
    regional = analyzer.analyze_by_region(df)
    return pipeline.create_artifact(regional, "regional_analysis")

@pipeline.task(
    depends_on=["extract_sales"],
    consumes="raw_sales",
    produces="product_analysis"
)
def analyze_products():
    """Analyze sales by product."""
    LOG.info("Analyzing product sales...")
    sales = pipeline.get_artifact("raw_sales")
    df = sales.as_dataframe()
    
    products = analyzer.analyze_by_product(df)
    return pipeline.create_artifact(products, "product_analysis")

@pipeline.task(
    depends_on=["extract_sales"],
    consumes="raw_sales",
    produces="top_customers"
)
def identify_top_customers():
    """Identify top customers."""
    LOG.info("Identifying top customers...")
    sales = pipeline.get_artifact("raw_sales")
    df = sales.as_dataframe()
    
    top = analyzer.identify_top_customers(df, top_n=10)
    return pipeline.create_artifact(top, "top_customers")

@pipeline.task(
    depends_on=["analyze_regions", "analyze_products", "identify_top_customers"],
    consumes=["regional_analysis", "product_analysis", "top_customers"]
)
def generate_report():
    """Generate final report."""
    LOG.info("Generating report...")
    
    regional = pipeline.get_artifact("regional_analysis").as_dataframe()
    products = pipeline.get_artifact("product_analysis").as_dataframe()
    customers = pipeline.get_artifact("top_customers").as_dataframe()
    
    # Save reports
    regional.to_csv("output/regional_sales.csv")
    products.to_csv("output/product_sales.csv")
    customers.to_csv("output/top_customers.csv")
    
    LOG.info("Reports saved to output/")
    
    # Print summary
    print("\n=== Sales Analytics Summary ===")
    print(f"\nTop Region: {regional['total']['sum'].idxmax()}")
    print(f"Top Product: {products.index[0]}")
    print(f"Top Customer: Customer {customers.iloc[0]['customer_id']}")

def run():
    """Execute the pipeline."""
    extract_sales()
    analyze_regions()
    analyze_products()
    identify_top_customers()
    generate_report()
    
    return pipeline.execute(parallel=True)

if __name__ == "__main__":
    results = run()
    print(f"\nPipeline completed: {results['tasks_executed']} tasks executed")
```

#### Step 6: Run the Pipeline

```bash
# Make sure output directory exists
mkdir -p output

# Run the workflow
python pipelines/workflows/sales_analytics_workflow.py

# Or use the runner
python run_workflow.py sales_analytics_workflow
```

### 7.2 Common Use Cases

#### CSV Processing Pipeline

```python
@pipeline.task()
def process_csv_files():
    """Process multiple CSV files."""
    from pathlib import Path
    
    all_data = []
    for csv_file in Path("data").glob("*.csv"):
        df = pd.read_csv(csv_file)
        # Add source file info
        df['source_file'] = csv_file.name
        all_data.append(df)
    
    combined = pd.concat(all_data, ignore_index=True)
    return pipeline.create_artifact(combined, "all_csv_data")
```

#### API Data Extraction

```python
@pipeline.task()
def extract_from_api():
    """Extract data from REST API."""
    import requests
    
    response = requests.get(
        "https://api.example.com/data",
        headers={"Authorization": f"Bearer {API_KEY}"},
        params={"limit": 1000}
    )
    response.raise_for_status()
    
    data = response.json()
    df = pd.DataFrame(data['results'])
    
    return pipeline.create_artifact(df, "api_data")
```

#### Database ETL

```python
from sqlalchemy import create_engine

@pipeline.task()
def extract_from_database():
    """Extract from SQL database."""
    engine = create_engine("postgresql://user:pass@localhost/db")
    
    query = """
    SELECT * FROM sales 
    WHERE date >= CURRENT_DATE - INTERVAL '30 days'
    """
    
    df = pd.read_sql(query, engine)
    return pipeline.create_artifact(df, "db_data")

@pipeline.task()
def load_to_database(transformed_data):
    """Load to SQL database."""
    engine = create_engine("postgresql://user:pass@localhost/db")
    df = transformed_data.as_dataframe()
    
    df.to_sql(
        'processed_sales',
        engine,
        if_exists='append',
        index=False
    )
```

#### Real-time Streaming

```python
from airpipe.core.streaming import MicroBatchProcessor, StreamConfig

def setup_streaming_pipeline():
    """Setup real-time streaming pipeline."""
    
    config = StreamConfig(
        batch_size=100,
        batch_interval=10.0,
        enable_monitoring=True
    )
    
    @pipeline.task()
    def process_stream_batch():
        batch = pipeline.get_artifact("stream_batch")
        df = batch.as_dataframe()
        
        # Process batch
        processed = df[df['value'] > threshold]
        
        # Save or forward
        processed.to_csv(f"output/batch_{batch.metadata.tags['batch_id']}.csv")
        
        return pipeline.create_artifact(processed, "processed_batch")
    
    processor = MicroBatchProcessor(pipeline, config)
    return processor
```

---

## Part 8: Reference

### 8.1 API Reference

#### TaskPipeline Methods

| Method | Description | Parameters | Returns |
|--------|-------------|------------|---------|
| `task()` | Decorator for task functions | `depends_on`, `produces`, `consumes` | Decorated function |
| `create_artifact()` | Create data artifact | `data`, `name`, `metadata` | DataArtifact |
| `get_artifact()` | Retrieve named artifact | `name` | DataArtifact |
| `execute()` | Run the pipeline | `parallel`, `max_workers` | Results dict |
| `visualize_dag()` | Visualize task dependencies | `format` | String (ASCII/Mermaid) |
| `get_task_statistics()` | Get pipeline statistics | None | Dict |
| `validate_dag()` | Check for cycles | None | None (raises on error) |

#### DataArtifact Methods

| Method | Description | Returns |
|--------|-------------|---------|
| `as_dataframe()` | Get as Pandas DataFrame | pd.DataFrame |
| `as_spark_dataframe()` | Get as Spark DataFrame | pyspark.sql.DataFrame |
| `as_dict()` | Get as dictionary | dict |
| `as_list()` | Get as list | list |
| `transform()` | Apply transformation function | DataArtifact |
| `save_to_disk()` | Persist to disk | None |
| `load_from_disk()` | Load from disk | DataArtifact |

### 8.2 Configuration Options

#### Pipeline Configuration

```python
pipeline = TaskPipeline(
    name="my_pipeline",
    parallel=True,           # Enable parallel execution
    max_workers=4,          # Max parallel tasks
    cache_artifacts=True,   # Cache artifacts in memory
    validate_dag=True       # Validate DAG on execute
)
```

#### Streaming Configuration

```python
StreamConfig(
    batch_size=1000,          # Records per batch
    batch_interval=30.0,      # Seconds between batches
    max_batches=None,         # None for infinite
    checkpoint_interval=10,   # Checkpoint every N batches
    enable_monitoring=True,   # Enable metrics
    watermark_delay="10 minutes"  # Late data handling
)
```

#### Spark Configuration

```python
SparkSessionManager.get_or_create({
    'app_name': 'Pipeline',
    'master': 'local[*]',     # or 'spark://host:7077'
    'config': {
        'spark.driver.memory': '4g',
        'spark.executor.memory': '4g',
        'spark.sql.shuffle.partitions': '200',
        'spark.sql.adaptive.enabled': 'true'
    }
})
```

### 8.3 Troubleshooting

#### Common Issues and Solutions

| Issue | Cause | Solution |
|-------|-------|----------|
| `ModuleNotFoundError: airpipe` | AirPipe not installed | Run `pip install -e .` in AirPipe directory |
| `RuntimeError: Circular dependency` | Tasks depend on each other | Review `depends_on` declarations |
| `KeyError: artifact not found` | Artifact name mismatch | Check `produces`/`consumes` names |
| `MemoryError` | Large datasets | Use Spark or process in chunks |
| Tasks not running in parallel | Parallel not enabled | Set `parallel=True` in `execute()` |
| Spark session fails | Spark not installed | Install with `pip install pyspark` |

#### Performance Optimization

1. **Enable Parallel Execution**
   ```python
   pipeline.execute(parallel=True, max_workers=8)
   ```

2. **Use Spark for Large Data**
   ```python
   # Convert large DataFrames to Spark
   spark_df = pandas_to_spark(large_df)
   ```

3. **Batch Processing**
   ```python
   # Process in chunks
   for chunk in pd.read_csv("huge.csv", chunksize=10000):
       process_chunk(chunk)
   ```

4. **Cache Frequently Used Data**
   ```python
   @pipeline.task()
   @lru_cache(maxsize=128)
   def expensive_extraction():
       # Cached after first call
       return load_large_dataset()
   ```

#### Debugging Tips

1. **Enable Detailed Logging**
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

2. **Visualize Task Dependencies**
   ```bash
   python run_workflow.py my_workflow --visualize
   ```

3. **Test Individual Tasks**
   ```python
   # Test single task
   def test_extract():
       result = extract()
       assert result is not None
       print(result.metadata)
   ```

4. **Use Interactive Debugging**
   ```python
   import pdb
   
   @pipeline.task()
   def debug_task(data):
       pdb.set_trace()  # Breakpoint
       # Your code here
   ```

---

## Part 9: Deployment

### 9.1 Production Deployment

#### Environment Setup

```bash
# Production requirements
cat > requirements-prod.txt << EOF
airpipe
pandas>=1.3.0
pyspark>=3.0.0  # If using Spark
psycopg2-binary  # For PostgreSQL
redis  # For caching
gunicorn  # For API serving
EOF

# Install production dependencies
pip install -r requirements-prod.txt
```

#### Docker Deployment

```dockerfile
# Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY pipelines/ ./pipelines/
COPY run_workflow.py .

# Run workflow
CMD ["python", "run_workflow.py", "production_workflow"]
```

```yaml
# docker-compose.yml
version: '3.8'
services:
  airpipe:
    build: .
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/mydb
      - REDIS_URL=redis://redis:6379
    depends_on:
      - db
      - redis
  
  db:
    image: postgres:13
    environment:
      - POSTGRES_PASSWORD=pass
  
  redis:
    image: redis:6-alpine
```

#### Scheduling with Cron

```bash
# Add to crontab
# Run daily at 2 AM
0 2 * * * cd /opt/airpipe && python run_workflow.py daily_etl >> /var/log/airpipe.log 2>&1

# Run every hour
0 * * * * cd /opt/airpipe && python run_workflow.py hourly_sync >> /var/log/airpipe.log 2>&1
```

#### Scheduling with Apache Airflow

```python
# airflow_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def run_airpipe_workflow():
    import subprocess
    result = subprocess.run(
        ["python", "/opt/airpipe/run_workflow.py", "my_workflow"],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Workflow failed: {result.stderr}")

dag = DAG(
    'airpipe_etl',
    default_args={
        'owner': 'data-team',
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    },
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1)
)

task = PythonOperator(
    task_id='run_etl',
    python_callable=run_airpipe_workflow,
    dag=dag
)
```

### 9.2 Scaling

#### Local to Cluster Migration

```python
# development.py
spark_config = {
    'master': 'local[*]',
    'config': {'spark.driver.memory': '2g'}
}

# production.py
spark_config = {
    'master': 'spark://spark-master:7077',
    'config': {
        'spark.driver.memory': '8g',
        'spark.executor.memory': '8g',
        'spark.executor.instances': '10',
        'spark.dynamicAllocation.enabled': 'true'
    }
}
```

#### Resource Management

```python
# Adaptive resource allocation
@pipeline.task()
def adaptive_processing(data):
    df = data.as_dataframe()
    
    # Use Spark for large datasets
    if len(df) > 1_000_000:
        spark_df = pandas_to_spark(df)
        # Process with Spark
        result = process_with_spark(spark_df)
        return pipeline.create_artifact(result, "processed")
    else:
        # Process with Pandas for small data
        result = process_with_pandas(df)
        return pipeline.create_artifact(result, "processed")
```

#### Performance Monitoring

```python
# Add performance tracking
import time
from functools import wraps

def track_performance(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        duration = time.time() - start
        
        # Log to monitoring system
        LOG.info(f"{func.__name__} took {duration:.2f} seconds")
        
        # Send to metrics service
        send_metric(f"pipeline.{func.__name__}.duration", duration)
        
        return result
    return wrapper

@pipeline.task()
@track_performance
def monitored_task(data):
    # Your processing logic
    pass
```

---

## Conclusion

AirPipe provides a powerful yet simple framework for building data pipelines. Its key strengths are:

- **Simplicity**: Write pipelines as plain Python functions
- **Flexibility**: Support for batch, streaming, and Spark processing
- **Scalability**: From local development to production clusters
- **Clean Architecture**: Clear separation of framework and business logic

### Next Steps

1. **Start Small**: Create a simple pipeline with the `simple` template
2. **Explore Examples**: Study the provided templates and workflows
3. **Build Your Pipeline**: Apply to your specific use case
4. **Scale as Needed**: Add streaming or Spark when required

### Getting Help

- **Documentation**: This guide and the code documentation
- **Examples**: Template projects provide working examples
- **Issues**: Report bugs or request features on GitHub

Happy data processing with AirPipe! 🚀