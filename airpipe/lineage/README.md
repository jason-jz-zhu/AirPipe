# Apache Spline Integration for AirPipe

This module provides data lineage tracking for AirPipe pipelines using Apache Spline.

## Overview

Apache Spline is an open-source data lineage tracking system that captures and visualizes data flow across your pipelines. This integration automatically tracks:

- Task execution flow and dependencies
- Input and output datasets
- Data transformations
- Execution metrics and performance
- Schema information

## Prerequisites

### 1. Apache Spline Server

You need a running Apache Spline server. The easiest way to get started:

```bash
# Using Docker Compose
wget https://raw.githubusercontent.com/AbsaOSS/spline-getting-started/main/docker/docker-compose.yml
wget https://raw.githubusercontent.com/AbsaOSS/spline-getting-started/main/docker/.env
docker-compose up
```

This will start:
- Spline Server on http://localhost:8080
- ArangoDB (graph database) on http://localhost:8529
- Spline UI on http://localhost:8080

### 2. Python Dependencies

The integration uses only the `requests` library, which is already included in AirPipe's requirements.

## Quick Start

### Basic Usage

```python
from airpipe.core.task import TaskPipeline
from airpipe.lineage.spline_tracker import SplineLineageTracker
from airpipe.lineage.config import SplineConfig

# Configure Spline
config = SplineConfig(
    spline_url="http://localhost:8080",
    enabled=True,
    capture_schemas=True,
    capture_row_counts=True
)

# Create lineage tracker
tracker = SplineLineageTracker(config)

# Create pipeline with lineage tracking
pipeline = TaskPipeline("my_pipeline", lineage_tracker=tracker)

# Define your tasks as usual
@pipeline.task()
def extract():
    # Your extraction logic
    pass

# Execute pipeline - lineage is automatically captured
pipeline.execute()
```

### Configuration Options

#### From Environment Variables

```bash
export SPLINE_URL=http://localhost:8080
export SPLINE_ENABLED=true
export SPLINE_ENVIRONMENT=production
export SPLINE_APP_NAME=MyDataPipeline
```

```python
# Automatically loads from environment
config = SplineConfig.from_env()
tracker = SplineLineageTracker(config)
```

#### From Configuration File

```json
{
  "spline_url": "http://localhost:8080",
  "enabled": true,
  "capture_schemas": true,
  "capture_row_counts": true,
  "application_name": "MyApp",
  "environment": "production"
}
```

```python
config = SplineConfig.from_file("config/spline.json")
tracker = SplineLineageTracker(config)
```

### Full Configuration Reference

```python
config = SplineConfig(
    # Spline Server settings
    spline_url="http://localhost:8080",          # Spline server URL
    producer_api_path="/producer/v1/lineage",    # API endpoint path
    
    # Authentication (if needed)
    auth_enabled=False,                          # Enable authentication
    auth_token=None,                             # Bearer token
    
    # Capture settings
    enabled=True,                                # Enable/disable tracking
    capture_schemas=True,                        # Capture data schemas
    capture_row_counts=True,                     # Capture row counts
    capture_execution_time=True,                 # Track execution times
    batch_size=1,                                # Events per batch
    
    # Metadata
    application_name="AirPipe",                  # Your app name
    application_version="1.0.0",                 # App version
    environment="development",                   # Environment name
    
    # Advanced
    timeout_seconds=30,                          # HTTP timeout
    retry_count=3,                               # Retry attempts
    verify_ssl=True,                            # SSL verification
    
    # Custom metadata
    custom_metadata={
        "team": "Data Engineering",
        "project": "ETL Pipeline"
    }
)
```

## Running the Example

We provide a complete example workflow with Spline integration:

```bash
# Run with Spline tracking (default)
python pipelines/workflows/spline_example_workflow.py

# Run without Spline tracking
python pipelines/workflows/spline_example_workflow.py --no-spline

# Specify custom Spline server
python pipelines/workflows/spline_example_workflow.py --spline-url http://my-spline:8080

# Just visualize the DAG
python pipelines/workflows/spline_example_workflow.py --visualize
```

## Viewing Lineage in Spline UI

1. Open Spline UI: http://localhost:8080
2. Navigate to "Executions" or "Lineage" section
3. Find your pipeline execution by name
4. Click to view the interactive lineage graph

The UI shows:
- Complete data flow graph
- Task dependencies
- Input/output datasets
- Execution times
- Data schemas
- Row counts

## What Gets Tracked

### Automatic Tracking

The integration automatically captures:

1. **Pipeline Metadata**
   - Pipeline name and ID
   - Start/end times
   - Total execution duration
   - Success/failure status

2. **Task Information**
   - Task names and types (extractor/transformer/loader)
   - Dependencies between tasks
   - Execution duration per task
   - Input/output artifacts

3. **Data Artifacts**
   - Artifact names and IDs
   - Data formats (pandas, spark, etc.)
   - Schemas (column names and types)
   - Row counts
   - Relationships between artifacts

4. **Execution Metrics**
   - Task execution times
   - Pipeline parallelism settings
   - Error messages (if any)

### Custom Metadata

You can add custom metadata at various levels:

```python
# Pipeline-level metadata
config.custom_metadata = {
    "team": "Analytics",
    "cost_center": "12345",
    "sla_hours": 2
}

# This metadata is included with all lineage events
```

## Architecture

The integration consists of three main components:

### 1. SplineLineageTracker
- Tracks pipeline and task execution
- Captures data artifacts and schemas
- Sends lineage events to Spline server

### 2. SplineConfig
- Manages configuration settings
- Supports environment variables and files
- Provides sensible defaults

### 3. Data Models
- Represents Spline's lineage data structures
- Handles serialization for Spline API
- Maps AirPipe concepts to Spline concepts

## How It Works

1. **Pipeline Start**: When `pipeline.execute()` is called, the tracker creates a new execution plan
2. **Task Execution**: Before and after each task, the tracker captures:
   - Task metadata and dependencies
   - Input artifacts consumed
   - Output artifacts produced
3. **Artifact Creation**: When artifacts are created, the tracker captures:
   - Data schemas (for DataFrames)
   - Row counts and formats
   - Lineage relationships
4. **Pipeline End**: The complete lineage is sent to Spline server

## Troubleshooting

### Spline Server Not Reachable

```python
# Check if Spline is running
import requests
response = requests.get("http://localhost:8080/health")
print(response.status_code)  # Should be 200
```

### Lineage Not Appearing in UI

1. Check logs for errors:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

2. Verify configuration:
```python
print(config.producer_url)  # Should be full URL
print(config.enabled)       # Should be True
```

3. Test connection manually:
```python
tracker._send_event(test_event)  # Check return value
```

### Performance Considerations

- **Batching**: Set `batch_size > 1` to batch multiple events
- **Async**: The tracker sends events synchronously by default
- **Schemas**: Set `capture_schemas=False` for large datasets
- **Row Counts**: Set `capture_row_counts=False` for performance

## Advanced Usage

### Conditional Tracking

```python
# Only track in production
if os.getenv('ENVIRONMENT') == 'production':
    tracker = SplineLineageTracker(config)
else:
    tracker = None

pipeline = TaskPipeline("my_pipeline", lineage_tracker=tracker)
```

### Custom Lineage Events

```python
# Extend SplineLineageTracker for custom behavior
class CustomLineageTracker(SplineLineageTracker):
    def track_task_complete(self, task_name, result, input_artifacts, output_artifact):
        # Add custom logic
        super().track_task_complete(task_name, result, input_artifacts, output_artifact)
        
        # Send custom metrics
        self.send_custom_metric(task_name, custom_data)
```

### Integration with CI/CD

```yaml
# GitHub Actions example
- name: Run Pipeline with Lineage
  env:
    SPLINE_URL: ${{ secrets.SPLINE_URL }}
    SPLINE_AUTH_TOKEN: ${{ secrets.SPLINE_TOKEN }}
    SPLINE_ENVIRONMENT: ci
  run: |
    python my_pipeline.py
```

## Limitations

1. **Spline Server Required**: Must have a running Spline instance
2. **Synchronous Sending**: Currently sends events synchronously (may add async in future)
3. **Memory Artifacts**: Artifacts are tracked as "memory://" URIs (not actual file paths)

## Future Enhancements

Planned improvements:
- Asynchronous event sending
- Kafka producer support
- OpenLineage format support
- Custom serializers for complex data types
- Integration with more visualization tools

## Support

For issues or questions:
1. Check the example workflow: `pipelines/workflows/spline_example_workflow.py`
2. Enable debug logging to see detailed tracking information
3. Verify Spline server is running and accessible
4. Check Spline UI for any partial lineage data

## Resources

- [Apache Spline Documentation](https://absaoss.github.io/spline/)
- [Spline GitHub Repository](https://github.com/AbsaOSS/spline)
- [Spline Getting Started](https://github.com/AbsaOSS/spline-getting-started)
- [AirPipe Documentation](../../README.md)