"""Spark template - Big data processing with Apache Spark."""

TEMPLATE = {
    "name": "spark",
    "description": "Big data processing with Apache Spark",
    "source_files": [
        # Core files
        ("run_workflow.py", "run_workflow.py"),
        ("pipelines/__init__.py", "pipelines/__init__.py"),
        ("pipelines/workflows/__init__.py", "pipelines/workflows/__init__.py"),
        
        # Spark workflow
        ("pipelines/workflows/spark_log_analysis_workflow.py", "pipelines/workflows/spark_log_analysis_workflow.py"),
    ],
    "create_directories": [
        "pipelines/workflows",
        "data",
        "data/logs",  # For log files
        "output",
        "output/spark_log_analysis",  # For Spark output
    ],
    "sample_data": [],
    "readme_template": "spark_readme.md.j2",
    "requirements": [
        "airpipe",
        "pyspark>=3.0.0",
        "pyarrow>=6.0.0",  # For Parquet and Arrow optimization
        "pandas>=1.3.0",
        "numpy>=1.21.0",
    ],
    "example_workflows": ["spark_log_analysis_workflow"],
}