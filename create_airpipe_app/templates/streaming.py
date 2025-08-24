"""Streaming template - streaming data processing pipeline."""

TEMPLATE = {
    "name": "streaming",
    "description": "Streaming data processing with micro-batches",
    "source_files": [
        # Core files
        ("run_workflow.py", "run_workflow.py"),
        ("run_streaming.py", "run_streaming.py"),
        ("pipelines/__init__.py", "pipelines/__init__.py"), 
        ("pipelines/workflows/__init__.py", "pipelines/workflows/__init__.py"),
        
        # Streaming workflow
        ("pipelines/workflows/streaming_example_workflow.py", "pipelines/workflows/streaming_example_workflow.py"),
        
        # Streaming components (if any exist in the future)
        ("pipelines/examples/streaming/", "pipelines/examples/streaming/"),
    ],
    "create_directories": [
        "pipelines/workflows",
        "pipelines/examples/streaming",
        "data", 
        "output",
    ],
    "sample_data": [],
    "readme_template": "streaming_readme.md.j2",
    "requirements": [
        "airpipe",
        "pandas>=1.3.0",
        "numpy>=1.21.0",
    ],
    "example_workflows": ["streaming_example_workflow"],
}