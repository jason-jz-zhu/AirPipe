"""Simple template - basic ETL example."""

TEMPLATE = {
    "name": "simple", 
    "description": "Basic ETL workflow example",
    "source_files": [
        # Core files
        ("run_workflow.py", "run_workflow.py"),
        ("pipelines/__init__.py", "pipelines/__init__.py"),
        ("pipelines/workflows/__init__.py", "pipelines/workflows/__init__.py"),
        
        # Simple workflow
        ("pipelines/workflows/simple_task_workflow.py", "pipelines/workflows/simple_task_workflow.py"),
        
        # Simple components
        ("pipelines/examples/simple/", "pipelines/examples/simple/"),
    ],
    "create_directories": [
        "pipelines/workflows", 
        "pipelines/examples/simple/extractors",
        "pipelines/examples/simple/transformers", 
        "pipelines/examples/simple/loaders",
        "data",
        "output",
    ],
    "sample_data": [],
    "readme_template": "simple_readme.md.j2", 
    "requirements": [
        "airpipe",
        "pandas>=1.3.0",
        "numpy>=1.21.0",
    ],
    "example_workflows": ["simple_task_workflow"],
}