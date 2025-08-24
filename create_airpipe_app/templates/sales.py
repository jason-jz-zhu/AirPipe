"""Sales template - sales analytics pipeline."""

TEMPLATE = {
    "name": "sales",
    "description": "Advanced sales analytics with regional insights",
    "source_files": [
        # Core files
        ("run_workflow.py", "run_workflow.py"), 
        ("pipelines/__init__.py", "pipelines/__init__.py"),
        ("pipelines/workflows/__init__.py", "pipelines/workflows/__init__.py"),
        
        # Sales workflow
        ("pipelines/workflows/advanced_task_workflow.py", "pipelines/workflows/advanced_task_workflow.py"),
        
        # Sales components  
        ("pipelines/sales/", "pipelines/sales/"),
    ],
    "create_directories": [
        "pipelines/workflows",
        "pipelines/sales/extractors", 
        "pipelines/sales/transformers",
        "pipelines/sales/loaders",
        "data",
        "output",
    ],
    "sample_data": [],
    "readme_template": "sales_readme.md.j2",
    "requirements": [
        "airpipe",
        "pandas>=1.3.0", 
        "numpy>=1.21.0",
    ],
    "example_workflows": ["advanced_task_workflow"],
}