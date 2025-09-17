"""Full template - all examples for learning and reference."""

TEMPLATE = {
    "name": "full",
    "description": "Complete example project with all pipeline types",
    "source_files": [
        # Core files
        ("run_workflow.py", "run_workflow.py"),
        ("run_streaming.py", "run_streaming.py"),
        ("pipelines/__init__.py", "pipelines/__init__.py"),
        
        # All workflows
        ("pipelines/workflows/", "pipelines/workflows/"),
        
        # All pipeline components
        ("pipelines/employee/", "pipelines/employee/"),
        ("pipelines/sales/", "pipelines/sales/"),  
        ("pipelines/examples/", "pipelines/examples/"),
    ],
    "create_directories": [
        "pipelines/workflows",
        "pipelines/employee/extractors",
        "pipelines/employee/transformers",
        "pipelines/employee/loaders", 
        "pipelines/sales/extractors",
        "pipelines/sales/transformers",
        "pipelines/sales/loaders",
        "pipelines/examples/simple/extractors",
        "pipelines/examples/simple/transformers",
        "pipelines/examples/simple/loaders",
        "pipelines/examples/streaming",
        "data",
        "output", 
        "examples",
    ],
    "sample_data": [
        ("examples/sample_data.csv", "examples/sample_data.csv"),
    ],
    "readme_template": "full_readme.md.j2",
    "requirements": [
        "airpipe",
        "pandas>=1.3.0",
        "numpy>=1.21.0", 
    ],
    "example_workflows": [
        "simple_task_workflow",
        "employee_task_workflow", 
        "employee_enhanced_workflow",
        "advanced_task_workflow",
        "streaming_example_workflow"
    ],
}