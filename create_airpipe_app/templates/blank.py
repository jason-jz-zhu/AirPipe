"""Blank template - minimal project structure."""

TEMPLATE = {
    "name": "blank",
    "description": "Clean project structure with no examples",
    "source_files": [
        # Copy run_workflow.py
        ("run_workflow.py", "run_workflow.py"),
        # Create minimal pipelines structure
        ("pipelines/__init__.py", "pipelines/__init__.py"),
        ("pipelines/workflows/__init__.py", "pipelines/workflows/__init__.py"),
    ],
    "create_directories": [
        "pipelines/workflows",
        "data",
        "output",
    ],
    "sample_data": [],
    "readme_template": "blank_readme.md.j2",
    "requirements": [
        "airpipe",
        "pandas>=1.3.0",
        "numpy>=1.21.0",
    ],
    "example_workflows": [],
}