"""Employee template - employee data analytics pipeline."""

TEMPLATE = {
    "name": "employee",
    "description": "Employee data analytics pipeline with HR insights", 
    "source_files": [
        # Core files
        ("run_workflow.py", "run_workflow.py"),
        ("pipelines/__init__.py", "pipelines/__init__.py"),
        ("pipelines/workflows/__init__.py", "pipelines/workflows/__init__.py"),
        
        # Employee workflows
        ("pipelines/workflows/employee_task_workflow.py", "pipelines/workflows/employee_task_workflow.py"),
        ("pipelines/workflows/employee_enhanced_workflow.py", "pipelines/workflows/employee_enhanced_workflow.py"),
        
        # Employee components
        ("pipelines/employee/", "pipelines/employee/"),
    ],
    "create_directories": [
        "pipelines/workflows",
        "pipelines/employee/extractors",
        "pipelines/employee/transformers", 
        "pipelines/employee/loaders",
        "data",
        "output",
        "examples",
    ],
    "sample_data": [
        ("examples/sample_data.csv", "examples/sample_data.csv"),
    ],
    "readme_template": "employee_readme.md.j2",
    "requirements": [
        "airpipe", 
        "pandas>=1.3.0",
        "numpy>=1.21.0",
    ],
    "example_workflows": ["employee_task_workflow", "employee_enhanced_workflow"],
}