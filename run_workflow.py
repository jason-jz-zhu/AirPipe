#!/usr/bin/env python
"""
Workflow runner for AirPipe ETL framework.

Usage:
    python run_workflow.py <workflow_name>
    python run_workflow.py --list
"""

import sys
import argparse
import importlib
from pathlib import Path
from typing import Dict, Any

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))


def discover_workflows():
    """Discover all workflows in the centralized workflows directory."""
    workflows_dir = Path(__file__).parent / "pipelines" / "workflows"
    workflows = {}
    
    # Find all workflow files in the centralized location
    for workflow_file in workflows_dir.glob("*_workflow.py"):
        if workflow_file.name == "__init__.py":
            continue
        
        # Simple workflow name (just the filename without extension)
        workflow_name = workflow_file.stem
        
        # Determine category from filename prefix
        if workflow_name.startswith('employee'):
            category = 'employee'
        elif workflow_name.startswith('advanced') or 'sales' in workflow_name:
            category = 'sales'
        elif workflow_name.startswith('simple') or workflow_name.startswith('streaming'):
            category = 'examples'
        else:
            category = 'unknown'
        
        workflows[workflow_name] = {
            'full_module_name': f"pipelines.workflows.{workflow_name}",
            'file_path': workflow_file,
            'pipeline_category': category
        }
    
    return workflows


def list_workflows():
    """List all available workflows."""
    workflows = discover_workflows()
    
    print("\nAvailable Task-Based Workflows:")
    print("=" * 50)
    
    # Group workflows by category
    categories = {}
    for name, info in workflows.items():
        category = info['pipeline_category']
        if category not in categories:
            categories[category] = []
        categories[category].append((name, info))
    
    for category, workflow_list in sorted(categories.items()):
        print(f"\n{category.upper()} PIPELINES:")
        print("-" * 30)
        
        for workflow_name, info in sorted(workflow_list):
            try:
                # Import the module to get its docstring
                module = importlib.import_module(info['full_module_name'])
                doc = module.__doc__ or "No description available"
                # Get first line of docstring
                description = doc.strip().split('\n')[0]
                
                print(f"  {workflow_name}")
                print(f"    {description}")
                    
            except Exception as e:
                print(f"  {workflow_name}")
                print(f"    Error loading: {e}")
    
    print("\n" + "=" * 50)
    print("Run a workflow with: python run_workflow.py <workflow_name>")
    print()


def run_workflow(workflow_name: str) -> Dict[str, Any]:
    """
    Run a specific workflow.
    
    Args:
        workflow_name: Name of the workflow module (without .py extension)
        
    Returns:
        Workflow execution results
    """
    workflows = discover_workflows()
    
    if workflow_name not in workflows:
        print(f"\nError: Workflow '{workflow_name}' not found")
        print("\nAvailable workflows:")
        list_workflows()
        sys.exit(1)
    
    workflow_info = workflows[workflow_name]
    
    try:
        # Import the workflow module using the full module path
        module = importlib.import_module(workflow_info['full_module_name'])
        
        # Check if module has a main or run function
        if hasattr(module, 'main'):
            print(f"\nRunning workflow: {workflow_name}")
            print("-" * 40)
            results = module.main()
            return results
        elif hasattr(module, 'run'):
            print(f"\nRunning workflow: {workflow_name}")
            print("-" * 40)
            results = module.run()
            return results
        else:
            print(f"Error: Workflow '{workflow_name}' has no main() or run() function")
            sys.exit(1)
            
    except ImportError as e:
        print(f"\nError: Could not import workflow '{workflow_name}'")
        print(f"Details: {e}")
        print(f"Module path: {workflow_info['full_module_name']}")
        sys.exit(1)
    except Exception as e:
        print(f"\nError running workflow: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def visualize_workflow(workflow_name: str, format: str = 'ascii') -> None:
    """
    Visualize a workflow's DAG without executing it.
    
    Args:
        workflow_name: Name of the workflow module
        format: Visualization format ('ascii' or 'mermaid')
    """
    workflows = discover_workflows()
    
    if workflow_name not in workflows:
        print(f"\nError: Workflow '{workflow_name}' not found")
        print("\nAvailable workflows:")
        list_workflows()
        sys.exit(1)
    
    workflow_info = workflows[workflow_name]
    
    try:
        # Import the workflow module using the full module path
        module = importlib.import_module(workflow_info['full_module_name'])
        
        # Get the pipeline object
        if hasattr(module, 'pipeline'):
            pipeline = module.pipeline
            
            print(f"\nVisualizing workflow: {workflow_name}")
            print("=" * 60)
            
            # Generate visualization
            visualization = pipeline.visualize_dag(format=format)
            print(visualization)
            
            # Also show statistics
            stats = pipeline.get_task_statistics()
            print("\n" + "=" * 60)
            print("Pipeline Statistics:")
            print("-" * 40)
            print(f"Total tasks: {stats['total_tasks']}")
            print(f"Task types: {stats['task_types']}")
            print(f"Critical path length: {stats['critical_path_length']}")
            print(f"Root tasks: {', '.join(stats['root_tasks'])}")
            
            # Validate DAG
            try:
                pipeline.validate_dag()
                print("\n✓ DAG is valid (no cycles detected)")
            except RuntimeError as e:
                print(f"\n✗ DAG validation failed: {e}")
        else:
            print(f"Error: Workflow '{workflow_name}' has no 'pipeline' object")
            print("Make sure the workflow uses TaskPipeline")
            sys.exit(1)
            
    except ImportError as e:
        print(f"\nError: Could not import workflow '{workflow_name}'")
        print(f"Details: {e}")
        print(f"Module path: {workflow_info['full_module_name']}")
        sys.exit(1)
    except Exception as e:
        print(f"\nError visualizing workflow: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run AirPipe ETL workflows",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_workflow.py --list                    # List all workflows
  python run_workflow.py employee_task_workflow    # Run employee workflow
  python run_workflow.py simple_task_workflow      # Run simple workflow
        """
    )
    
    parser.add_argument(
        "workflow",
        nargs="?",
        help="Name of the workflow to run (without .py extension)"
    )
    
    parser.add_argument(
        "--list", "-l",
        action="store_true",
        help="List all available workflows"
    )
    
    parser.add_argument(
        "--visualize", "-v",
        action="store_true",
        help="Visualize workflow DAG without executing"
    )
    
    parser.add_argument(
        "--format", "-f",
        choices=['ascii', 'mermaid'],
        default='ascii',
        help="DAG visualization format (default: ascii)"
    )
    
    args = parser.parse_args()
    
    if args.list or not args.workflow:
        list_workflows()
    elif args.visualize and args.workflow:
        # Just visualize the workflow DAG
        visualize_workflow(args.workflow, format=args.format)
    else:
        results = run_workflow(args.workflow)
        
        # Print final summary if results provided
        if results and isinstance(results, dict):
            print("\n" + "=" * 50)
            print("Workflow Execution Summary")
            print("=" * 50)
            if 'tasks_executed' in results:
                print(f"Tasks executed: {results['tasks_executed']}")
            if 'artifacts_created' in results:
                print(f"Artifacts created: {results['artifacts_created']}")
            print("=" * 50)


if __name__ == "__main__":
    main()