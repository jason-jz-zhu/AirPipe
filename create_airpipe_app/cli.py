#!/usr/bin/env python3
"""
AirPipe Project Generator CLI

Usage:
    create-airpipe-app my-project
    create-airpipe-app my-project --template employee
    python -m create_airpipe_app my-project --template simple
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

from .template_manager import TemplateManager
from .utils import validate_project_name, setup_git_repo, create_virtual_env


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Create a new AirPipe project",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  create-airpipe-app my-project                    # Interactive template selection
  create-airpipe-app my-project --template simple # Use simple template
  create-airpipe-app my-project --template employee --no-git  # Skip git setup
        """
    )
    
    parser.add_argument(
        "project_name",
        help="Name of the project to create"
    )
    
    parser.add_argument(
        "--template", "-t",
        choices=["blank", "simple", "employee", "sales", "streaming", "spark", "full"],
        help="Template to use (default: interactive selection)"
    )
    
    parser.add_argument(
        "--output-dir", "-o",
        default=".",
        help="Directory to create project in (default: current directory)"
    )
    
    parser.add_argument(
        "--no-git",
        action="store_true",
        help="Skip git repository initialization"
    )
    
    parser.add_argument(
        "--no-venv",
        action="store_true", 
        help="Skip virtual environment setup"
    )
    
    parser.add_argument(
        "--no-install",
        action="store_true",
        help="Skip dependency installation"
    )
    
    args = parser.parse_args()
    
    # Validate project name
    if not validate_project_name(args.project_name):
        print("❌ Invalid project name. Use letters, numbers, hyphens, and underscores only.")
        sys.exit(1)
    
    # Determine output directory
    output_dir = Path(args.output_dir).resolve()
    project_dir = output_dir / args.project_name
    
    # Check if directory already exists
    if project_dir.exists():
        print(f"❌ Directory '{project_dir}' already exists.")
        sys.exit(1)
    
    # Initialize template manager
    template_manager = TemplateManager()
    
    # Select template
    template_name = args.template
    if not template_name:
        template_name = interactive_template_selection(template_manager)
    
    if not template_name:
        print("❌ No template selected.")
        sys.exit(1)
    
    # Check if AirPipe is installed before creating project
    try:
        import airpipe
        print("✓ AirPipe framework detected")
    except ImportError:
        print("⚠ Warning: AirPipe framework is not installed!")
        print("\nPlease install AirPipe first using one of these methods:")
        print("  1. From source: pip install -e /path/to/airpipe")
        print("  2. From Git: pip install git+https://github.com/yourusername/airpipe.git")
        print("\nContinuing anyway, but the project won't work until AirPipe is installed.")
        response = input("\nContinue? (y/N): ")
        if response.lower() != 'y':
            print("Aborted.")
            sys.exit(1)
    
    print(f"🚀 Creating AirPipe project '{args.project_name}' with '{template_name}' template...")
    
    try:
        # Create project from template
        template_manager.create_project(
            project_name=args.project_name,
            template_name=template_name,
            output_dir=output_dir
        )
        
        print("✓ Project files created")
        
        # Initialize git repository
        if not args.no_git:
            if setup_git_repo(project_dir):
                print("✓ Git repository initialized")
            else:
                print("⚠ Git repository initialization failed (git not found)")
        
        # Set up virtual environment
        if not args.no_venv:
            if create_virtual_env(project_dir):
                print("✓ Virtual environment created")
            else:
                print("⚠ Virtual environment creation failed")
        
        # Install dependencies
        if not args.no_install:
            print("📦 Installing dependencies...")
            from .utils import install_dependencies
            if install_dependencies(project_dir, use_venv=not args.no_venv):
                print("✓ Dependencies installed")
            else:
                print("⚠ Dependency installation failed")
        
        print_success_message(args.project_name, template_name, project_dir)
        
    except Exception as e:
        print(f"❌ Error creating project: {e}")
        sys.exit(1)


def interactive_template_selection(template_manager: TemplateManager) -> Optional[str]:
    """Interactive template selection."""
    templates = template_manager.list_templates()
    
    print("\n📋 Available templates:")
    for i, (name, description) in enumerate(templates.items(), 1):
        print(f"  {i}. {name:12} - {description}")
    
    while True:
        try:
            choice = input(f"\n? Choose a template (1-{len(templates)}): ").strip()
            if not choice:
                return None
            
            index = int(choice) - 1
            if 0 <= index < len(templates):
                return list(templates.keys())[index]
            else:
                print(f"Please enter a number between 1 and {len(templates)}")
        except (ValueError, KeyboardInterrupt):
            return None


def print_success_message(project_name: str, template_name: str, project_dir: Path):
    """Print success message with next steps."""
    print(f"""
🎉 Project '{project_name}' created successfully!

📁 Project location: {project_dir}
📄 Template used: {template_name}

🚀 Next steps:
  cd {project_name}
  python run_workflow.py --list
  python run_workflow.py <workflow_name>

📚 Learn more:
  - Edit workflows in pipelines/workflows/
  - Add components in pipelines/[domain]/
  - Check README.md for details

Happy data processing! 🌟
""")


if __name__ == "__main__":
    main()