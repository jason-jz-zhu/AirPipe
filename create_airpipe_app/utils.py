"""Utilities for project creation and setup."""

import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional


def validate_project_name(name: str) -> bool:
    """Validate project name format."""
    # Allow letters, numbers, hyphens, underscores
    # Must start with letter or underscore
    pattern = r'^[a-zA-Z_][a-zA-Z0-9_-]*$'
    return bool(re.match(pattern, name)) and len(name) <= 50


def copy_file_or_directory(src: Path, dest: Path) -> None:
    """Copy a file or directory, creating parent directories as needed."""
    # Create parent directory if it doesn't exist
    dest.parent.mkdir(parents=True, exist_ok=True)
    
    if src.is_file():
        shutil.copy2(src, dest)
    elif src.is_dir():
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(src, dest)
    else:
        raise ValueError(f"Source path does not exist: {src}")


def setup_git_repo(project_dir: Path) -> bool:
    """Initialize git repository in project directory."""
    try:
        subprocess.run(
            ["git", "init"],
            cwd=project_dir,
            check=True,
            capture_output=True
        )
        
        # Create initial commit
        subprocess.run(
            ["git", "add", "."],
            cwd=project_dir, 
            check=True,
            capture_output=True
        )
        
        subprocess.run(
            ["git", "commit", "-m", "Initial commit - AirPipe project"],
            cwd=project_dir,
            check=True,
            capture_output=True
        )
        
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def create_virtual_env(project_dir: Path) -> bool:
    """Create virtual environment in project directory."""
    venv_dir = project_dir / "venv"
    
    try:
        subprocess.run(
            [sys.executable, "-m", "venv", str(venv_dir)],
            check=True,
            capture_output=True
        )
        return True
    except subprocess.CalledProcessError:
        return False


def install_dependencies(project_dir: Path, use_venv: bool = True) -> bool:
    """Install project dependencies."""
    requirements_file = project_dir / "requirements.txt"
    if not requirements_file.exists():
        return False
    
    try:
        if use_venv:
            # Use virtual environment pip if it exists
            venv_pip = project_dir / "venv" / "bin" / "pip"
            if not venv_pip.exists():
                # Windows path
                venv_pip = project_dir / "venv" / "Scripts" / "pip.exe"
            
            if venv_pip.exists():
                pip_cmd = str(venv_pip)
            else:
                pip_cmd = "pip"
        else:
            pip_cmd = "pip"
        
        subprocess.run(
            [pip_cmd, "install", "-r", str(requirements_file)],
            cwd=project_dir,
            check=True,
            capture_output=True
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def render_template_file(template_path: Path, output_path: Path, context: dict) -> None:
    """Render a template file with context variables."""
    try:
        # Simple template rendering - replace {{variable}} patterns
        content = template_path.read_text()
        
        for key, value in context.items():
            content = content.replace(f"{{{{{key}}}}}", str(value))
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(content)
    except Exception as e:
        raise RuntimeError(f"Failed to render template {template_path}: {e}")


def check_command_available(command: str) -> bool:
    """Check if a command is available in the system."""
    try:
        subprocess.run(
            [command, "--version"],
            capture_output=True,
            check=True
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False