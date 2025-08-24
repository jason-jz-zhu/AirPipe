"""Template management for AirPipe project generation."""

import importlib
import shutil
from pathlib import Path
from typing import Dict, Any

from .utils import copy_file_or_directory, render_template_file


class TemplateManager:
    """Manages template definitions and project creation."""
    
    def __init__(self):
        self.template_dir = Path(__file__).parent / "templates"
        self.project_root = Path(__file__).parent.parent
        
    def list_templates(self) -> Dict[str, str]:
        """List available templates with descriptions."""
        templates = {}
        
        template_names = ["blank", "simple", "employee", "sales", "streaming", "spark", "full"]
        
        for name in template_names:
            try:
                template_def = self._load_template_definition(name)
                templates[name] = template_def["description"]
            except Exception as e:
                print(f"Warning: Could not load template '{name}': {e}")
                
        return templates
    
    def create_project(self, project_name: str, template_name: str, output_dir: Path):
        """Create a new project from template."""
        template_def = self._load_template_definition(template_name)
        project_dir = output_dir / project_name
        
        # Create project directory
        project_dir.mkdir(parents=True, exist_ok=True)
        
        # Create directory structure
        self._create_directories(project_dir, template_def)
        
        # Copy source files
        self._copy_source_files(project_dir, template_def)
        
        # Copy sample data
        self._copy_sample_data(project_dir, template_def)
        
        # Generate project files
        self._generate_project_files(project_dir, project_name, template_def)
        
        print(f"✓ Project '{project_name}' created successfully!")
    
    def _load_template_definition(self, template_name: str) -> Dict[str, Any]:
        """Load template definition from module."""
        try:
            module = importlib.import_module(f".templates.{template_name}", package="create_airpipe_app")
            return module.TEMPLATE
        except ImportError as e:
            raise ValueError(f"Template '{template_name}' not found") from e
    
    def _create_directories(self, project_dir: Path, template_def: Dict[str, Any]):
        """Create directory structure."""
        for dir_path in template_def.get("create_directories", []):
            (project_dir / dir_path).mkdir(parents=True, exist_ok=True)
    
    def _copy_source_files(self, project_dir: Path, template_def: Dict[str, Any]):
        """Copy source files from current AirPipe project."""
        for src_path, dest_path in template_def.get("source_files", []):
            src_full = self.project_root / src_path
            dest_full = project_dir / dest_path
            
            if src_full.exists():
                copy_file_or_directory(src_full, dest_full)
            else:
                print(f"Warning: Source file not found: {src_full}")
    
    def _copy_sample_data(self, project_dir: Path, template_def: Dict[str, Any]):
        """Copy sample data files."""
        for src_path, dest_path in template_def.get("sample_data", []):
            src_full = self.project_root / src_path
            dest_full = project_dir / dest_path
            
            if src_full.exists():
                copy_file_or_directory(src_full, dest_full)
            else:
                print(f"Warning: Sample data file not found: {src_full}")
    
    def _generate_project_files(self, project_dir: Path, project_name: str, template_def: Dict[str, Any]):
        """Generate project-specific files."""
        # Generate requirements.txt
        self._generate_requirements_txt(project_dir, template_def)
        
        # Generate README.md
        self._generate_readme(project_dir, project_name, template_def)
        
        # Generate .gitignore
        self._generate_gitignore(project_dir)
    
    def _generate_requirements_txt(self, project_dir: Path, template_def: Dict[str, Any]):
        """Generate requirements.txt file."""
        requirements = template_def.get("requirements", ["airpipe"])
        
        requirements_content = """# AirPipe Project Dependencies
#
# Install AirPipe framework using one of these methods:
# 
# Option 1: Install from local development (if you have the source)
#   pip install -e /path/to/airpipe
#
# Option 2: Install from Git repository
#   pip install git+https://github.com/yourusername/airpipe.git
#
# Option 3: Install from PyPI (when published)
#   pip install airpipe
#
# For now, using local development install:

"""
        
        # Check if airpipe is installed and get its location
        import subprocess
        try:
            result = subprocess.run(
                ["pip", "show", "airpipe"],
                capture_output=True,
                text=True,
                check=False
            )
            if result.returncode == 0:
                # AirPipe is installed, try to get location
                for line in result.stdout.split('\n'):
                    if line.startswith('Location:'):
                        location = line.split(':', 1)[1].strip()
                        if location:
                            requirements_content += f"# Currently installed from: {location}\n"
                            break
        except:
            pass
        
        requirements_content += "\n"
        for req in requirements:
            requirements_content += f"{req}\n"
        
        (project_dir / "requirements.txt").write_text(requirements_content)
    
    def _generate_readme(self, project_dir: Path, project_name: str, template_def: Dict[str, Any]):
        """Generate README.md file."""
        template_name = template_def["name"]
        description = template_def["description"]
        example_workflows = template_def.get("example_workflows", [])
        
        readme_content = f"""# {project_name}

{description}

## Prerequisites

This project requires the AirPipe ETL framework to be installed. AirPipe provides the core functionality for building and running data pipelines.

### Installing AirPipe

If AirPipe is not already installed, you can install it using one of these methods:

1. **From local source (development)**:
   ```bash
   git clone https://github.com/yourusername/airpipe.git
   cd airpipe
   pip install -e .
   ```

2. **From Git repository**:
   ```bash
   pip install git+https://github.com/yourusername/airpipe.git
   ```

3. **From PyPI** (when published):
   ```bash
   pip install airpipe
   ```

## Getting Started

### Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

### Running Workflows

List available workflows:
```bash
python run_workflow.py --list
```
"""
        
        if example_workflows:
            readme_content += f"""
Run example workflows:
```bash
"""
            for workflow in example_workflows:
                readme_content += f"python run_workflow.py {workflow}\n"
            readme_content += "```"
        
        readme_content += f"""

### Project Structure

```
{project_name}/
├── README.md                 # This file
├── requirements.txt          # Python dependencies  
├── run_workflow.py          # Workflow runner CLI"""
        
        # Add run_streaming.py if template includes streaming
        if "streaming" in template_name or template_name == "full":
            readme_content += """
├── run_streaming.py         # Streaming runner CLI"""
        
        readme_content += """
├── pipelines/               # Your ETL logic
│   ├── workflows/           # Workflow definitions (orchestration)
│   └── [domain]/            # Domain-specific components
│       ├── extractors/      # Data extraction logic
│       ├── transformers/    # Data transformation logic
│       └── loaders/         # Data loading logic
├── data/                    # Input data files
└── output/                  # Output files

```

### Creating New Workflows

1. Create workflow file in `pipelines/workflows/`
2. Create domain components in `pipelines/[domain]/`  
3. Import components in workflow and define tasks
4. Run with `python run_workflow.py <workflow_name>`

### DAG Visualization

Visualize workflow dependencies:
```bash
python run_workflow.py <workflow_name> --visualize
python run_workflow.py <workflow_name> --visualize --format mermaid
```

## Architecture Notes

**Important**: This project uses the AirPipe framework as an external dependency. The `airpipe/` folder is NOT included in this project - it's installed as a Python package. This design ensures:

- Clean separation between framework and business logic
- Easy framework updates without modifying project code
- Standard Python packaging practices
- No code duplication

Your business logic lives in the `pipelines/` directory, while the framework provides core functionality through the installed `airpipe` package.

## Learn More

- [AirPipe Documentation](https://github.com/yourusername/AirPipe)
- Check example workflows in `pipelines/workflows/`
- Explore domain components in `pipelines/[domain]/`

Happy data processing! 🚀
"""
        
        (project_dir / "README.md").write_text(readme_content)
    
    def _generate_gitignore(self, project_dir: Path):
        """Generate .gitignore file."""
        gitignore_content = """# Byte-compiled / optimized / DLL files
__pycache__/
*.py[cod]
*$py.class

# C extensions
*.so

# Distribution / packaging
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg
MANIFEST

# PyInstaller
#  Usually these files are written by a python script from a template
#  before PyInstaller builds the exe, so as to inject date/other infos into it.
*.manifest
*.spec

# Installer logs
pip-log.txt
pip-delete-this-directory.txt

# Unit test / coverage reports
htmlcov/
.tox/
.nox/
.coverage
.coverage.*
.cache
nosetests.xml
coverage.xml
*.cover
.hypothesis/
.pytest_cache/

# Translations
*.mo
*.pot

# Django stuff:
*.log
local_settings.py
db.sqlite3

# Flask stuff:
instance/
.webassets-cache

# Scrapy stuff:
.scrapy

# Sphinx documentation
docs/_build/

# PyBuilder
target/

# Jupyter Notebook
.ipynb_checkpoints

# IPython
profile_default/
ipython_config.py

# pyenv
.python-version

# celery beat schedule file
celerybeat-schedule

# SageMath parsed files
*.sage.py

# Environments
.env
.venv
env/
venv/
ENV/
env.bak/
venv.bak/

# Spyder project settings
.spyderproject
.spyproject

# Rope project settings
.ropeproject

# mkdocs documentation
/site

# mypy
.mypy_cache/
.dmypy.json
dmypy.json

# AirPipe specific
output/
*.csv
*.json
*.parquet
.DS_Store
"""
        
        (project_dir / ".gitignore").write_text(gitignore_content)