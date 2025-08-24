"""
Setup configuration for AirPipe ETL framework.
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README
readme_path = Path(__file__).parent / "README.md"
long_description = readme_path.read_text() if readme_path.exists() else ""

setup(
    name="airpipe",
    version="0.1.0",
    author="AirPipe Team",
    description="A production-grade ETL framework with pluggable components",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/airpipe",
    packages=find_packages(exclude=["tests*", "examples*"]),
    python_requires=">=3.8",
    install_requires=[
        "pandas>=1.3.0",
        "pyyaml>=5.4",
        "requests>=2.26.0",
        "sqlalchemy>=1.4.0",
        "tabulate>=0.8.9",
    ],
    extras_require={
        "dev": [
            "pytest>=6.2.0",
            "pytest-cov>=2.12.0",
            "black>=21.6b0",
            "flake8>=3.9.0",
            "mypy>=0.910",
        ],
        "postgres": ["psycopg2-binary>=2.9.0"],
        "mysql": ["pymysql>=1.0.0"],
        "mssql": ["pyodbc>=4.0.0"],
    },
    entry_points={
        "console_scripts": [
            "airpipe=airpipe.cli:main",
            "create-airpipe-app=create_airpipe_app.cli:main",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
        "Topic :: System :: Archiving",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)