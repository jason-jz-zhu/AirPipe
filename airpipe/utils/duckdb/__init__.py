"""
DuckDB Integration for AirPipe.

This module provides DuckDB support for AirPipe pipelines, including:
- Session management for DuckDB connections
- SQL task decorator for pipeline tasks
- DuckDB-enhanced artifacts
- Common operations and utilities
"""

from .session import DuckDBSession
from .operations import DuckDBOperations
from .artifact import DuckDBArtifact
from .sql_task import (
    sql_task,
    SQLPipeline,
    add_sql_task_to_pipeline
)

__all__ = [
    'DuckDBSession',
    'DuckDBOperations',
    'DuckDBArtifact',
    'sql_task',
    'SQLPipeline'
]

# Auto-initialize SQL task support
add_sql_task_to_pipeline()