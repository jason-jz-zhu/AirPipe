"""
AirPipe - A task-based ETL framework using imperative Python code.
"""

__version__ = "0.2.0"
__author__ = "AirPipe Team"

from airpipe.core.task import TaskPipeline, task
from airpipe.artifacts.data_artifact import DataArtifact

__all__ = [
    "TaskPipeline",
    "task",
    "DataArtifact",
]