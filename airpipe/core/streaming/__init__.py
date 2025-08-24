"""
Streaming support for AirPipe - Micro-batch processing for existing pipelines.
"""

from .micro_batch import (
    MicroBatchProcessor,
    StreamConfig,
    StreamingSource,
    WindowedAggregator,
    StreamingStats
)
from .state import StateManager, StreamingContext
from .monitor import StreamMonitor, AlertRule, HealthChecker
from .sources import (
    SimulatedDataSource,
    create_source
)
from .spark_micro_batch import (
    SparkMicroBatchProcessor,
    SparkStreamConfig,
    create_spark_file_stream
)

__all__ = [
    'MicroBatchProcessor',
    'StreamConfig',
    'StreamingSource',
    'WindowedAggregator',
    'StateManager',
    'StreamingContext',
    'StreamMonitor',
    'StreamingStats',
    'AlertRule',
    'HealthChecker',
    'SimulatedDataSource',
    'create_source',
    'SparkMicroBatchProcessor',
    'SparkStreamConfig',
    'create_spark_file_stream'
]