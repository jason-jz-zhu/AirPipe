"""Benchmark scenarios"""

from .single_task_scenario import SingleTaskBenchmark
from .parallel_scenario import ParallelBenchmark
from .streaming_scenario import StreamingBenchmark

__all__ = ["SingleTaskBenchmark", "ParallelBenchmark", "StreamingBenchmark"]