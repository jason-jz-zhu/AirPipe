#!/usr/bin/env python
"""Quick test of benchmark components"""

import sys
import os
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from benchmarks.data_generators import DataGenerator
from benchmarks.monitoring import ResourceMonitor, MetricsCollector
from benchmarks.scenarios import SingleTaskBenchmark
import pandas as pd

def test_benchmarks():
    """Test benchmark components"""

    print("Testing data generation...")
    generator = DataGenerator()
    df = generator.generate_dataframe("1MB")
    print(f"Generated DataFrame with {len(df)} rows, {len(df.columns)} columns")

    # Save test data
    test_file = "benchmarks/data/test_small.csv"
    os.makedirs("benchmarks/data", exist_ok=True)
    df.head(100).to_csv(test_file, index=False)
    print(f"Saved test data to {test_file}")

    print("\nTesting resource monitor...")
    monitor = ResourceMonitor()
    monitor.start()

    # Do some work
    df2 = pd.read_csv(test_file)
    df2['new_col'] = df2.select_dtypes(include=['float64', 'int64']).sum(axis=1)

    metrics = monitor.stop()
    print(f"Resource metrics: {metrics}")

    print("\nTesting single task benchmark...")
    benchmark = SingleTaskBenchmark()

    try:
        result = benchmark.run_etl_benchmark(test_file, "test")
        print(f"Benchmark result: {result}")
    except Exception as e:
        print(f"Benchmark error: {e}")

    print("\nTest complete!")

if __name__ == "__main__":
    test_benchmarks()