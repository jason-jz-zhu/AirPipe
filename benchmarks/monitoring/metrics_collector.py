"""Metrics collection and aggregation for benchmarks"""

import time
import json
import os
from typing import Dict, List, Optional, Any
from datetime import datetime
import pandas as pd


class MetricsCollector:
    """Collect and aggregate benchmark metrics"""

    def __init__(self):
        """Initialize metrics collector"""
        self.results = []
        self.current_test = None

    def start_test(self, test_name: str, dataset_size: str,
                   execution_mode: str, **kwargs):
        """Start tracking a new test"""
        self.current_test = {
            "test_name": test_name,
            "dataset_size": dataset_size,
            "execution_mode": execution_mode,
            "start_time": time.time(),
            "timestamp": datetime.now().isoformat(),
            **kwargs
        }

    def end_test(self, resource_metrics: Dict, **additional_metrics):
        """End current test and store results"""
        if self.current_test:
            self.current_test["end_time"] = time.time()
            self.current_test["duration"] = self.current_test["end_time"] - self.current_test["start_time"]
            self.current_test["resource_metrics"] = resource_metrics
            self.current_test.update(additional_metrics)

            # Calculate throughput if data size is available
            if "data_size_bytes" in self.current_test:
                throughput_mbps = (self.current_test["data_size_bytes"] / (1024 * 1024)) / self.current_test["duration"]
                self.current_test["throughput_mbps"] = throughput_mbps

            self.results.append(self.current_test)
            self.current_test = None

    def add_metric(self, key: str, value: Any):
        """Add a metric to current test"""
        if self.current_test:
            self.current_test[key] = value

    def get_results_dataframe(self) -> pd.DataFrame:
        """Get results as pandas DataFrame"""
        if not self.results:
            return pd.DataFrame()

        # Flatten nested metrics for DataFrame
        flattened = []
        for result in self.results:
            flat_result = {
                "test_name": result["test_name"],
                "dataset_size": result["dataset_size"],
                "execution_mode": result["execution_mode"],
                "duration_seconds": result.get("duration", 0),
                "timestamp": result["timestamp"]
            }

            # Add resource metrics if available
            if "resource_metrics" in result:
                metrics = result["resource_metrics"]
                if "memory" in metrics:
                    flat_result["peak_memory_mb"] = metrics["memory"]["peak_mb"]
                    flat_result["mean_memory_mb"] = metrics["memory"]["mean_mb"]
                if "cpu" in metrics:
                    flat_result["peak_cpu_percent"] = metrics["cpu"]["peak_percent"]
                    flat_result["mean_cpu_percent"] = metrics["cpu"]["mean_percent"]
                if "io" in metrics:
                    flat_result["total_io_read_mb"] = metrics["io"]["total_read_mb"]
                    flat_result["total_io_write_mb"] = metrics["io"]["total_write_mb"]

            # Add throughput if available
            if "throughput_mbps" in result:
                flat_result["throughput_mbps"] = result["throughput_mbps"]

            # Add any additional metrics
            for key, value in result.items():
                if key not in ["resource_metrics", "start_time", "end_time"]:
                    if key not in flat_result:
                        flat_result[key] = value

            flattened.append(flat_result)

        return pd.DataFrame(flattened)

    def save_results(self, filepath: str):
        """Save results to JSON file"""
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)

    def load_results(self, filepath: str):
        """Load results from JSON file"""
        with open(filepath, 'r') as f:
            self.results = json.load(f)

    def generate_summary_table(self) -> pd.DataFrame:
        """Generate summary table for benchmark results"""
        df = self.get_results_dataframe()
        if df.empty:
            return df

        # Pivot table for better visualization
        summary = df.pivot_table(
            index="dataset_size",
            columns="execution_mode",
            values=["duration_seconds", "peak_memory_mb", "throughput_mbps"],
            aggfunc="mean"
        ).round(2)

        return summary

    def print_summary(self):
        """Print formatted summary of results"""
        df = self.get_results_dataframe()
        if df.empty:
            print("No results to display")
            return

        print("\n" + "=" * 80)
        print("BENCHMARK RESULTS SUMMARY")
        print("=" * 80)

        # Group by dataset size and execution mode
        for size in df["dataset_size"].unique():
            print(f"\nDataset Size: {size}")
            print("-" * 40)

            size_df = df[df["dataset_size"] == size]

            for mode in size_df["execution_mode"].unique():
                mode_df = size_df[size_df["execution_mode"] == mode]

                if not mode_df.empty:
                    row = mode_df.iloc[0]
                    print(f"\n  Execution Mode: {mode}")
                    print(f"    Duration: {row.get('duration_seconds', 0):.2f} seconds")
                    print(f"    Peak Memory: {row.get('peak_memory_mb', 0):.2f} MB")
                    print(f"    Throughput: {row.get('throughput_mbps', 0):.2f} MB/s")
                    if 'peak_cpu_percent' in row:
                        print(f"    Peak CPU: {row.get('peak_cpu_percent', 0):.1f}%")

        print("\n" + "=" * 80)