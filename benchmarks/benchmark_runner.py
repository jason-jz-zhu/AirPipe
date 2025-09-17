#!/usr/bin/env python
"""Main benchmark runner for AirPipe framework"""

import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

import argparse
import time
import pandas as pd
from typing import Dict, List, Optional
import json
from datetime import datetime

from benchmarks.data_generators import DataGenerator
from benchmarks.monitoring import ResourceMonitor, MetricsCollector
from benchmarks.scenarios import SingleTaskBenchmark, ParallelBenchmark, StreamingBenchmark


class BenchmarkRunner:
    """Main benchmark orchestrator"""

    def __init__(self, output_dir: str = "benchmarks/results"):
        """Initialize benchmark runner

        Args:
            output_dir: Directory to store results
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

        self.data_generator = DataGenerator()
        self.metrics_collector = MetricsCollector()
        self.single_task = SingleTaskBenchmark()
        self.parallel = ParallelBenchmark()
        self.streaming = StreamingBenchmark()

        # Test configurations
        self.dataset_sizes = ["1MB", "10MB", "100MB", "1GB", "5GB", "10GB"]
        self.data_dir = "benchmarks/data"
        os.makedirs(self.data_dir, exist_ok=True)

    def prepare_test_data(self, sizes: Optional[List[str]] = None) -> Dict[str, str]:
        """Generate test data files

        Args:
            sizes: List of dataset sizes to generate

        Returns:
            Mapping of size to file path
        """
        sizes = sizes or self.dataset_sizes
        data_files = {}

        print("\n" + "=" * 60)
        print("PREPARING TEST DATA")
        print("=" * 60)

        for size in sizes:
            print(f"\nGenerating {size} dataset...")

            # Generate CSV for main testing
            csv_path = os.path.join(self.data_dir, f"test_{size}.csv")
            if not os.path.exists(csv_path) or os.path.getsize(csv_path) < 1000:
                self.data_generator.generate_csv(size, csv_path)

            # Also generate Parquet for format comparison
            parquet_path = os.path.join(self.data_dir, f"test_{size}.parquet")
            if not os.path.exists(parquet_path):
                self.data_generator.generate_parquet(size, parquet_path)

            data_files[size] = csv_path

        return data_files

    def run_single_task_benchmarks(self, data_files: Dict[str, str]) -> List[Dict]:
        """Run single task benchmarks

        Args:
            data_files: Mapping of size to file path

        Returns:
            List of benchmark results
        """
        results = []
        print("\n" + "=" * 60)
        print("RUNNING SINGLE TASK BENCHMARKS")
        print("=" * 60)

        for size, data_path in data_files.items():
            if not os.path.exists(data_path):
                print(f"Skipping {size} - file not found")
                continue

            print(f"\nTesting {size}...")
            monitor = ResourceMonitor()

            # Start monitoring
            monitor.start()
            self.metrics_collector.start_test("single_task_etl", size, "single")

            # Run benchmark
            try:
                metrics = self.single_task.run_etl_benchmark(data_path, size)
                success = True
            except Exception as e:
                print(f"  Error: {e}")
                metrics = {"error": str(e)}
                success = False

            # Stop monitoring
            resource_metrics = monitor.stop()

            # Collect results
            self.metrics_collector.end_test(
                resource_metrics,
                success=success,
                data_size_bytes=os.path.getsize(data_path),
                **metrics
            )

            # Print summary
            if resource_metrics:
                print(f"  Duration: {metrics.get('duration_seconds', 0):.2f}s")
                print(f"  Peak Memory: {resource_metrics['memory']['peak_mb']:.2f} MB")
                if 'io' in resource_metrics:
                    print(f"  I/O Read: {resource_metrics['io']['total_read_mb']:.2f} MB")

            results.append({
                "size": size,
                "mode": "single",
                "duration": metrics.get("duration_seconds", 0),
                "peak_memory_mb": resource_metrics.get("memory", {}).get("peak_mb", 0),
                "success": success
            })

        return results

    def run_parallel_benchmarks(self, data_files: Dict[str, str],
                              parallel_counts: List[int] = [10, 100]) -> List[Dict]:
        """Run parallel execution benchmarks

        Args:
            data_files: Mapping of size to file path
            parallel_counts: List of parallel task counts to test

        Returns:
            List of benchmark results
        """
        results = []
        print("\n" + "=" * 60)
        print("RUNNING PARALLEL EXECUTION BENCHMARKS")
        print("=" * 60)

        for size, data_path in data_files.items():
            if not os.path.exists(data_path):
                continue

            for parallel_count in parallel_counts:
                print(f"\nTesting {size} with {parallel_count} parallel tasks...")

                # Skip 100 parallel tasks for very large datasets (memory constraints)
                if parallel_count == 100 and size in ["5GB", "10GB"]:
                    print(f"  Skipping {parallel_count} parallel tasks for {size} (memory constraints)")
                    results.append({
                        "size": size,
                        "mode": f"{parallel_count}_parallel",
                        "duration": 0,
                        "peak_memory_mb": 0,
                        "success": False,
                        "skipped": True,
                        "reason": "memory_constraints"
                    })
                    continue

                monitor = ResourceMonitor()

                # Start monitoring
                monitor.start()
                self.metrics_collector.start_test(f"parallel_{parallel_count}", size, f"{parallel_count}_parallel")

                # Run benchmark
                try:
                    metrics = self.parallel.run_parallel_etl(data_path, size, parallel_count)
                    success = True
                except Exception as e:
                    print(f"  Error: {e}")
                    metrics = {"error": str(e)}
                    success = False

                # Stop monitoring
                resource_metrics = monitor.stop()

                # Collect results
                self.metrics_collector.end_test(
                    resource_metrics,
                    success=success,
                    parallel_count=parallel_count,
                    data_size_bytes=os.path.getsize(data_path),
                    **metrics
                )

                # Print summary
                if resource_metrics:
                    print(f"  Duration: {metrics.get('duration_seconds', 0):.2f}s")
                    print(f"  Peak Memory: {resource_metrics['memory']['peak_mb']:.2f} MB")

                results.append({
                    "size": size,
                    "mode": f"{parallel_count}_parallel",
                    "duration": metrics.get("duration_seconds", 0),
                    "peak_memory_mb": resource_metrics.get("memory", {}).get("peak_mb", 0),
                    "success": success
                })

        return results

    def run_streaming_benchmarks(self, data_files: Dict[str, str]) -> List[Dict]:
        """Run streaming mode benchmarks

        Args:
            data_files: Mapping of size to file path

        Returns:
            List of benchmark results
        """
        results = []
        print("\n" + "=" * 60)
        print("RUNNING STREAMING MODE BENCHMARKS")
        print("=" * 60)

        for size, data_path in data_files.items():
            if not os.path.exists(data_path):
                continue

            print(f"\nTesting streaming for {size}...")
            monitor = ResourceMonitor()

            # Start monitoring
            monitor.start()
            self.metrics_collector.start_test("streaming", size, "streaming")

            # Run benchmark
            try:
                metrics = self.streaming.run_streaming_etl(data_path, size)
                success = True
            except Exception as e:
                print(f"  Error: {e}")
                metrics = {"error": str(e)}
                success = False

            # Stop monitoring
            resource_metrics = monitor.stop()

            # Collect results
            self.metrics_collector.end_test(
                resource_metrics,
                success=success,
                data_size_bytes=os.path.getsize(data_path),
                **metrics
            )

            # Print summary
            if resource_metrics:
                print(f"  Duration: {metrics.get('duration_seconds', 0):.2f}s")
                print(f"  Peak Memory: {resource_metrics['memory']['peak_mb']:.2f} MB")
                print(f"  Throughput: {metrics.get('throughput_records_per_second', 0):.0f} records/s")

            results.append({
                "size": size,
                "mode": "streaming",
                "duration": metrics.get("duration_seconds", 0),
                "peak_memory_mb": resource_metrics.get("memory", {}).get("peak_mb", 0),
                "throughput_rps": metrics.get("throughput_records_per_second", 0),
                "success": success
            })

        return results

    def generate_results_table(self, all_results: List[Dict]) -> pd.DataFrame:
        """Generate formatted results table

        Args:
            all_results: List of all benchmark results

        Returns:
            Formatted DataFrame
        """
        # Create DataFrame
        df = pd.DataFrame(all_results)

        # Pivot for better visualization
        pivot_metrics = []

        for size in self.dataset_sizes:
            size_data = df[df['size'] == size]

            row = {"Dataset Size": size}

            # Single task
            single = size_data[size_data['mode'] == 'single']
            if not single.empty:
                row['Single Task'] = f"{single.iloc[0]['duration']:.1f}s / {single.iloc[0]['peak_memory_mb']:.0f}MB"
            else:
                row['Single Task'] = "N/A"

            # 10 Parallel
            parallel_10 = size_data[size_data['mode'] == '10_parallel']
            if not parallel_10.empty:
                row['10 Parallel'] = f"{parallel_10.iloc[0]['duration']:.1f}s / {parallel_10.iloc[0]['peak_memory_mb']:.0f}MB"
            else:
                row['10 Parallel'] = "N/A"

            # 100 Parallel
            parallel_100 = size_data[size_data['mode'] == '100_parallel']
            if not parallel_100.empty:
                if parallel_100.iloc[0].get('skipped', False):
                    row['100 Parallel'] = "OOM"
                else:
                    row['100 Parallel'] = f"{parallel_100.iloc[0]['duration']:.1f}s / {parallel_100.iloc[0]['peak_memory_mb']:.0f}MB"
            else:
                row['100 Parallel'] = "N/A"

            # Streaming
            streaming = size_data[size_data['mode'] == 'streaming']
            if not streaming.empty:
                row['Streaming Mode'] = f"{streaming.iloc[0]['duration']:.1f}s / {streaming.iloc[0]['peak_memory_mb']:.0f}MB"
            else:
                row['Streaming Mode'] = "N/A"

            pivot_metrics.append(row)

        return pd.DataFrame(pivot_metrics)

    def run_all_benchmarks(self, sizes: Optional[List[str]] = None):
        """Run all benchmarks and generate report

        Args:
            sizes: Optional list of dataset sizes to test
        """
        sizes = sizes or self.dataset_sizes
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Prepare test data
        data_files = self.prepare_test_data(sizes)

        all_results = []

        # Run single task benchmarks
        single_results = self.run_single_task_benchmarks(data_files)
        all_results.extend(single_results)

        # Run parallel benchmarks
        parallel_results = self.run_parallel_benchmarks(data_files, [10, 100])
        all_results.extend(parallel_results)

        # Run streaming benchmarks
        streaming_results = self.run_streaming_benchmarks(data_files)
        all_results.extend(streaming_results)

        # Generate results table
        results_table = self.generate_results_table(all_results)

        # Print final results
        print("\n" + "=" * 80)
        print("BENCHMARK RESULTS SUMMARY")
        print("=" * 80)
        print("\nFormat: Duration (seconds) / Peak Memory (MB)")
        print("-" * 80)
        print(results_table.to_string(index=False))
        print("-" * 80)

        # Save results
        results_file = os.path.join(self.output_dir, f"benchmark_results_{timestamp}.json")
        self.metrics_collector.save_results(results_file)

        table_file = os.path.join(self.output_dir, f"results_table_{timestamp}.csv")
        results_table.to_csv(table_file, index=False)

        print(f"\nResults saved to:")
        print(f"  - {results_file}")
        print(f"  - {table_file}")

        # Print observations
        self.print_observations(all_results)

    def print_observations(self, results: List[Dict]):
        """Print key observations from benchmark results"""
        print("\n" + "=" * 80)
        print("KEY OBSERVATIONS")
        print("=" * 80)

        df = pd.DataFrame(results)

        # Memory scaling
        single_df = df[df['mode'] == 'single']
        if not single_df.empty:
            print("\n1. Memory Scaling (Single Task):")
            for _, row in single_df.iterrows():
                if row['success']:
                    size_mb = float(row['size'].replace('GB', '000').replace('MB', ''))
                    memory_ratio = row['peak_memory_mb'] / size_mb if size_mb > 0 else 0
                    print(f"   - {row['size']}: {memory_ratio:.2f}x dataset size")

        # Parallel overhead
        print("\n2. Parallel Execution Overhead:")
        for size in ["1MB", "10MB", "100MB", "1GB"]:
            size_df = df[df['size'] == size]
            single = size_df[size_df['mode'] == 'single']
            parallel_10 = size_df[size_df['mode'] == '10_parallel']

            if not single.empty and not parallel_10.empty:
                memory_overhead = parallel_10.iloc[0]['peak_memory_mb'] / single.iloc[0]['peak_memory_mb']
                time_improvement = single.iloc[0]['duration'] / parallel_10.iloc[0]['duration'] if parallel_10.iloc[0]['duration'] > 0 else 0
                print(f"   - {size}: {memory_overhead:.1f}x memory, {time_improvement:.1f}x speedup")

        # Streaming advantages
        print("\n3. Streaming Mode Advantages:")
        for size in ["1GB", "5GB", "10GB"]:
            size_df = df[df['size'] == size]
            single = size_df[size_df['mode'] == 'single']
            streaming = size_df[size_df['mode'] == 'streaming']

            if not single.empty and not streaming.empty:
                memory_savings = (1 - streaming.iloc[0]['peak_memory_mb'] / single.iloc[0]['peak_memory_mb']) * 100
                print(f"   - {size}: {memory_savings:.0f}% memory savings")

        print("\n" + "=" * 80)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Run AirPipe benchmarks")
    parser.add_argument("--sizes", nargs="+", help="Dataset sizes to test",
                       default=["1MB", "10MB", "100MB", "1GB"])
    parser.add_argument("--output", help="Output directory", default="benchmarks/results")

    args = parser.parse_args()

    runner = BenchmarkRunner(output_dir=args.output)
    runner.run_all_benchmarks(sizes=args.sizes)


if __name__ == "__main__":
    main()