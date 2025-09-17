"""Parallel execution benchmark scenarios"""

import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

import pandas as pd
import time
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor
from airpipe.core.task import TaskPipeline


class ParallelBenchmark:
    """Benchmark parallel task execution"""

    def __init__(self):
        """Initialize parallel benchmark"""
        self.pipeline = None

    def run_parallel_etl(self, data_path: str, dataset_size: str,
                        parallel_count: int) -> Dict:
        """Run ETL with multiple parallel tasks

        Args:
            data_path: Path to input data file
            dataset_size: Size string for logging
            parallel_count: Number of parallel tasks (10 or 100)

        Returns:
            Execution metrics
        """
        pipeline = TaskPipeline(f"parallel_{parallel_count}_{dataset_size}")

        @pipeline.task(produces="raw_data")
        def extract():
            """Extract data from file"""
            file_ext = os.path.splitext(data_path)[1].lower()

            if file_ext == '.csv':
                df = pd.read_csv(data_path)
            elif file_ext == '.json':
                df = pd.read_json(data_path)
            elif file_ext == '.parquet':
                df = pd.read_parquet(data_path)
            else:
                raise ValueError(f"Unsupported file type: {file_ext}")

            return pipeline.create_artifact(df, "raw_data")

        # Create multiple parallel transformation tasks
        # We'll create these dynamically with unique names
        def create_transform_task(task_id):
            @pipeline.task(
                depends_on=["extract"],
                consumes="raw_data",
                produces=f"transformed_{task_id}"
            )
            def transform_task():
                """Transform a partition of data"""
                raw_data = pipeline.get_artifact("raw_data")
                df = raw_data.as_dataframe()

                # Split data into partitions
                partition_size = max(1, len(df) // parallel_count)
                start_idx = task_id * partition_size
                end_idx = start_idx + partition_size if task_id < parallel_count - 1 else len(df)
                partition = df.iloc[start_idx:end_idx].copy()

                # Apply transformations
                if 'price' in partition.columns:
                    partition['adjusted_price'] = partition['price'] * (1 + task_id * 0.01)

                if 'quantity' in partition.columns:
                    partition['adjusted_quantity'] = partition['quantity'] + task_id

                # Add processing metadata
                partition['processor_id'] = task_id
                partition['processing_time'] = pd.Timestamp.now()

                # Simulate some CPU-intensive work
                for col in partition.select_dtypes(include=['float64', 'int64']).columns[:3]:
                    partition[f'{col}_rolling_mean'] = partition[col].rolling(window=min(10, len(partition)), min_periods=1).mean()

                return pipeline.create_artifact(partition, f"transformed_{task_id}")

            # Set a unique name for the function
            transform_task.__name__ = f"transform_{task_id}"
            return transform_task

        # Create all transform tasks
        transform_tasks = []
        for i in range(parallel_count):
            transform_tasks.append(create_transform_task(i))

        @pipeline.task(
            depends_on=[f"transform_{i}" for i in range(parallel_count)],
            consumes=[f"transformed_{i}" for i in range(parallel_count)],
            produces="merged_data"
        )
        def merge_results():
            """Merge all parallel results"""
            partitions = []
            for i in range(parallel_count):
                artifact = pipeline.get_artifact(f"transformed_{i}")
                if artifact:
                    partitions.append(artifact.as_dataframe())

            merged_df = pd.concat(partitions, ignore_index=True)
            return pipeline.create_artifact(merged_df, "merged_data")

        @pipeline.task(
            depends_on=["merge_results"],
            consumes="merged_data"
        )
        def final_output():
            """Write final output"""
            merged_data = pipeline.get_artifact("merged_data")
            df = merged_data.as_dataframe()

            output_dir = f"benchmarks/output/parallel_{parallel_count}_{dataset_size}"
            os.makedirs(output_dir, exist_ok=True)
            df.to_parquet(f"{output_dir}/output.parquet", index=False)

            return pipeline.create_artifact({"rows_written": len(df)}, "output_result")

        # Execute pipeline with parallel execution
        start_time = time.time()
        results = pipeline.execute(parallel=True, max_workers=min(parallel_count, 10))
        end_time = time.time()

        metrics = {
            "duration_seconds": end_time - start_time,
            "parallel_tasks": parallel_count,
            "success": results is not None,
            "total_tasks": len(pipeline.tasks)
        }

        return metrics

    def run_independent_parallel_tasks(self, data_path: str, dataset_size: str,
                                      parallel_count: int) -> Dict:
        """Run completely independent parallel tasks

        Args:
            data_path: Path to input data file
            dataset_size: Size string for logging
            parallel_count: Number of parallel tasks

        Returns:
            Execution metrics
        """
        pipeline = TaskPipeline(f"independent_{parallel_count}_{dataset_size}")

        # Load data once
        file_ext = os.path.splitext(data_path)[1].lower()
        if file_ext == '.csv':
            base_df = pd.read_csv(data_path)
        elif file_ext == '.parquet':
            base_df = pd.read_parquet(data_path)
        else:
            base_df = pd.read_json(data_path)

        # Create independent tasks that don't depend on each other
        def create_independent_task(task_id, df):
            @pipeline.task(produces=f"result_{task_id}")
            def independent_task():
                """Independent processing task"""
                # Each task works on a copy of the data
                df_copy = df.copy()

                # Different processing for each task
                if task_id % 3 == 0:
                    # Aggregation task
                    if 'category' in df_copy.columns:
                        result = df_copy.groupby('category').agg({
                            col: 'mean' for col in df_copy.select_dtypes(include=['float64', 'int64']).columns
                        })
                    else:
                        result = df_copy.describe()
                elif task_id % 3 == 1:
                    # Filtering task
                    numeric_cols = df_copy.select_dtypes(include=['float64', 'int64']).columns
                    if len(numeric_cols) > 0:
                        col = numeric_cols[0]
                        result = df_copy[df_copy[col] > df_copy[col].median()]
                    else:
                        result = df_copy.head(len(df_copy) // 2)
                else:
                    # Transformation task
                    for col in df_copy.select_dtypes(include=['float64', 'int64']).columns[:3]:
                        df_copy[f'{col}_transformed_{task_id}'] = df_copy[col] * (1 + task_id * 0.1)
                    result = df_copy

                return pipeline.create_artifact(
                    {"task_id": task_id, "rows_processed": len(result)},
                    f"result_{task_id}"
                )

            # Set unique name
            independent_task.__name__ = f"independent_{task_id}"
            return independent_task

        # Create all independent tasks
        for i in range(parallel_count):
            create_independent_task(i, base_df)

        # Execute all independent tasks in parallel
        start_time = time.time()
        results = pipeline.execute(parallel=True, max_workers=min(parallel_count, 20))
        end_time = time.time()

        return {
            "duration_seconds": end_time - start_time,
            "parallel_tasks": parallel_count,
            "pipeline_type": "independent_parallel",
            "success": results is not None
        }

    def run_resource_contention_test(self, data_path: str, dataset_size: str,
                                    parallel_count: int) -> Dict:
        """Test resource contention with shared resources

        Args:
            data_path: Path to input data file
            dataset_size: Size string for logging
            parallel_count: Number of parallel tasks

        Returns:
            Execution metrics
        """
        pipeline = TaskPipeline(f"contention_{parallel_count}_{dataset_size}")

        # Shared resource (simulating database or file access)
        shared_output_file = f"benchmarks/output/shared_{dataset_size}.csv"

        @pipeline.task(produces="raw_data")
        def load_data():
            """Load data"""
            df = pd.read_csv(data_path) if data_path.endswith('.csv') else pd.read_parquet(data_path)
            return pipeline.create_artifact(df, "raw_data")

        # Create tasks that compete for shared resources
        def create_competing_task(task_id):
            @pipeline.task(
                depends_on=["load_data"],
                consumes="raw_data",
                produces=f"competed_{task_id}"
            )
            def competing_task():
                """Task competing for shared resource"""
                raw_data = pipeline.get_artifact("raw_data")
                df = raw_data.as_dataframe()

                # Process partition
                partition_size = max(1, len(df) // parallel_count)
                start = task_id * partition_size
                end = start + partition_size if task_id < parallel_count - 1 else len(df)
                partition = df.iloc[start:end]

                # Simulate resource contention (file I/O)
                import threading

                lock = threading.Lock()
                with lock:
                    # Append to shared file (simulating contention)
                    os.makedirs(os.path.dirname(shared_output_file), exist_ok=True)
                    partition.to_csv(
                        shared_output_file,
                        mode='a',
                        header=not os.path.exists(shared_output_file),
                        index=False
                    )

                return pipeline.create_artifact(
                    {"task_id": task_id, "rows_written": len(partition)},
                    f"competed_{task_id}"
                )

            # Set unique name
            competing_task.__name__ = f"competing_{task_id}"
            return competing_task

        # Create all competing tasks
        for i in range(parallel_count):
            create_competing_task(i)

        # Execute with parallel execution
        start_time = time.time()
        try:
            # Clean up any existing shared file
            if os.path.exists(shared_output_file):
                os.remove(shared_output_file)

            results = pipeline.execute(parallel=True, max_workers=min(parallel_count, 10))
            success = True
        except Exception as e:
            print(f"Contention test error: {e}")
            success = False
            results = None
        end_time = time.time()

        # Clean up
        if os.path.exists(shared_output_file):
            os.remove(shared_output_file)

        return {
            "duration_seconds": end_time - start_time,
            "parallel_tasks": parallel_count,
            "pipeline_type": "resource_contention",
            "success": success,
            "had_contention": True
        }