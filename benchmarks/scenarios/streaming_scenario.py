"""Streaming mode benchmark scenarios"""

import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

import pandas as pd
import time
import queue
import threading
from typing import Dict, Generator, List
from airpipe.core.task import TaskPipeline
from airpipe.core.streaming.micro_batch import MicroBatchProcessor, StreamConfig, StreamingStats


class StreamingBenchmark:
    """Benchmark streaming mode execution"""

    def __init__(self):
        """Initialize streaming benchmark"""
        self.pipeline = None

    def create_data_stream(self, data_path: str, batch_size: int = 1000) -> Generator:
        """Create a streaming data generator from file

        Args:
            data_path: Path to data file
            batch_size: Size of each batch

        Yields:
            DataFrame batches
        """
        # Read the full dataset
        file_ext = os.path.splitext(data_path)[1].lower()
        if file_ext == '.csv':
            df = pd.read_csv(data_path)
        elif file_ext == '.parquet':
            df = pd.read_parquet(data_path)
        else:
            df = pd.read_json(data_path)

        # Stream in batches
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            yield batch
            time.sleep(0.01)  # Simulate streaming delay

    def run_streaming_etl(self, data_path: str, dataset_size: str) -> Dict:
        """Run ETL pipeline in streaming mode

        Args:
            data_path: Path to input data file
            dataset_size: Size string for logging

        Returns:
            Execution metrics
        """
        # Create pipeline for batch processing
        pipeline = TaskPipeline(f"streaming_{dataset_size}")

        @pipeline.task(produces="batch_data")
        def process_batch(batch_df):
            """Process a batch of streaming data"""
            return pipeline.create_artifact(batch_df, "batch_data")

        @pipeline.task(depends_on=["process_batch"], consumes="batch_data", produces="transformed_batch")
        def transform_batch():
            """Transform streaming batch"""
            batch_data = pipeline.get_artifact("batch_data")
            df = batch_data.as_dataframe()

            # Apply transformations
            if 'price' in df.columns:
                df['price_adjusted'] = df['price'] * 1.1

            if 'quantity' in df.columns and 'price' in df.columns:
                df['total_value'] = df['quantity'] * df['price']

            # Add streaming metadata
            df['batch_timestamp'] = pd.Timestamp.now()
            df['stream_sequence'] = 0  # Will be updated per batch

            return pipeline.create_artifact(df, "transformed_batch")

        @pipeline.task(depends_on=["transform_batch"], consumes="transformed_batch", produces="aggregated")
        def aggregate_batch():
            """Perform streaming aggregations"""
            transformed_batch = pipeline.get_artifact("transformed_batch")
            df = transformed_batch.as_dataframe()

            # Compute batch statistics
            stats = {
                "batch_size": len(df),
                "timestamp": pd.Timestamp.now()
            }

            if 'price' in df.columns:
                stats["avg_price"] = df['price'].mean()
                stats["max_price"] = df['price'].max()

            if 'total_value' in df.columns:
                stats["total_value"] = df['total_value'].sum()

            return pipeline.create_artifact(stats, "aggregated")

        # Configure streaming processor
        config = StreamConfig(
            batch_size=1000,
            batch_interval=5.0,
            backpressure_threshold=10000,
            enable_monitoring=True
        )

        # Create processor
        processor = MicroBatchProcessor(pipeline, config)

        # Prepare data stream
        stream_generator = self.create_data_stream(data_path, batch_size=1000)

        # Process stream
        start_time = time.time()
        batch_count = 0
        total_records = 0

        try:
            for batch in stream_generator:
                # Process batch through pipeline
                pipeline.artifacts["batch_data"] = pipeline.create_artifact(batch, "batch_data")
                results = pipeline.execute(parallel=False)
                batch_count += 1
                total_records += len(batch)

                # Simulate continuous streaming
                if batch_count >= 100:  # Limit for benchmark
                    break

        except Exception as e:
            print(f"Streaming error: {e}")

        end_time = time.time()

        metrics = {
            "duration_seconds": end_time - start_time,
            "batches_processed": batch_count,
            "records_processed": total_records,
            "throughput_records_per_second": total_records / (end_time - start_time) if end_time > start_time else 0,
            "avg_batch_time": (end_time - start_time) / batch_count if batch_count > 0 else 0,
            "pipeline_type": "streaming_etl"
        }

        return metrics

    def run_windowed_aggregation(self, data_path: str, dataset_size: str) -> Dict:
        """Run streaming with windowed aggregations

        Args:
            data_path: Path to input data file
            dataset_size: Size string for logging

        Returns:
            Execution metrics
        """
        # Create pipeline
        pipeline = TaskPipeline(f"windowed_{dataset_size}")

        # Window state
        window_state = {
            "window_data": [],
            "window_size": 10000,  # Records
            "slide_interval": 5000  # Records
        }

        @pipeline.task(produces="batch_data")
        def ingest_batch(batch_df):
            """Ingest streaming batch"""
            return pipeline.create_artifact(batch_df, "batch_data")

        @pipeline.task(depends_on=["ingest_batch"], consumes="batch_data", produces="windowed")
        def apply_window():
            """Apply windowing logic"""
            batch_data = pipeline.get_artifact("batch_data")
            df = batch_data.as_dataframe()

            # Add to window
            window_state["window_data"].append(df)

            # Check if window is full
            total_records = sum(len(d) for d in window_state["window_data"])

            if total_records >= window_state["window_size"]:
                # Process window
                window_df = pd.concat(window_state["window_data"], ignore_index=True)

                # Keep only slide interval for next window
                records_to_keep = window_state["slide_interval"]
                if len(window_df) > records_to_keep:
                    window_df = window_df.iloc[-records_to_keep:]
                    window_state["window_data"] = [window_df]
                else:
                    window_state["window_data"] = [window_df]

                return pipeline.create_artifact(window_df, "windowed")

            return pipeline.create_artifact(pd.DataFrame(), "windowed")

        @pipeline.task(depends_on=["apply_window"], consumes="windowed", produces="window_stats")
        def compute_window_stats():
            """Compute statistics over window"""
            windowed = pipeline.get_artifact("windowed")
            df = windowed.as_dataframe()

            if df.empty:
                return pipeline.create_artifact({}, "window_stats")

            stats = {
                "window_size": len(df),
                "timestamp": pd.Timestamp.now()
            }

            # Compute statistics
            numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
            for col in numeric_cols[:5]:  # Limit columns
                if col in df.columns:
                    stats[f"{col}_mean"] = df[col].mean()
                    stats[f"{col}_std"] = df[col].std()
                    stats[f"{col}_min"] = df[col].min()
                    stats[f"{col}_max"] = df[col].max()

            return pipeline.create_artifact(stats, "window_stats")

        # Process stream with windows
        stream_generator = self.create_data_stream(data_path, batch_size=500)

        start_time = time.time()
        windows_processed = 0
        total_records = 0

        for batch in stream_generator:
            pipeline.artifacts["batch_data"] = pipeline.create_artifact(batch, "batch_data")
            results = pipeline.execute(parallel=False)

            # Check if window was processed
            window_stats = pipeline.get_artifact("window_stats")
            if window_stats and window_stats.data:
                windows_processed += 1

            total_records += len(batch)

            # Limit for benchmark
            if windows_processed >= 10 or total_records >= 50000:
                break

        end_time = time.time()

        return {
            "duration_seconds": end_time - start_time,
            "windows_processed": windows_processed,
            "records_processed": total_records,
            "throughput_records_per_second": total_records / (end_time - start_time) if end_time > start_time else 0,
            "pipeline_type": "windowed_aggregation"
        }

    def run_stateful_streaming(self, data_path: str, dataset_size: str) -> Dict:
        """Run streaming with stateful processing

        Args:
            data_path: Path to input data file
            dataset_size: Size string for logging

        Returns:
            Execution metrics
        """
        pipeline = TaskPipeline(f"stateful_{dataset_size}")

        # Maintain state across batches
        processing_state = {
            "running_total": 0,
            "record_count": 0,
            "category_counts": {},
            "anomalies_detected": 0
        }

        @pipeline.task(produces="batch")
        def receive_batch(batch_df):
            """Receive streaming batch"""
            return pipeline.create_artifact(batch_df, "batch")

        @pipeline.task(depends_on=["receive_batch"], consumes="batch", produces="stateful_result")
        def stateful_processing():
            """Process batch with state"""
            batch = pipeline.get_artifact("batch")
            df = batch.as_dataframe()

            # Update running statistics
            processing_state["record_count"] += len(df)

            if 'price' in df.columns:
                processing_state["running_total"] += df['price'].sum()

                # Detect anomalies based on running average
                running_avg = processing_state["running_total"] / processing_state["record_count"]
                anomalies = df[df['price'] > running_avg * 2]
                processing_state["anomalies_detected"] += len(anomalies)

            if 'category' in df.columns:
                category_counts = df['category'].value_counts().to_dict()
                for cat, count in category_counts.items():
                    processing_state["category_counts"][cat] = (
                        processing_state["category_counts"].get(cat, 0) + count
                    )

            # Return current state
            return pipeline.create_artifact(processing_state.copy(), "stateful_result")

        # Process stream
        stream_generator = self.create_data_stream(data_path, batch_size=2000)

        start_time = time.time()
        batches_processed = 0
        final_state = None

        for batch in stream_generator:
            pipeline.artifact_store._artifacts["batch"] = pipeline.create_artifact(batch, "batch")
            results = pipeline.execute(parallel=False)
            batches_processed += 1

            # Get latest state
            state_artifact = pipeline.get_artifact("stateful_result")
            if state_artifact:
                final_state = state_artifact.data

            # Limit for benchmark
            if batches_processed >= 50:
                break

        end_time = time.time()

        metrics = {
            "duration_seconds": end_time - start_time,
            "batches_processed": batches_processed,
            "final_record_count": final_state["record_count"] if final_state else 0,
            "anomalies_detected": final_state["anomalies_detected"] if final_state else 0,
            "unique_categories": len(final_state["category_counts"]) if final_state else 0,
            "pipeline_type": "stateful_streaming"
        }

        return metrics