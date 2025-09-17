"""Single task benchmark scenarios"""

import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

import pandas as pd
import time
from typing import Dict, Optional
from airpipe.core.task import TaskPipeline


class SingleTaskBenchmark:
    """Benchmark single task execution"""

    def __init__(self):
        """Initialize single task benchmark"""
        self.pipeline = None

    def run_etl_benchmark(self, data_path: str, dataset_size: str) -> Dict:
        """Run standard ETL pipeline benchmark

        Args:
            data_path: Path to input data file
            dataset_size: Size string for logging

        Returns:
            Execution metrics
        """
        # Create pipeline
        pipeline = TaskPipeline(f"single_etl_{dataset_size}")

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

        @pipeline.task(depends_on=["extract"], consumes="raw_data", produces="transformed_data")
        def transform():
            """Transform data - filtering, aggregation, enrichment"""
            raw_data = pipeline.get_artifact("raw_data")
            df = raw_data.as_dataframe()

            # Apply transformations
            # 1. Filter records
            if 'price' in df.columns:
                df = df[df['price'] > df['price'].median()].copy()

            # 2. Add calculated columns
            if 'quantity' in df.columns and 'price' in df.columns:
                df.loc[:, 'total_value'] = df['quantity'] * df['price']

            # 3. Aggregation
            if 'category' in df.columns and 'price' in df.columns:
                category_stats = df.groupby('category')['price'].agg(['mean', 'sum', 'count']).reset_index()
                category_stats.columns = ['category', 'price_mean', 'price_sum', 'price_count']
                df = df.merge(category_stats, on='category', how='left')

            # 4. Data enrichment
            df['processing_timestamp'] = pd.Timestamp.now()
            df['data_quality_score'] = (df.notna().sum(axis=1) / len(df.columns)) * 100

            return pipeline.create_artifact(df, "transformed_data")

        @pipeline.task(depends_on=["transform"], consumes="transformed_data")
        def load():
            """Load data to output"""
            transformed_data = pipeline.get_artifact("transformed_data")
            df = transformed_data.as_dataframe()

            # Simulate writing to different outputs
            output_dir = f"benchmarks/output/{dataset_size}"
            os.makedirs(output_dir, exist_ok=True)

            # Write to multiple formats to test I/O
            df.to_csv(f"{output_dir}/output.csv", index=False)
            df.to_parquet(f"{output_dir}/output.parquet", index=False)

            return pipeline.create_artifact({"rows_written": len(df)}, "load_result")

        # Execute pipeline
        start_time = time.time()
        results = pipeline.execute(parallel=False)
        end_time = time.time()

        # Get execution metrics
        metrics = {
            "duration_seconds": end_time - start_time,
            "task_count": len(pipeline.tasks),
            "artifacts_created": len(pipeline.artifacts),
            "success": results is not None
        }

        # Add task-level metrics
        task_stats = pipeline.get_task_statistics()
        metrics.update(task_stats)

        return metrics

    def run_transformation_benchmark(self, data_path: str, dataset_size: str) -> Dict:
        """Run complex transformation benchmark

        Args:
            data_path: Path to input data file
            dataset_size: Size string for logging

        Returns:
            Execution metrics
        """
        pipeline = TaskPipeline(f"single_transform_{dataset_size}")

        @pipeline.task(produces="raw_data")
        def extract():
            """Extract data"""
            df = pd.read_csv(data_path) if data_path.endswith('.csv') else pd.read_parquet(data_path)
            return pipeline.create_artifact(df, "raw_data")

        @pipeline.task(depends_on=["extract"], consumes="raw_data", produces="cleaned_data")
        def clean_data():
            """Data cleaning operations"""
            raw_data = pipeline.get_artifact("raw_data")
            df = raw_data.as_dataframe()

            # Remove duplicates
            df = df.drop_duplicates()

            # Handle missing values
            numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
            df[numeric_columns] = df[numeric_columns].fillna(df[numeric_columns].median())

            # Standardize text columns
            text_columns = df.select_dtypes(include=['object']).columns
            for col in text_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip().str.lower()

            return pipeline.create_artifact(df, "cleaned_data")

        @pipeline.task(depends_on=["clean_data"], consumes="cleaned_data", produces="features")
        def feature_engineering():
            """Create derived features"""
            cleaned_data = pipeline.get_artifact("cleaned_data")
            df = cleaned_data.as_dataframe()

            # Create time-based features if timestamp exists
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df['hour'] = df['timestamp'].dt.hour
                df['day_of_week'] = df['timestamp'].dt.dayofweek
                df['month'] = df['timestamp'].dt.month

            # Create statistical features
            numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
            for col in numeric_cols[:5]:  # Limit to avoid explosion
                df[f'{col}_zscore'] = (df[col] - df[col].mean()) / df[col].std()
                df[f'{col}_percentile'] = df[col].rank(pct=True)

            return pipeline.create_artifact(df, "features")

        @pipeline.task(depends_on=["feature_engineering"], consumes="features", produces="final")
        def final_aggregation():
            """Final aggregation and summary"""
            features = pipeline.get_artifact("features")
            df = features.as_dataframe()

            # Create summary statistics
            summary = {
                "row_count": len(df),
                "column_count": len(df.columns),
                "memory_usage_mb": df.memory_usage(deep=True).sum() / (1024 * 1024),
                "null_percentage": (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
            }

            # Perform aggregations
            if 'category' in df.columns:
                category_summary = df.groupby('category').agg({
                    col: ['mean', 'std'] for col in df.select_dtypes(include=['float64', 'int64']).columns[:3]
                })
                summary['categories'] = len(category_summary)

            return pipeline.create_artifact(summary, "final")

        # Execute pipeline
        start_time = time.time()
        results = pipeline.execute(parallel=False)
        end_time = time.time()

        metrics = {
            "duration_seconds": end_time - start_time,
            "pipeline_type": "complex_transformation",
            "success": results is not None
        }

        return metrics

    def run_io_intensive_benchmark(self, data_path: str, dataset_size: str) -> Dict:
        """Run I/O intensive benchmark

        Args:
            data_path: Path to input data file
            dataset_size: Size string for logging

        Returns:
            Execution metrics
        """
        pipeline = TaskPipeline(f"single_io_{dataset_size}")

        @pipeline.task(produces="data")
        def read_multiple_formats():
            """Read data in multiple formats"""
            df = pd.read_csv(data_path) if data_path.endswith('.csv') else pd.read_parquet(data_path)

            # Write to temporary formats
            temp_dir = f"benchmarks/temp/{dataset_size}"
            os.makedirs(temp_dir, exist_ok=True)

            # Test different I/O operations
            df.to_csv(f"{temp_dir}/temp.csv", index=False)
            df.to_json(f"{temp_dir}/temp.json", orient='records')
            df.to_parquet(f"{temp_dir}/temp.parquet", index=False)

            # Read them back
            df_csv = pd.read_csv(f"{temp_dir}/temp.csv")
            df_json = pd.read_json(f"{temp_dir}/temp.json")
            df_parquet = pd.read_parquet(f"{temp_dir}/temp.parquet")

            return pipeline.create_artifact(df, "data")

        @pipeline.task(depends_on=["read_multiple_formats"], consumes="data", produces="split_data")
        def split_and_merge():
            """Split data and merge back"""
            data = pipeline.get_artifact("data")
            df = data.as_dataframe()

            # Split into chunks
            chunk_size = max(1, len(df) // 10)
            chunks = [df.iloc[i:i+chunk_size] for i in range(0, len(df), chunk_size)]

            # Process each chunk
            processed_chunks = []
            for i, chunk in enumerate(chunks):
                # Simulate processing
                chunk['chunk_id'] = i
                processed_chunks.append(chunk)

            # Merge back
            merged_df = pd.concat(processed_chunks, ignore_index=True)

            return pipeline.create_artifact(merged_df, "split_data")

        # Execute
        start_time = time.time()
        results = pipeline.execute(parallel=False)
        end_time = time.time()

        # Clean up temp files
        import shutil
        temp_dir = f"benchmarks/temp/{dataset_size}"
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

        return {
            "duration_seconds": end_time - start_time,
            "pipeline_type": "io_intensive",
            "success": results is not None
        }