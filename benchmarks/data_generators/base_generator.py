"""Base data generator for creating synthetic datasets"""

import os
import pandas as pd
import numpy as np
import json
import pyarrow.parquet as pq
import pyarrow as pa
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import random
import string


class DataGenerator:
    """Generate synthetic datasets of various sizes and formats"""

    # Size mappings
    SIZE_MAP = {
        "1MB": 1024 * 1024,
        "10MB": 10 * 1024 * 1024,
        "100MB": 100 * 1024 * 1024,
        "1GB": 1024 * 1024 * 1024,
        "5GB": 5 * 1024 * 1024 * 1024,
        "10GB": 10 * 1024 * 1024 * 1024,
    }

    def __init__(self, seed: int = 42):
        """Initialize generator with seed for reproducibility"""
        self.seed = seed
        np.random.seed(seed)
        random.seed(seed)

    def estimate_row_count(self, target_size_bytes: int, columns: int = 10) -> int:
        """Estimate number of rows needed for target size"""
        # Rough estimate: ~100 bytes per row for mixed data types
        bytes_per_row = columns * 50  # Conservative estimate
        return int(target_size_bytes / bytes_per_row)

    def generate_dataframe(self,
                          size_str: str,
                          columns: Optional[List[str]] = None,
                          dtypes: Optional[Dict[str, str]] = None) -> pd.DataFrame:
        """Generate a DataFrame of specified size

        Args:
            size_str: Size string like "1MB", "10MB", "1GB"
            columns: Column names (default: auto-generated)
            dtypes: Column data types (default: mixed types)

        Returns:
            Generated DataFrame
        """
        target_bytes = self.SIZE_MAP.get(size_str, 1024 * 1024)

        # Default columns if not specified
        if columns is None:
            columns = [
                "id", "timestamp", "customer_id", "product_id",
                "quantity", "price", "discount", "category",
                "region", "status", "description", "rating"
            ]

        row_count = self.estimate_row_count(target_bytes, len(columns))
        print(f"Generating {size_str} dataset with ~{row_count:,} rows")

        # Generate data based on column types
        data = {}
        base_date = datetime.now() - timedelta(days=365)

        for col in columns:
            if col == "id":
                data[col] = np.arange(row_count)
            elif col == "timestamp":
                data[col] = [base_date + timedelta(seconds=i*10) for i in range(row_count)]
            elif col in ["customer_id", "product_id"]:
                data[col] = np.random.randint(1000, 10000, row_count)
            elif col == "quantity":
                data[col] = np.random.randint(1, 100, row_count)
            elif col in ["price", "discount"]:
                data[col] = np.random.uniform(10.0, 1000.0, row_count).round(2)
            elif col == "category":
                categories = ["Electronics", "Clothing", "Food", "Books", "Sports"]
                data[col] = np.random.choice(categories, row_count)
            elif col == "region":
                regions = ["North", "South", "East", "West", "Central"]
                data[col] = np.random.choice(regions, row_count)
            elif col == "status":
                statuses = ["pending", "completed", "cancelled", "processing"]
                data[col] = np.random.choice(statuses, row_count)
            elif col == "description":
                # Generate random text
                data[col] = [''.join(random.choices(string.ascii_letters + ' ', k=50))
                            for _ in range(row_count)]
            elif col == "rating":
                data[col] = np.random.uniform(1.0, 5.0, row_count).round(1)
            else:
                # Default to random floats
                data[col] = np.random.randn(row_count)

        df = pd.DataFrame(data)

        # Check actual size and adjust if needed
        actual_bytes = df.memory_usage(deep=True).sum()
        if actual_bytes < target_bytes * 0.9:  # If less than 90% of target
            # Add padding column to reach target size
            padding_size = int((target_bytes - actual_bytes) / row_count)
            if padding_size > 0:
                df['padding'] = ['x' * padding_size for _ in range(row_count)]

        return df

    def generate_csv(self, size_str: str, output_path: str) -> str:
        """Generate CSV file of specified size"""
        df = self.generate_dataframe(size_str)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        actual_size = os.path.getsize(output_path)
        print(f"Generated CSV: {output_path} ({actual_size / (1024*1024):.2f} MB)")
        return output_path

    def generate_json(self, size_str: str, output_path: str) -> str:
        """Generate JSON file of specified size"""
        df = self.generate_dataframe(size_str)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Convert to records format for JSON
        records = df.to_dict('records')
        with open(output_path, 'w') as f:
            json.dump(records, f, default=str)

        actual_size = os.path.getsize(output_path)
        print(f"Generated JSON: {output_path} ({actual_size / (1024*1024):.2f} MB)")
        return output_path

    def generate_parquet(self, size_str: str, output_path: str) -> str:
        """Generate Parquet file of specified size"""
        df = self.generate_dataframe(size_str)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Convert to pyarrow table and write
        table = pa.Table.from_pandas(df)
        pq.write_table(table, output_path, compression='snappy')

        actual_size = os.path.getsize(output_path)
        print(f"Generated Parquet: {output_path} ({actual_size / (1024*1024):.2f} MB)")
        return output_path

    def generate_streaming_data(self, records_per_batch: int = 1000,
                              total_batches: int = 100) -> List[pd.DataFrame]:
        """Generate data for streaming scenarios"""
        batches = []
        for i in range(total_batches):
            # Generate small batch
            batch_data = {
                "batch_id": [i] * records_per_batch,
                "record_id": np.arange(i * records_per_batch, (i + 1) * records_per_batch),
                "timestamp": [datetime.now() + timedelta(seconds=j) for j in range(records_per_batch)],
                "value": np.random.randn(records_per_batch),
                "category": np.random.choice(["A", "B", "C"], records_per_batch),
                "metric": np.random.uniform(0, 100, records_per_batch)
            }
            batches.append(pd.DataFrame(batch_data))
        return batches

    def cleanup_generated_files(self, directory: str = "benchmarks/data"):
        """Clean up generated test data files"""
        if os.path.exists(directory):
            import shutil
            shutil.rmtree(directory)
            print(f"Cleaned up {directory}")