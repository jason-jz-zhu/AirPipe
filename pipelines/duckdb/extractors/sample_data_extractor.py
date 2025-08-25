"""
Sample Data Extractor for DuckDB Analytics.

Generates synthetic datasets for analytical workflows using pandas and numpy.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Optional

from airpipe.utils.duckdb import DuckDBArtifact
from airpipe.artifacts.data_artifact import ArtifactMetadata

LOG = logging.getLogger(__name__)


class SalesDataExtractor:
    """Extracts/generates sample sales data for analytics."""
    
    def __init__(self, seed: Optional[int] = 42):
        """Initialize extractor with optional random seed."""
        self.seed = seed
        
    def extract_sales_data(self, n_records: int = 10000) -> DuckDBArtifact:
        """
        Generate sample sales data.
        
        Args:
            n_records: Number of sales records to generate
            
        Returns:
            DuckDBArtifact containing sales data
        """
        LOG.info(f"Generating {n_records} sample sales records...")
        
        # Set random seed for reproducibility
        if self.seed is not None:
            np.random.seed(self.seed)
        
        # Create sample data
        df = pd.DataFrame({
            'order_id': range(1, n_records + 1),
            'customer_id': np.random.randint(1, 1000, n_records),
            'product_id': np.random.randint(1, 100, n_records),
            'quantity': np.random.randint(1, 10, n_records),
            'price': np.random.uniform(10, 1000, n_records),
            'discount': np.random.uniform(0, 0.3, n_records),
            'order_date': pd.date_range(
                start=datetime.now() - timedelta(days=365),
                end=datetime.now(),
                periods=n_records
            ),
            'region': np.random.choice(['North', 'South', 'East', 'West'], n_records),
            'category': np.random.choice(['Electronics', 'Clothing', 'Food', 'Books'], n_records)
        })
        
        # Calculate total amount
        df['total_amount'] = df['quantity'] * df['price'] * (1 - df['discount'])
        
        # Create DuckDB artifact
        metadata = ArtifactMetadata(
            source_component='SalesDataExtractor',
            row_count=len(df),
            column_count=len(df.columns),
            tags={
                'generated': str(datetime.now()),
                'extractor': 'SalesDataExtractor',
                'seed': self.seed,
                'records': len(df)
            }
        )
        
        artifact = DuckDBArtifact(
            df,
            name="sales_data",
            metadata=metadata
        )
        
        LOG.info(f"Generated {len(df)} sales records")
        return artifact
    
    def extract_customer_data(self, n_customers: int = 1000) -> DuckDBArtifact:
        """
        Generate sample customer data.
        
        Args:
            n_customers: Number of customers to generate
            
        Returns:
            DuckDBArtifact containing customer data
        """
        LOG.info(f"Generating {n_customers} sample customer records...")
        
        if self.seed is not None:
            np.random.seed(self.seed + 1)  # Different seed for customers
        
        df = pd.DataFrame({
            'customer_id': range(1, n_customers + 1),
            'customer_name': [f"Customer_{i}" for i in range(1, n_customers + 1)],
            'email': [f"customer{i}@example.com" for i in range(1, n_customers + 1)],
            'signup_date': pd.date_range(
                start=datetime.now() - timedelta(days=730),  # 2 years
                end=datetime.now() - timedelta(days=30),     # At least 30 days old
                periods=n_customers
            ),
            'age': np.random.randint(18, 80, n_customers),
            'income_bracket': np.random.choice(
                ['Low', 'Medium', 'High', 'Very High'], 
                n_customers,
                p=[0.3, 0.4, 0.2, 0.1]
            ),
            'location': np.random.choice(['Urban', 'Suburban', 'Rural'], n_customers)
        })
        
        # Create DuckDB artifact
        metadata = ArtifactMetadata(
            source_component='SalesDataExtractor',
            row_count=len(df),
            column_count=len(df.columns),
            tags={
                'generated': str(datetime.now()),
                'extractor': 'SalesDataExtractor',
                'seed': self.seed + 1,
                'records': len(df)
            }
        )
        
        artifact = DuckDBArtifact(
            df,
            name="customer_data",
            metadata=metadata
        )
        
        LOG.info(f"Generated {len(df)} customer records")
        return artifact
    
    def extract_product_data(self, n_products: int = 100) -> DuckDBArtifact:
        """
        Generate sample product data.
        
        Args:
            n_products: Number of products to generate
            
        Returns:
            DuckDBArtifact containing product data
        """
        LOG.info(f"Generating {n_products} sample product records...")
        
        if self.seed is not None:
            np.random.seed(self.seed + 2)  # Different seed for products
        
        categories = ['Electronics', 'Clothing', 'Food', 'Books']
        
        df = pd.DataFrame({
            'product_id': range(1, n_products + 1),
            'product_name': [f"Product_{i}" for i in range(1, n_products + 1)],
            'category': np.random.choice(categories, n_products),
            'base_price': np.random.uniform(10, 1000, n_products),
            'cost': np.random.uniform(5, 500, n_products),
            'launch_date': pd.date_range(
                start=datetime.now() - timedelta(days=1095),  # 3 years
                end=datetime.now() - timedelta(days=1),       # At least 1 day old
                periods=n_products
            ),
            'weight': np.random.uniform(0.1, 50.0, n_products),  # kg
            'discontinued': np.random.choice([True, False], n_products, p=[0.1, 0.9])
        })
        
        # Calculate profit margin
        df['profit_margin'] = (df['base_price'] - df['cost']) / df['base_price']
        
        # Create DuckDB artifact
        metadata = ArtifactMetadata(
            source_component='SalesDataExtractor',
            row_count=len(df),
            column_count=len(df.columns),
            tags={
                'generated': str(datetime.now()),
                'extractor': 'SalesDataExtractor',
                'seed': self.seed + 2,
                'records': len(df)
            }
        )
        
        artifact = DuckDBArtifact(
            df,
            name="product_data",
            metadata=metadata
        )
        
        LOG.info(f"Generated {len(df)} product records")
        return artifact