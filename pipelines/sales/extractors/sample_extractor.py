"""Sample sales data extractor."""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import logging

LOG = logging.getLogger(__name__)


class SalesDataExtractor:
    """Extract sample sales data."""
    
    def extract_sales_data(self, n_records: int = 1000) -> pd.DataFrame:
        """Extract sample sales data.
        
        Args:
            n_records: Number of records to generate
            
        Returns:
            DataFrame with sales data
        """
        LOG.info(f"Extracting {n_records} sales records...")
        
        data = pd.DataFrame({
            'transaction_id': range(1, n_records + 1),
            'date': [datetime.now() - timedelta(days=random.randint(0, 365)) 
                    for _ in range(n_records)],
            'product': np.random.choice(['Widget', 'Gadget', 'Tool', 'Device'], n_records),
            'quantity': np.random.randint(1, 20, n_records),
            'price': np.random.uniform(10, 500, n_records),
            'region': np.random.choice(['North', 'South', 'East', 'West'], n_records),
            'customer_type': np.random.choice(['Retail', 'Wholesale', 'Online'], n_records)
        })
        
        data['total'] = data['quantity'] * data['price']
        
        LOG.info(f"Extracted {len(data)} sales records")
        return data
    
    def extract_customer_data(self, n_customers: int = 200) -> pd.DataFrame:
        """Extract sample customer data.
        
        Args:
            n_customers: Number of customers to generate
            
        Returns:
            DataFrame with customer data
        """
        LOG.info(f"Extracting {n_customers} customer records...")
        
        data = pd.DataFrame({
            'customer_id': range(1, n_customers + 1),
            'customer_type': np.random.choice(['Retail', 'Wholesale', 'Online'], n_customers),
            'loyalty_tier': np.random.choice(['Bronze', 'Silver', 'Gold', 'Platinum'], n_customers),
            'join_date': [datetime.now() - timedelta(days=random.randint(0, 1000)) 
                         for _ in range(n_customers)],
            'total_purchases': np.random.randint(1, 100, n_customers)
        })
        
        LOG.info(f"Extracted {len(data)} customer records")
        return data