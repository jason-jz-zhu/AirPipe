"""Customer segment transformer."""

import pandas as pd
import logging

LOG = logging.getLogger(__name__)


class CustomerTransformer:
    """Transform and analyze customer segments."""
    
    def analyze_customer_segments(self, sales_df: pd.DataFrame, customer_df: pd.DataFrame) -> pd.DataFrame:
        """Analyze customer segments.
        
        Args:
            sales_df: Sales dataframe with columns: customer_type, total, quantity
            customer_df: Customer dataframe with columns: customer_type
            
        Returns:
            DataFrame with customer segment analysis
        """
        segment_sales = sales_df.groupby('customer_type').agg({
            'total': ['sum', 'mean', 'count'],
            'quantity': 'sum'
        })
        
        segment_sales.columns = ['_'.join(col).strip() for col in segment_sales.columns.values]
        segment_sales = segment_sales.reset_index()
        
        customer_counts = customer_df['customer_type'].value_counts().reset_index()
        customer_counts.columns = ['customer_type', 'customer_count']
        
        segments = pd.merge(segment_sales, customer_counts, on='customer_type', how='left')
        
        segments['avg_customer_value'] = segments['total_sum'] / segments['customer_count']
        segments['avg_order_frequency'] = segments['total_count'] / segments['customer_count']
        
        LOG.info(f"Analyzed {len(segments)} customer segments")
        return segments