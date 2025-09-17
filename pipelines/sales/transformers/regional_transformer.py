"""Regional sales data transformer."""

import pandas as pd
import logging

LOG = logging.getLogger(__name__)


class RegionalTransformer:
    """Transform sales data by region."""
    
    def calculate_regional_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate metrics by region.
        
        Args:
            df: Sales dataframe with columns: region, total, quantity, transaction_id
            
        Returns:
            DataFrame with regional metrics
        """
        regional = df.groupby('region').agg({
            'total': ['sum', 'mean', 'count'],
            'quantity': 'sum',
            'transaction_id': 'count'
        })
        
        regional.columns = ['_'.join(col).strip() for col in regional.columns.values]
        regional = regional.reset_index()
        regional.rename(columns={'transaction_id_count': 'num_transactions'}, inplace=True)
        
        regional['avg_transaction_size'] = regional['total_sum'] / regional['num_transactions']
        regional['avg_quantity_per_transaction'] = regional['quantity_sum'] / regional['num_transactions']
        
        LOG.info(f"Calculated metrics for {len(regional)} regions")
        return regional