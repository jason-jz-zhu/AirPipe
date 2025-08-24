"""Product sales data transformer."""

import pandas as pd
import logging

LOG = logging.getLogger(__name__)


class ProductTransformer:
    """Transform sales data by product."""
    
    def identify_top_products(self, df: pd.DataFrame) -> pd.DataFrame:
        """Identify top selling products.
        
        Args:
            df: Sales dataframe with columns: product, quantity, total, transaction_id
            
        Returns:
            DataFrame with product rankings and metrics
        """
        products = df.groupby('product').agg({
            'quantity': 'sum',
            'total': 'sum',
            'transaction_id': 'count'
        }).reset_index()
        
        products.columns = ['product', 'units_sold', 'revenue', 'num_transactions']
        
        products = products.sort_values('revenue', ascending=False)
        products['rank'] = range(1, len(products) + 1)
        
        total_revenue = products['revenue'].sum()
        products['revenue_percentage'] = (products['revenue'] / total_revenue * 100).round(2)
        
        LOG.info(f"Top product: {products.iloc[0]['product']} with ${products.iloc[0]['revenue']:,.2f} revenue")
        return products