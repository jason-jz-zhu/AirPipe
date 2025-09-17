"""Business insights transformer."""

import pandas as pd
from datetime import datetime
import logging

LOG = logging.getLogger(__name__)


class InsightsTransformer:
    """Generate business insights from analysis results."""
    
    def generate_insights(self, regional_df: pd.DataFrame, products_df: pd.DataFrame, 
                         segments_df: pd.DataFrame) -> pd.DataFrame:
        """Generate business insights from all analyses.
        
        Args:
            regional_df: Regional metrics dataframe
            products_df: Product rankings dataframe
            segments_df: Customer segments dataframe
            
        Returns:
            DataFrame with business insights
        """
        insights = []
        
        # Regional insights
        top_region = regional_df.loc[regional_df['total_sum'].idxmax()]
        insights.append(f"Top performing region: {top_region['region']} with ${top_region['total_sum']:,.2f} in sales")
        
        # Product insights
        top_3_products = products_df.head(3)
        insights.append(f"Top 3 products account for {top_3_products['revenue_percentage'].sum():.1f}% of revenue")
        
        # Customer segment insights
        best_segment = segments_df.loc[segments_df['avg_customer_value'].idxmax()]
        insights.append(f"Most valuable customer segment: {best_segment['customer_type']} "
                       f"with avg value of ${best_segment['avg_customer_value']:,.2f}")
        
        # Create insights DataFrame
        insights_df = pd.DataFrame({
            'insight_id': range(1, len(insights) + 1),
            'insight': insights,
            'generated_at': datetime.now()
        })
        
        LOG.info(f"Generated {len(insights)} business insights")
        return insights_df