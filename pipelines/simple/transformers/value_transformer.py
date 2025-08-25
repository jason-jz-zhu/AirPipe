"""
Value transformation logic for simple workflows.
"""

import pandas as pd
import numpy as np
import logging
from typing import Optional, Callable
from airpipe.utils.transformers.filter_utils import FilterUtils

logger = logging.getLogger(__name__)


class ValueTransformer:
    """Transform and manipulate value data."""
    
    def __init__(self):
        self.filter_utils = FilterUtils()
    
    def filter_and_transform(self,
                            df: pd.DataFrame,
                            threshold: float = 500,
                            transformation: str = 'square') -> pd.DataFrame:
        """
        Filter data by value threshold and apply transformation.
        
        Args:
            df: Input DataFrame with 'value' column
            threshold: Value threshold for filtering
            transformation: Type of transformation ('square', 'sqrt', 'log')
            
        Returns:
            Filtered and transformed DataFrame
        """
        # Filter by threshold
        filtered = self.filter_utils.filter_by_column(df, 'value', '>', threshold)
        
        # Apply transformation
        if transformation == 'square':
            filtered['value_squared'] = filtered['value'] ** 2
            logger.info(f"Applied square transformation to {len(filtered)} records")
        elif transformation == 'sqrt':
            filtered['value_sqrt'] = np.sqrt(filtered['value'])
            logger.info(f"Applied square root transformation to {len(filtered)} records")
        elif transformation == 'log':
            filtered['value_log'] = np.log(filtered['value'])
            logger.info(f"Applied log transformation to {len(filtered)} records")
        else:
            logger.warning(f"Unknown transformation: {transformation}")
        
        logger.info(f"Filtered from {len(df)} to {len(filtered)} records (threshold: {threshold})")
        return filtered
    
    def add_derived_metrics(self,
                           df: pd.DataFrame) -> pd.DataFrame:
        """
        Add derived metrics to the DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with additional derived metrics
        """
        df_copy = df.copy()
        
        # Add percentage of max value
        if 'value' in df_copy.columns:
            max_value = df_copy['value'].max()
            df_copy['value_pct_of_max'] = (df_copy['value'] / max_value) * 100
        
        # Add cumulative sum
        if 'value' in df_copy.columns:
            df_copy['value_cumsum'] = df_copy['value'].cumsum()
        
        # Add rank
        if 'value' in df_copy.columns:
            df_copy['value_rank'] = df_copy['value'].rank(ascending=False)
        
        logger.info(f"Added derived metrics to {len(df_copy)} records")
        return df_copy
    
    def categorize_values(self,
                         df: pd.DataFrame,
                         bins: Optional[list] = None,
                         labels: Optional[list] = None) -> pd.DataFrame:
        """
        Categorize values into bins.
        
        Args:
            df: Input DataFrame with 'value' column
            bins: Custom bin edges
            labels: Labels for bins
            
        Returns:
            DataFrame with value categories
        """
        df_copy = df.copy()
        
        if 'value' not in df_copy.columns:
            logger.warning("No 'value' column found for categorization")
            return df_copy
        
        if bins is None:
            # Create quartile bins
            bins = [0, df_copy['value'].quantile(0.25), 
                   df_copy['value'].quantile(0.5),
                   df_copy['value'].quantile(0.75),
                   df_copy['value'].max() + 1]
            
        if labels is None:
            labels = ['Low', 'Medium-Low', 'Medium-High', 'High']
        
        df_copy['value_category'] = pd.cut(df_copy['value'], bins=bins, labels=labels)
        
        logger.info(f"Categorized {len(df_copy)} records into {len(labels)} categories")
        return df_copy