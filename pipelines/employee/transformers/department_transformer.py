"""
Employee department analysis and transformation logic.
"""

import pandas as pd
import logging
from typing import Dict, Any, Optional
from airpipe.utils.transformers.aggregation_utils import AggregationUtils

logger = logging.getLogger(__name__)


class EmployeeDepartmentTransformer:
    """Transform and analyze employee data by department."""
    
    def __init__(self):
        self.agg_utils = AggregationUtils()
    
    def aggregate_by_department(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate comprehensive statistics by department.
        
        Args:
            df: Employee DataFrame
            
        Returns:
            DataFrame with department statistics
        """
        if 'department' not in df.columns:
            logger.warning("No department column found")
            return pd.DataFrame()
        
        # Group by department and calculate stats
        stats = df.groupby('department').agg({
            'salary': ['mean', 'max', 'min', 'count'],
            'age': 'mean' if 'age' in df.columns else lambda x: None,
            'id': 'count'
        })
        
        # Flatten column names
        stats.columns = ['_'.join(col).strip() if col[1] else col[0] 
                        for col in stats.columns.values]
        stats = stats.reset_index()
        
        # Clean up column names
        stats = stats.rename(columns={
            'id_count': 'employee_count',
            'salary_count': 'salary_records'
        })
        
        # Add additional metrics
        if 'salary_mean' in stats.columns and 'salary_max' in stats.columns:
            stats['salary_range'] = stats['salary_max'] - stats['salary_min']
            stats['salary_spread_ratio'] = stats['salary_max'] / stats['salary_min']
        
        logger.info(f"Calculated statistics for {len(stats)} departments")
        return stats
    
    def rank_departments(self, 
                        df: pd.DataFrame,
                        rank_by: str = 'salary_mean') -> pd.DataFrame:
        """
        Rank departments by specified metric.
        
        Args:
            df: Department statistics DataFrame
            rank_by: Column to rank by
            
        Returns:
            DataFrame with department rankings
        """
        if rank_by not in df.columns:
            logger.warning(f"Column {rank_by} not found for ranking")
            return df
        
        df_copy = df.copy()
        df_copy['rank'] = df_copy[rank_by].rank(ascending=False)
        df_copy = df_copy.sort_values('rank')
        
        logger.info(f"Ranked {len(df_copy)} departments by {rank_by}")
        return df_copy
    
    def calculate_department_growth(self,
                                   current_df: pd.DataFrame,
                                   previous_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Calculate department growth metrics if previous data available.
        
        Args:
            current_df: Current department statistics
            previous_df: Previous period statistics (optional)
            
        Returns:
            DataFrame with growth metrics
        """
        if previous_df is None:
            logger.info("No previous data provided, returning current stats only")
            return current_df
        
        # Merge current and previous data
        merged = pd.merge(
            current_df,
            previous_df,
            on='department',
            suffixes=('_current', '_previous'),
            how='outer'
        )
        
        # Calculate growth metrics
        if 'employee_count_current' in merged.columns:
            merged['headcount_growth'] = (
                (merged['employee_count_current'] - merged['employee_count_previous']) / 
                merged['employee_count_previous'] * 100
            )
        
        if 'salary_mean_current' in merged.columns:
            merged['salary_growth'] = (
                (merged['salary_mean_current'] - merged['salary_mean_previous']) / 
                merged['salary_mean_previous'] * 100
            )
        
        logger.info(f"Calculated growth metrics for {len(merged)} departments")
        return merged