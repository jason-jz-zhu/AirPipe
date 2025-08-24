"""
Employee salary transformation logic.
"""

import pandas as pd
import numpy as np
from typing import Optional, Dict, Any
from airpipe.utils.transformers.filter_utils import FilterUtils
from airpipe.utils.transformers.aggregation_utils import AggregationUtils
import logging

logger = logging.getLogger(__name__)


class SalaryTransformer:
    """Transform and analyze employee salary data."""
    
    def __init__(self):
        self.filter_utils = FilterUtils()
        self.agg_utils = AggregationUtils()
    
    def filter_high_earners(self, 
                           df: pd.DataFrame, 
                           threshold: float = 70000) -> pd.DataFrame:
        """
        Filter employees by salary threshold with business rules.
        
        Args:
            df: Employee DataFrame
            threshold: Salary threshold for high earners
            
        Returns:
            DataFrame of high earning employees
        """
        # Use utility for basic filtering
        filtered = self.filter_utils.filter_by_column(df, 'salary', '>', threshold)
        
        # Apply business logic for categorization
        filtered['earner_category'] = filtered['salary'].apply(
            lambda x: 'very_high' if x > 100000 else (
                'high' if x > threshold else 'standard'
            )
        )
        
        # Department-specific adjustments
        if 'department' in filtered.columns:
            # Engineering has different thresholds
            eng_mask = filtered['department'] == 'Engineering'
            filtered.loc[eng_mask, 'earner_category'] = filtered.loc[eng_mask, 'salary'].apply(
                lambda x: 'very_high' if x > 120000 else 'high'
            )
            
            # Sales includes commission
            sales_mask = filtered['department'] == 'Sales'
            if 'commission' in filtered.columns:
                filtered.loc[sales_mask, 'total_compensation'] = (
                    filtered.loc[sales_mask, 'salary'] + 
                    filtered.loc[sales_mask, 'commission'].fillna(0)
                )
        
        logger.info(f"Filtered {len(filtered)} high earners from {len(df)} employees")
        return filtered
    
    def calculate_salary_statistics(self, 
                                   df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate comprehensive salary statistics.
        
        Args:
            df: Employee DataFrame
            
        Returns:
            DataFrame with salary statistics
        """
        stats = {}
        
        # Basic statistics
        stats['mean_salary'] = df['salary'].mean()
        stats['median_salary'] = df['salary'].median()
        stats['std_salary'] = df['salary'].std()
        stats['min_salary'] = df['salary'].min()
        stats['max_salary'] = df['salary'].max()
        
        # Percentiles
        percentiles = self.agg_utils.calculate_percentiles(df, 'salary', [10, 25, 50, 75, 90])
        stats.update(percentiles)
        
        # Salary range
        stats['salary_range'] = stats['max_salary'] - stats['min_salary']
        
        # Coefficient of variation
        stats['cv'] = (stats['std_salary'] / stats['mean_salary']) * 100
        
        # Create DataFrame
        stats_df = pd.DataFrame([stats])
        stats_df['analysis_date'] = pd.Timestamp.now()
        
        logger.info("Calculated comprehensive salary statistics")
        return stats_df
    
    def analyze_salary_by_department(self, 
                                    df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze salary distribution by department.
        
        Args:
            df: Employee DataFrame
            
        Returns:
            DataFrame with department salary analysis
        """
        if 'department' not in df.columns:
            logger.warning("No department column found, returning empty analysis")
            return pd.DataFrame()
        
        # Use aggregation utility
        dept_stats = self.agg_utils.group_and_aggregate(
            df,
            group_by='department',
            aggregations={
                'salary': ['mean', 'median', 'std', 'min', 'max', 'count'],
                'id': 'count'
            }
        )
        
        # Rename columns for clarity
        dept_stats = dept_stats.rename(columns={
            'salary_mean': 'avg_salary',
            'salary_median': 'median_salary',
            'salary_std': 'salary_std_dev',
            'salary_min': 'min_salary',
            'salary_max': 'max_salary',
            'salary_count': 'employee_count',
            'id_count': 'total_employees'
        })
        
        # Calculate additional metrics
        dept_stats['salary_range'] = dept_stats['max_salary'] - dept_stats['min_salary']
        dept_stats['cv'] = (dept_stats['salary_std_dev'] / dept_stats['avg_salary']) * 100
        
        # Rank departments by average salary
        dept_stats['salary_rank'] = dept_stats['avg_salary'].rank(ascending=False)
        
        logger.info(f"Analyzed salary for {len(dept_stats)} departments")
        return dept_stats
    
    def create_salary_bands(self, 
                          df: pd.DataFrame,
                          num_bands: int = 5) -> pd.DataFrame:
        """
        Create salary bands for compensation analysis.
        
        Args:
            df: Employee DataFrame
            num_bands: Number of salary bands to create
            
        Returns:
            DataFrame with salary band assignments
        """
        df_copy = df.copy()
        
        # Create salary bands using quantiles
        band_labels = [f'Band_{i+1}' for i in range(num_bands)]
        df_copy['salary_band'] = pd.qcut(
            df_copy['salary'],
            q=num_bands,
            labels=band_labels
        )
        
        # Calculate band ranges
        band_ranges = df_copy.groupby('salary_band')['salary'].agg(['min', 'max', 'count'])
        band_ranges.columns = ['band_min', 'band_max', 'employee_count']
        
        # Add band midpoint
        band_ranges['band_midpoint'] = (band_ranges['band_min'] + band_ranges['band_max']) / 2
        
        logger.info(f"Created {num_bands} salary bands")
        
        # Add band information back to employee data
        df_copy = df_copy.merge(
            band_ranges,
            left_on='salary_band',
            right_index=True,
            how='left'
        )
        
        return df_copy
    
    def calculate_pay_equity_metrics(self, 
                                    df: pd.DataFrame,
                                    group_column: str = 'department') -> pd.DataFrame:
        """
        Calculate pay equity metrics across groups.
        
        Args:
            df: Employee DataFrame
            group_column: Column to analyze equity across
            
        Returns:
            DataFrame with pay equity metrics
        """
        if group_column not in df.columns:
            logger.warning(f"Column {group_column} not found")
            return pd.DataFrame()
        
        equity_metrics = []
        
        for group in df[group_column].unique():
            group_df = df[df[group_column] == group]
            
            metrics = {
                group_column: group,
                'employee_count': len(group_df),
                'salary_mean': group_df['salary'].mean(),
                'salary_median': group_df['salary'].median(),
                'salary_std': group_df['salary'].std(),
                'gini_coefficient': self._calculate_gini(group_df['salary'])
            }
            
            # Calculate pay ratio (highest to lowest)
            if len(group_df) > 0:
                metrics['pay_ratio'] = group_df['salary'].max() / group_df['salary'].min()
            
            equity_metrics.append(metrics)
        
        result = pd.DataFrame(equity_metrics)
        
        # Add overall comparison
        overall_mean = df['salary'].mean()
        result['mean_vs_overall'] = ((result['salary_mean'] - overall_mean) / overall_mean) * 100
        
        logger.info(f"Calculated pay equity metrics for {len(result)} groups")
        return result
    
    def _calculate_gini(self, salaries: pd.Series) -> float:
        """
        Calculate Gini coefficient for salary distribution.
        
        Args:
            salaries: Series of salary values
            
        Returns:
            Gini coefficient (0 = perfect equality, 1 = perfect inequality)
        """
        sorted_salaries = np.sort(salaries)
        n = len(sorted_salaries)
        
        if n == 0:
            return 0.0
        
        index = np.arange(1, n + 1)
        return (2 * np.sum(index * sorted_salaries)) / (n * np.sum(sorted_salaries)) - (n + 1) / n