"""
Aggregation utilities for data transformation.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union, Any
import logging

logger = logging.getLogger(__name__)


class AggregationUtils:
    """Reusable aggregation operations for DataFrames."""
    
    # Common aggregation functions
    AGGREGATIONS = {
        'sum': 'sum',
        'mean': 'mean',
        'median': 'median',
        'min': 'min',
        'max': 'max',
        'count': 'count',
        'std': 'std',
        'var': 'var',
        'first': 'first',
        'last': 'last',
        'nunique': 'nunique',
        'mode': lambda x: x.mode().iloc[0] if not x.mode().empty else None,
        'range': lambda x: x.max() - x.min(),
        'q25': lambda x: x.quantile(0.25),
        'q75': lambda x: x.quantile(0.75),
        'iqr': lambda x: x.quantile(0.75) - x.quantile(0.25),
    }
    
    @staticmethod
    def group_and_aggregate(df: pd.DataFrame,
                           group_by: Union[str, List[str]],
                           aggregations: Dict[str, Union[str, List[str], Dict]]) -> pd.DataFrame:
        """
        Group DataFrame and apply aggregations.
        
        Args:
            df: DataFrame to aggregate
            group_by: Column(s) to group by
            aggregations: Dict mapping columns to aggregation functions
                         e.g., {'salary': ['mean', 'max'], 'age': 'mean'}
            
        Returns:
            Aggregated DataFrame
        """
        if isinstance(group_by, str):
            group_by = [group_by]
        
        # Validate group by columns
        missing_cols = set(group_by) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Group by columns not found: {missing_cols}")
        
        try:
            # Perform aggregation
            grouped = df.groupby(group_by)
            result = grouped.agg(aggregations)
            
            # Flatten column names if multi-level
            if isinstance(result.columns, pd.MultiIndex):
                result.columns = ['_'.join(col).strip() for col in result.columns.values]
            
            result = result.reset_index()
            
            logger.info(f"Grouped by {group_by} with {len(result)} groups")
            return result
        except Exception as e:
            logger.error(f"Error during aggregation: {e}")
            raise
    
    @staticmethod
    def pivot_table(df: pd.DataFrame,
                   values: Union[str, List[str]],
                   index: Union[str, List[str]],
                   columns: Optional[Union[str, List[str]]] = None,
                   aggfunc: Union[str, Dict] = 'mean',
                   fill_value: Any = None) -> pd.DataFrame:
        """
        Create pivot table from DataFrame.
        
        Args:
            df: DataFrame to pivot
            values: Column(s) to aggregate
            index: Column(s) to use as index
            columns: Column(s) to use as columns
            aggfunc: Aggregation function(s)
            fill_value: Value to use for missing data
            
        Returns:
            Pivot table as DataFrame
        """
        try:
            pivot = pd.pivot_table(
                df,
                values=values,
                index=index,
                columns=columns,
                aggfunc=aggfunc,
                fill_value=fill_value
            )
            
            # Reset index to make it a regular DataFrame
            pivot = pivot.reset_index()
            
            logger.info(f"Created pivot table with shape {pivot.shape}")
            return pivot
        except Exception as e:
            logger.error(f"Error creating pivot table: {e}")
            raise
    
    @staticmethod
    def rolling_aggregation(df: pd.DataFrame,
                          column: str,
                          window: int,
                          function: str = 'mean',
                          min_periods: Optional[int] = None,
                          center: bool = False) -> pd.Series:
        """
        Apply rolling window aggregation.
        
        Args:
            df: DataFrame containing the data
            column: Column to aggregate
            window: Size of the rolling window
            function: Aggregation function ('mean', 'sum', 'std', etc.)
            min_periods: Minimum number of observations required
            center: Whether to center the window
            
        Returns:
            Series with rolling aggregation results
        """
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")
        
        try:
            rolling = df[column].rolling(
                window=window,
                min_periods=min_periods,
                center=center
            )
            
            if function in ['mean', 'sum', 'std', 'var', 'min', 'max', 'median']:
                result = getattr(rolling, function)()
            else:
                raise ValueError(f"Unsupported rolling function: {function}")
            
            logger.info(f"Applied {window}-period rolling {function} to {column}")
            return result
        except Exception as e:
            logger.error(f"Error in rolling aggregation: {e}")
            raise
    
    @staticmethod
    def time_based_aggregation(df: pd.DataFrame,
                             date_column: str,
                             freq: str,
                             aggregations: Dict[str, Union[str, List[str]]]) -> pd.DataFrame:
        """
        Aggregate data by time periods.
        
        Args:
            df: DataFrame with time series data
            date_column: Name of datetime column
            freq: Frequency for resampling ('D', 'W', 'M', 'Q', 'Y')
            aggregations: Aggregation functions for each column
            
        Returns:
            Time-aggregated DataFrame
        """
        if date_column not in df.columns:
            raise ValueError(f"Date column '{date_column}' not found")
        
        try:
            # Ensure date column is datetime
            df_copy = df.copy()
            df_copy[date_column] = pd.to_datetime(df_copy[date_column])
            
            # Set as index for resampling
            df_copy = df_copy.set_index(date_column)
            
            # Resample and aggregate
            result = df_copy.resample(freq).agg(aggregations)
            
            # Flatten column names if multi-level
            if isinstance(result.columns, pd.MultiIndex):
                result.columns = ['_'.join(col).strip() for col in result.columns.values]
            
            result = result.reset_index()
            
            logger.info(f"Aggregated by {freq} frequency: {len(result)} periods")
            return result
        except Exception as e:
            logger.error(f"Error in time-based aggregation: {e}")
            raise
    
    @staticmethod
    def calculate_percentiles(df: pd.DataFrame,
                            column: str,
                            percentiles: List[float] = [25, 50, 75]) -> Dict[str, float]:
        """
        Calculate percentiles for a column.
        
        Args:
            df: DataFrame containing the data
            column: Column to calculate percentiles for
            percentiles: List of percentiles to calculate (0-100)
            
        Returns:
            Dictionary mapping percentile to value
        """
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")
        
        result = {}
        for p in percentiles:
            if not 0 <= p <= 100:
                logger.warning(f"Invalid percentile {p}, skipping")
                continue
            
            value = df[column].quantile(p / 100)
            result[f"p{int(p)}"] = value
        
        logger.info(f"Calculated {len(result)} percentiles for {column}")
        return result
    
    @staticmethod
    def create_bins(df: pd.DataFrame,
                   column: str,
                   bins: Union[int, List],
                   labels: Optional[List[str]] = None,
                   right: bool = True) -> pd.Series:
        """
        Create bins/buckets for continuous data.
        
        Args:
            df: DataFrame containing the data
            column: Column to bin
            bins: Number of bins or bin edges
            labels: Labels for the bins
            right: Include right edge of bin
            
        Returns:
            Series with binned values
        """
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")
        
        try:
            binned = pd.cut(df[column], bins=bins, labels=labels, right=right)
            logger.info(f"Created {len(binned.cat.categories)} bins for {column}")
            return binned
        except Exception as e:
            logger.error(f"Error creating bins: {e}")
            raise