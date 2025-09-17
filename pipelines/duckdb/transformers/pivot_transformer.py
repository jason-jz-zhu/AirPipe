"""
Pivot Transformer for DuckDB Analytics.

Provides data pivoting and reshaping operations using DuckDB's high-performance
operations for analytical workflows.
"""

import logging
from typing import Dict, List, Any, Optional

from airpipe.utils.duckdb import DuckDBArtifact, DuckDBSession, DuckDBOperations

LOG = logging.getLogger(__name__)


class PivotTransformer:
    """Pivot and reshape operations for DuckDB data."""
    
    def __init__(self):
        """Initialize pivot transformer."""
        self.operations = DuckDBOperations()
    
    def create_monthly_revenue_pivot(
        self, 
        monthly_summary_artifact: DuckDBArtifact
    ) -> DuckDBArtifact:
        """
        Create a pivot report of monthly revenue by region.
        
        Args:
            monthly_summary_artifact: Input monthly summary data
            
        Returns:
            DuckDBArtifact with pivoted monthly revenue data
        """
        LOG.info("Creating monthly revenue pivot report")
        
        # Get connection and register data
        conn = DuckDBSession.get_or_create()
        df = monthly_summary_artifact.as_dataframe()
        conn.register('monthly_data', df)
        
        # Use DuckDB operations to pivot
        pivoted = self.operations.pivot_data(
            table='monthly_data',
            index=['month'],
            columns='region',
            values='total_revenue',
            agg_func='SUM'
        )
        
        # Create artifact
        artifact = DuckDBArtifact(
            pivoted,
            name="monthly_pivot",
            metadata={
                'pivot_columns': 'region',
                'pivot_values': 'total_revenue',
                'pivot_index': 'month',
                'original_records': len(df),
                'pivoted_shape': pivoted.shape,
                'transformer': 'PivotTransformer.create_monthly_revenue_pivot'
            }
        )
        
        LOG.info(f"Created pivot report with shape: {pivoted.shape}")
        return artifact
    
    def create_category_performance_pivot(
        self,
        sales_artifact: DuckDBArtifact
    ) -> DuckDBArtifact:
        """
        Create a pivot of sales performance by category and region.
        
        Args:
            sales_artifact: Input sales data
            
        Returns:
            DuckDBArtifact with category performance pivot
        """
        LOG.info("Creating category performance pivot")
        
        # Get connection and register data
        conn = DuckDBSession.get_or_create()
        df = sales_artifact.as_dataframe()
        conn.register('sales_data', df)
        
        # First aggregate the data
        agg_sql = """
        SELECT 
            category,
            region,
            COUNT(*) as order_count,
            SUM(total_amount) as total_revenue,
            AVG(total_amount) as avg_order_value
        FROM sales_data
        GROUP BY category, region
        """
        
        agg_df = conn.execute(agg_sql).fetchdf()
        conn.register('category_agg', agg_df)
        
        # Pivot the aggregated data
        pivoted = self.operations.pivot_data(
            table='category_agg',
            index=['category'],
            columns='region',
            values='total_revenue',
            agg_func='SUM'
        )
        
        # Create artifact
        artifact = DuckDBArtifact(
            pivoted,
            name="category_performance_pivot",
            metadata={
                'pivot_columns': 'region',
                'pivot_values': 'total_revenue',
                'pivot_index': 'category',
                'original_records': len(df),
                'aggregated_records': len(agg_df),
                'pivoted_shape': pivoted.shape,
                'transformer': 'PivotTransformer.create_category_performance_pivot'
            }
        )
        
        LOG.info(f"Created category performance pivot with shape: {pivoted.shape}")
        return artifact
    
    def create_customer_segment_pivot(
        self,
        customer_analysis_artifact: DuckDBArtifact
    ) -> DuckDBArtifact:
        """
        Create a pivot of customer metrics by segment.
        
        Args:
            customer_analysis_artifact: Input customer analysis data
            
        Returns:
            DuckDBArtifact with customer segment pivot
        """
        LOG.info("Creating customer segment pivot")
        
        # Get connection and register data
        conn = DuckDBSession.get_or_create()
        df = customer_analysis_artifact.as_dataframe()
        conn.register('customer_data', df)
        
        # Aggregate customer data by segment
        agg_sql = """
        SELECT 
            segment,
            COUNT(*) as customer_count,
            AVG(lifetime_value) as avg_lifetime_value,
            AVG(total_orders) as avg_orders,
            AVG(avg_order_value) as avg_order_value,
            SUM(lifetime_value) as total_segment_value
        FROM customer_data
        GROUP BY segment
        """
        
        agg_df = conn.execute(agg_sql).fetchdf()
        
        # For this case, we'll transpose the metrics as columns
        transposed_sql = """
        SELECT 
            'Customer Count' as metric,
            SUM(CASE WHEN segment = 'Bronze' THEN customer_count END) as Bronze,
            SUM(CASE WHEN segment = 'Silver' THEN customer_count END) as Silver,
            SUM(CASE WHEN segment = 'Gold' THEN customer_count END) as Gold,
            SUM(CASE WHEN segment = 'Platinum' THEN customer_count END) as Platinum
        FROM customer_segment_agg
        
        UNION ALL
        
        SELECT 
            'Avg Lifetime Value' as metric,
            AVG(CASE WHEN segment = 'Bronze' THEN avg_lifetime_value END) as Bronze,
            AVG(CASE WHEN segment = 'Silver' THEN avg_lifetime_value END) as Silver,
            AVG(CASE WHEN segment = 'Gold' THEN avg_lifetime_value END) as Gold,
            AVG(CASE WHEN segment = 'Platinum' THEN avg_lifetime_value END) as Platinum
        FROM customer_segment_agg
        
        UNION ALL
        
        SELECT 
            'Avg Orders' as metric,
            AVG(CASE WHEN segment = 'Bronze' THEN avg_orders END) as Bronze,
            AVG(CASE WHEN segment = 'Silver' THEN avg_orders END) as Silver,
            AVG(CASE WHEN segment = 'Gold' THEN avg_orders END) as Gold,
            AVG(CASE WHEN segment = 'Platinum' THEN avg_orders END) as Platinum
        FROM customer_segment_agg
        """
        
        conn.register('customer_segment_agg', agg_df)
        pivoted_df = conn.execute(transposed_sql).fetchdf()
        
        # Create artifact
        artifact = DuckDBArtifact(
            pivoted_df,
            name="customer_segment_pivot",
            metadata={
                'pivot_type': 'segment_metrics_transpose',
                'original_records': len(df),
                'segments_analyzed': len(agg_df),
                'pivoted_shape': pivoted_df.shape,
                'transformer': 'PivotTransformer.create_customer_segment_pivot'
            }
        )
        
        LOG.info(f"Created customer segment pivot with shape: {pivoted_df.shape}")
        return artifact
    
    def create_time_series_pivot(
        self,
        sales_artifact: DuckDBArtifact,
        date_column: str = 'order_date',
        value_column: str = 'total_amount',
        pivot_column: str = 'category',
        time_unit: str = 'month'
    ) -> DuckDBArtifact:
        """
        Create a time series pivot for trend analysis.
        
        Args:
            sales_artifact: Input sales data
            date_column: Name of date column for time axis
            value_column: Name of value column to aggregate
            pivot_column: Column values to pivot as columns
            time_unit: Time unit for aggregation ('day', 'week', 'month', 'quarter')
            
        Returns:
            DuckDBArtifact with time series pivot
        """
        LOG.info(f"Creating time series pivot by {time_unit} for {pivot_column}")
        
        # Get connection and register data
        conn = DuckDBSession.get_or_create()
        df = sales_artifact.as_dataframe()
        conn.register('sales_data', df)
        
        # Create time series aggregation
        time_trunc_sql = f"DATE_TRUNC('{time_unit}', {date_column})"
        
        agg_sql = f"""
        SELECT 
            {time_trunc_sql} as time_period,
            {pivot_column},
            SUM({value_column}) as total_value,
            COUNT(*) as record_count
        FROM sales_data
        GROUP BY {time_trunc_sql}, {pivot_column}
        ORDER BY time_period, {pivot_column}
        """
        
        agg_df = conn.execute(agg_sql).fetchdf()
        conn.register('time_series_agg', agg_df)
        
        # Pivot the time series data
        pivoted = self.operations.pivot_data(
            table='time_series_agg',
            index=['time_period'],
            columns=pivot_column,
            values='total_value',
            agg_func='SUM'
        )
        
        # Create artifact
        artifact = DuckDBArtifact(
            pivoted,
            name="time_series_pivot",
            metadata={
                'pivot_columns': pivot_column,
                'pivot_values': value_column,
                'pivot_index': 'time_period',
                'time_unit': time_unit,
                'date_column': date_column,
                'original_records': len(df),
                'aggregated_records': len(agg_df),
                'pivoted_shape': pivoted.shape,
                'transformer': 'PivotTransformer.create_time_series_pivot'
            }
        )
        
        LOG.info(f"Created time series pivot with shape: {pivoted.shape}")
        return artifact
    
    def create_custom_pivot(
        self,
        artifact: DuckDBArtifact,
        index_columns: List[str],
        pivot_column: str,
        value_column: str,
        agg_func: str = 'SUM',
        table_name: str = 'input_data'
    ) -> DuckDBArtifact:
        """
        Create a custom pivot with specified parameters.
        
        Args:
            artifact: Input data artifact
            index_columns: Columns to use as index (rows)
            pivot_column: Column values to pivot as columns
            value_column: Column values to aggregate
            agg_func: Aggregation function ('SUM', 'AVG', 'COUNT', etc.)
            table_name: Name for temporary table registration
            
        Returns:
            DuckDBArtifact with custom pivot
        """
        LOG.info(f"Creating custom pivot: {index_columns} x {pivot_column} = {value_column}")
        
        # Get connection and register data
        conn = DuckDBSession.get_or_create()
        df = artifact.as_dataframe()
        conn.register(table_name, df)
        
        # Use DuckDB operations to pivot
        pivoted = self.operations.pivot_data(
            table=table_name,
            index=index_columns,
            columns=pivot_column,
            values=value_column,
            agg_func=agg_func
        )
        
        # Create artifact
        result_artifact = DuckDBArtifact(
            pivoted,
            name=f"{artifact.name}_custom_pivot",
            metadata={
                'pivot_columns': pivot_column,
                'pivot_values': value_column,
                'pivot_index': index_columns,
                'agg_func': agg_func,
                'original_records': len(df),
                'pivoted_shape': pivoted.shape,
                'transformer': 'PivotTransformer.create_custom_pivot'
            }
        )
        
        LOG.info(f"Created custom pivot with shape: {pivoted.shape}")
        return result_artifact