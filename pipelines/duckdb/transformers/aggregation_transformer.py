"""
Aggregation Transformer for DuckDB Analytics.

Provides SQL-based aggregation operations for analytical workflows.
"""

import logging
from typing import Dict, List, Any, Optional

from airpipe.utils.duckdb import DuckDBArtifact, DuckDBSession
from airpipe.artifacts.data_artifact import ArtifactMetadata

LOG = logging.getLogger(__name__)


class AggregationTransformer:
    """SQL-based aggregation operations for DuckDB data."""
    
    def __init__(self):
        """Initialize aggregation transformer."""
        pass
    
    def aggregate_monthly_sales(
        self, 
        sales_artifact: DuckDBArtifact
    ) -> DuckDBArtifact:
        """
        Aggregate sales data by month, region, and category.
        
        Args:
            sales_artifact: Input sales data artifact
            
        Returns:
            DuckDBArtifact with monthly aggregated sales
        """
        LOG.info("Aggregating sales data by month, region, and category")
        
        # Get connection and register data
        conn = DuckDBSession.get_or_create()
        df = sales_artifact.as_dataframe()
        conn.register('sales_data', df)
        
        sql = """
        SELECT 
            DATE_TRUNC('month', order_date) as month,
            region,
            category,
            COUNT(*) as order_count,
            SUM(total_amount) as total_revenue,
            AVG(total_amount) as avg_order_value,
            SUM(quantity) as total_quantity,
            MIN(total_amount) as min_order_value,
            MAX(total_amount) as max_order_value
        FROM sales_data
        GROUP BY DATE_TRUNC('month', order_date), region, category
        ORDER BY month, region, category
        """
        
        LOG.info(f"Executing monthly aggregation query")
        result_df = conn.execute(sql).fetchdf()
        
        # Create result artifact
        metadata = ArtifactMetadata(
            source_component='AggregationTransformer.aggregate_monthly_sales',
            row_count=len(result_df),
            column_count=len(result_df.columns),
            tags={
                'original_records': len(df),
                'aggregated_records': len(result_df),
                'aggregation_level': 'month_region_category',
                'transformer': 'AggregationTransformer.aggregate_monthly_sales'
            }
        )
        
        artifact = DuckDBArtifact(
            result_df,
            name="monthly_summary",
            metadata=metadata
        )
        
        LOG.info(f"Aggregated {len(df)} records to {len(result_df)} monthly summaries")
        return artifact
    
    def aggregate_customer_metrics(
        self, 
        sales_artifact: DuckDBArtifact
    ) -> DuckDBArtifact:
        """
        Aggregate customer-level metrics.
        
        Args:
            sales_artifact: Input sales data artifact
            
        Returns:
            DuckDBArtifact with customer metrics
        """
        LOG.info("Aggregating customer-level metrics")
        
        # Get connection and register data
        conn = DuckDBSession.get_or_create()
        df = sales_artifact.as_dataframe()
        conn.register('sales_data', df)
        
        sql = """
        SELECT 
            customer_id,
            COUNT(DISTINCT order_id) as total_orders,
            SUM(total_amount) as lifetime_value,
            AVG(total_amount) as avg_order_value,
            MIN(order_date) as first_order,
            MAX(order_date) as last_order,
            COUNT(DISTINCT DATE_TRUNC('month', order_date)) as active_months,
            COUNT(DISTINCT category) as categories_purchased,
            COUNT(DISTINCT product_id) as unique_products,
            SUM(quantity) as total_items_purchased
        FROM sales_data
        GROUP BY customer_id
        ORDER BY lifetime_value DESC
        """
        
        LOG.info(f"Executing customer metrics aggregation query")
        result_df = conn.execute(sql).fetchdf()
        
        # Create result artifact
        artifact = DuckDBArtifact(
            result_df,
            name="customer_metrics",
            metadata={
                'original_records': len(df),
                'customer_records': len(result_df),
                'aggregation_level': 'customer',
                'transformer': 'AggregationTransformer.aggregate_customer_metrics'
            }
        )
        
        LOG.info(f"Aggregated {len(df)} sales records to {len(result_df)} customer metrics")
        return artifact
    
    def aggregate_regional_performance(
        self, 
        sales_artifact: DuckDBArtifact
    ) -> DuckDBArtifact:
        """
        Aggregate regional performance metrics.
        
        Args:
            sales_artifact: Input sales data artifact
            
        Returns:
            DuckDBArtifact with regional performance data
        """
        LOG.info("Aggregating regional performance metrics")
        
        # Get connection and register data
        conn = DuckDBSession.get_or_create()
        df = sales_artifact.as_dataframe()
        conn.register('sales_data', df)
        
        sql = """
        SELECT 
            region,
            COUNT(*) as total_orders,
            COUNT(DISTINCT customer_id) as unique_customers,
            COUNT(DISTINCT product_id) as unique_products,
            SUM(total_amount) as total_revenue,
            AVG(total_amount) as avg_order_value,
            SUM(quantity) as total_quantity,
            AVG(discount) as avg_discount_rate,
            MIN(order_date) as earliest_order,
            MAX(order_date) as latest_order
        FROM sales_data
        GROUP BY region
        ORDER BY total_revenue DESC
        """
        
        LOG.info(f"Executing regional performance aggregation query")
        result_df = conn.execute(sql).fetchdf()
        
        # Create result artifact
        artifact = DuckDBArtifact(
            result_df,
            name="regional_performance",
            metadata={
                'original_records': len(df),
                'regional_records': len(result_df),
                'aggregation_level': 'region',
                'transformer': 'AggregationTransformer.aggregate_regional_performance'
            }
        )
        
        LOG.info(f"Aggregated {len(df)} sales records to {len(result_df)} regional summaries")
        return artifact
    
    def aggregate_product_performance(
        self, 
        sales_artifact: DuckDBArtifact
    ) -> DuckDBArtifact:
        """
        Aggregate product performance metrics.
        
        Args:
            sales_artifact: Input sales data artifact
            
        Returns:
            DuckDBArtifact with product performance data
        """
        LOG.info("Aggregating product performance metrics")
        
        # Get connection and register data
        conn = DuckDBSession.get_or_create()
        df = sales_artifact.as_dataframe()
        conn.register('sales_data', df)
        
        sql = """
        SELECT 
            product_id,
            category,
            COUNT(*) as total_orders,
            COUNT(DISTINCT customer_id) as unique_customers,
            SUM(total_amount) as total_revenue,
            AVG(total_amount) as avg_order_value,
            SUM(quantity) as total_quantity_sold,
            AVG(price) as avg_selling_price,
            AVG(discount) as avg_discount_rate,
            MIN(order_date) as first_sale,
            MAX(order_date) as last_sale
        FROM sales_data
        GROUP BY product_id, category
        ORDER BY total_revenue DESC
        """
        
        LOG.info(f"Executing product performance aggregation query")
        result_df = conn.execute(sql).fetchdf()
        
        # Create result artifact
        artifact = DuckDBArtifact(
            result_df,
            name="product_performance",
            metadata={
                'original_records': len(df),
                'product_records': len(result_df),
                'aggregation_level': 'product',
                'transformer': 'AggregationTransformer.aggregate_product_performance'
            }
        )
        
        LOG.info(f"Aggregated {len(df)} sales records to {len(result_df)} product summaries")
        return artifact
    
    def get_sql_for_monthly_aggregation(self, table_name: str) -> str:
        """
        Generate SQL for monthly sales aggregation.
        
        Args:
            table_name: Name of the input table/artifact
            
        Returns:
            SQL query string
        """
        return f"""
        SELECT 
            DATE_TRUNC('month', order_date) as month,
            region,
            category,
            COUNT(*) as order_count,
            SUM(total_amount) as total_revenue,
            AVG(total_amount) as avg_order_value,
            SUM(quantity) as total_quantity
        FROM {{{table_name}}}
        GROUP BY DATE_TRUNC('month', order_date), region, category
        ORDER BY month, region, category
        """