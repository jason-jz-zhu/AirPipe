"""
Analytics Transformer for DuckDB Analytics.

Provides complex SQL-based analytics operations including customer segmentation,
behavior analysis, and business intelligence computations.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple

from airpipe.utils.duckdb import DuckDBArtifact, DuckDBSession

LOG = logging.getLogger(__name__)


class AnalyticsTransformer:
    """Complex analytics operations for DuckDB data."""
    
    def __init__(self):
        """Initialize analytics transformer."""
        pass
    
    def analyze_customer_segments(
        self, 
        sales_artifact: DuckDBArtifact,
        high_value_artifact: Optional[DuckDBArtifact] = None
    ) -> DuckDBArtifact:
        """
        Perform comprehensive customer segmentation analysis.
        
        Args:
            sales_artifact: Input sales data artifact
            high_value_artifact: Optional high-value sales artifact for cross-reference
            
        Returns:
            DuckDBArtifact with customer analysis and segmentation
        """
        LOG.info("Performing customer segmentation analysis")
        
        # Get connection and register data
        conn = DuckDBSession.get_or_create()
        
        # Register sales data
        sales_df = sales_artifact.as_dataframe()
        conn.register('sales_data', sales_df)
        
        # Register high-value data if provided
        if high_value_artifact:
            high_value_df = high_value_artifact.as_dataframe()
            conn.register('high_value_sales', high_value_df)
            high_value_join = """
            LEFT JOIN (SELECT DISTINCT customer_id FROM high_value_sales) hvc 
            ON cs.customer_id = hvc.customer_id
            """
            is_high_value = "CASE WHEN hvc.customer_id IS NOT NULL THEN TRUE ELSE FALSE END"
        else:
            high_value_join = ""
            is_high_value = "FALSE"
        
        sql = f"""
        WITH customer_stats AS (
            SELECT 
                customer_id,
                COUNT(DISTINCT order_id) as total_orders,
                SUM(total_amount) as lifetime_value,
                AVG(total_amount) as avg_order_value,
                MIN(order_date) as first_order,
                MAX(order_date) as last_order,
                COUNT(DISTINCT DATE_TRUNC('month', order_date)) as active_months,
                COUNT(DISTINCT category) as categories_purchased,
                COUNT(DISTINCT product_id) as unique_products_purchased,
                COUNT(DISTINCT region) as regions_shopped,
                SUM(quantity) as total_items_purchased,
                AVG(discount) as avg_discount_used
            FROM sales_data
            GROUP BY customer_id
        ),
        customer_segments AS (
            SELECT 
                cs.*,
                {is_high_value} as is_high_value,
                CASE
                    WHEN cs.lifetime_value > 10000 THEN 'Platinum'
                    WHEN cs.lifetime_value > 5000 THEN 'Gold'
                    WHEN cs.lifetime_value > 1000 THEN 'Silver'
                    ELSE 'Bronze'
                END as segment,
                CASE
                    WHEN cs.total_orders >= 20 THEN 'Frequent'
                    WHEN cs.total_orders >= 10 THEN 'Regular'
                    WHEN cs.total_orders >= 5 THEN 'Occasional'
                    ELSE 'Rare'
                END as purchase_frequency,
                EXTRACT(DAY FROM cs.last_order - cs.first_order) as customer_tenure_days,
                CASE
                    WHEN EXTRACT(DAY FROM CURRENT_DATE - cs.last_order) <= 30 THEN 'Active'
                    WHEN EXTRACT(DAY FROM CURRENT_DATE - cs.last_order) <= 90 THEN 'At Risk'
                    ELSE 'Churned'
                END as activity_status
            FROM customer_stats cs
            {high_value_join}
        )
        SELECT * FROM customer_segments
        ORDER BY lifetime_value DESC
        """
        
        LOG.info(f"Executing customer segmentation query")
        result_df = conn.execute(sql).fetchdf()
        
        # Create result artifact
        artifact = DuckDBArtifact(
            result_df,
            name="customer_analysis",
            metadata={
                'original_records': len(sales_df),
                'customer_records': len(result_df),
                'analysis_type': 'customer_segmentation',
                'includes_high_value_flag': high_value_artifact is not None,
                'transformer': 'AnalyticsTransformer.analyze_customer_segments'
            }
        )
        
        LOG.info(f"Analyzed {len(result_df)} customers with segmentation")
        return artifact
    
    def calculate_cohort_analysis(
        self,
        sales_artifact: DuckDBArtifact
    ) -> DuckDBArtifact:
        """
        Calculate customer cohort analysis based on first purchase month.
        
        Args:
            sales_artifact: Input sales data artifact
            
        Returns:
            DuckDBArtifact with cohort analysis data
        """
        LOG.info("Calculating customer cohort analysis")
        
        # Get connection and register data
        conn = DuckDBSession.get_or_create()
        df = sales_artifact.as_dataframe()
        conn.register('sales_data', df)
        
        sql = """
        WITH customer_cohorts AS (
            -- Get first purchase month for each customer
            SELECT 
                customer_id,
                DATE_TRUNC('month', MIN(order_date)) as cohort_month
            FROM sales_data
            GROUP BY customer_id
        ),
        cohort_data AS (
            SELECT 
                cc.cohort_month,
                DATE_TRUNC('month', s.order_date) as period_month,
                EXTRACT(MONTH FROM AGE(DATE_TRUNC('month', s.order_date), cc.cohort_month)) as period_number,
                s.customer_id,
                SUM(s.total_amount) as revenue
            FROM sales_data s
            JOIN customer_cohorts cc ON s.customer_id = cc.customer_id
            GROUP BY cc.cohort_month, DATE_TRUNC('month', s.order_date), s.customer_id
        )
        SELECT 
            cohort_month,
            period_number,
            COUNT(DISTINCT customer_id) as customers,
            SUM(revenue) as total_revenue,
            AVG(revenue) as avg_revenue_per_customer
        FROM cohort_data
        GROUP BY cohort_month, period_number
        ORDER BY cohort_month, period_number
        """
        
        LOG.info(f"Executing cohort analysis query")
        result_df = conn.execute(sql).fetchdf()
        
        # Create result artifact
        artifact = DuckDBArtifact(
            result_df,
            name="cohort_analysis",
            metadata={
                'original_records': len(df),
                'cohort_records': len(result_df),
                'analysis_type': 'cohort_analysis',
                'transformer': 'AnalyticsTransformer.calculate_cohort_analysis'
            }
        )
        
        LOG.info(f"Generated {len(result_df)} cohort data points")
        return artifact
    
    def analyze_rfm_segments(
        self,
        sales_artifact: DuckDBArtifact
    ) -> DuckDBArtifact:
        """
        Calculate RFM (Recency, Frequency, Monetary) analysis for customer segmentation.
        
        Args:
            sales_artifact: Input sales data artifact
            
        Returns:
            DuckDBArtifact with RFM analysis data
        """
        LOG.info("Calculating RFM (Recency, Frequency, Monetary) analysis")
        
        # Get connection and register data
        conn = DuckDBSession.get_or_create()
        df = sales_artifact.as_dataframe()
        conn.register('sales_data', df)
        
        sql = """
        WITH rfm_base AS (
            SELECT 
                customer_id,
                EXTRACT(DAY FROM CURRENT_DATE - MAX(order_date)) as recency_days,
                COUNT(DISTINCT order_id) as frequency,
                SUM(total_amount) as monetary_value
            FROM sales_data
            GROUP BY customer_id
        ),
        rfm_percentiles AS (
            SELECT 
                *,
                NTILE(5) OVER (ORDER BY recency_days DESC) as recency_score,
                NTILE(5) OVER (ORDER BY frequency ASC) as frequency_score, 
                NTILE(5) OVER (ORDER BY monetary_value ASC) as monetary_score
            FROM rfm_base
        ),
        rfm_segments AS (
            SELECT 
                *,
                CONCAT(recency_score, frequency_score, monetary_score) as rfm_score,
                CASE
                    WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
                    WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal Customers'
                    WHEN recency_score >= 4 AND frequency_score <= 2 THEN 'New Customers'
                    WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score <= 2 THEN 'Potential Loyalists'
                    WHEN recency_score >= 3 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'Promising'
                    WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Cant Lose Them'
                    WHEN recency_score <= 2 AND frequency_score >= 2 AND monetary_score >= 2 THEN 'At Risk'
                    WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'Price Sensitive'
                    ELSE 'Others'
                END as rfm_segment
            FROM rfm_percentiles
        )
        SELECT * FROM rfm_segments
        ORDER BY monetary_value DESC
        """
        
        LOG.info(f"Executing RFM analysis query")
        result_df = conn.execute(sql).fetchdf()
        
        # Create result artifact
        artifact = DuckDBArtifact(
            result_df,
            name="rfm_analysis",
            metadata={
                'original_records': len(df),
                'customer_records': len(result_df),
                'analysis_type': 'rfm_analysis',
                'transformer': 'AnalyticsTransformer.analyze_rfm_segments'
            }
        )
        
        LOG.info(f"Analyzed {len(result_df)} customers with RFM segmentation")
        return artifact
    
    def get_sql_for_customer_analysis(
        self, 
        sales_table: str, 
        high_value_table: Optional[str] = None
    ) -> str:
        """
        Generate SQL for customer segmentation analysis.
        
        Args:
            sales_table: Name of the sales table/artifact
            high_value_table: Optional name of high-value sales table/artifact
            
        Returns:
            SQL query string
        """
        if high_value_table:
            high_value_join = f"""
            LEFT JOIN (SELECT DISTINCT customer_id FROM {{{high_value_table}}}) hvc 
            ON cs.customer_id = hvc.customer_id
            """
            is_high_value = "CASE WHEN hvc.customer_id IS NOT NULL THEN TRUE ELSE FALSE END"
        else:
            high_value_join = ""
            is_high_value = "FALSE"
        
        return f"""
        WITH customer_stats AS (
            SELECT 
                customer_id,
                COUNT(DISTINCT order_id) as total_orders,
                SUM(total_amount) as lifetime_value,
                AVG(total_amount) as avg_order_value,
                MIN(order_date) as first_order,
                MAX(order_date) as last_order,
                COUNT(DISTINCT DATE_TRUNC('month', order_date)) as active_months,
                COUNT(DISTINCT category) as categories_purchased
            FROM {{{sales_table}}}
            GROUP BY customer_id
        ),
        customer_segments AS (
            SELECT 
                cs.*,
                {is_high_value} as is_high_value,
                CASE
                    WHEN cs.lifetime_value > 10000 THEN 'Platinum'
                    WHEN cs.lifetime_value > 5000 THEN 'Gold'
                    WHEN cs.lifetime_value > 1000 THEN 'Silver'
                    ELSE 'Bronze'
                END as segment,
                EXTRACT(DAY FROM cs.last_order - cs.first_order) as customer_tenure_days
            FROM customer_stats cs
            {high_value_join}
        )
        SELECT * FROM customer_segments
        ORDER BY lifetime_value DESC
        """