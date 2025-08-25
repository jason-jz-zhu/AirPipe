"""
Profile Transformer for DuckDB Analytics.

Provides data profiling and quality assessment operations for analytical workflows.
"""

import logging
from typing import Dict, List, Any, Optional

from airpipe.utils.duckdb import DuckDBArtifact, DuckDBSession

LOG = logging.getLogger(__name__)


class ProfileTransformer:
    """Data profiling operations for DuckDB data."""
    
    def __init__(self):
        """Initialize profile transformer."""
        pass
    
    def profile_data_artifact(
        self, 
        artifact: DuckDBArtifact,
        print_summary: bool = True
    ) -> Dict[str, Any]:
        """
        Profile data quality and characteristics of a DuckDB artifact.
        
        Args:
            artifact: Input data artifact to profile
            print_summary: Whether to print profile summary to console
            
        Returns:
            Dictionary containing profile information
        """
        LOG.info(f"Profiling data artifact: {artifact.name}")
        
        # Get profile from artifact
        profile = artifact.profile()
        
        if print_summary:
            self._print_profile_summary(artifact.name, profile)
        
        return profile
    
    def profile_customer_data(
        self,
        customer_artifact: DuckDBArtifact,
        print_summary: bool = True
    ) -> Dict[str, Any]:
        """
        Profile customer analysis data with business-specific insights.
        
        Args:
            customer_artifact: Customer data artifact
            print_summary: Whether to print profile summary to console
            
        Returns:
            Dictionary containing customer profile information
        """
        LOG.info("Profiling customer data with business insights")
        
        # Get connection and register data
        conn = DuckDBSession.get_or_create()
        df = customer_artifact.as_dataframe()
        conn.register('customer_data', df)
        
        # Basic profile
        profile = customer_artifact.profile()
        
        # Get customer-specific insights
        insights_sql = """
        SELECT 
            COUNT(*) as total_customers,
            AVG(lifetime_value) as avg_lifetime_value,
            MEDIAN(lifetime_value) as median_lifetime_value,
            MIN(lifetime_value) as min_lifetime_value,
            MAX(lifetime_value) as max_lifetime_value,
            AVG(total_orders) as avg_orders_per_customer,
            AVG(avg_order_value) as avg_order_value_overall,
            COUNT(CASE WHEN segment = 'Platinum' THEN 1 END) as platinum_customers,
            COUNT(CASE WHEN segment = 'Gold' THEN 1 END) as gold_customers,
            COUNT(CASE WHEN segment = 'Silver' THEN 1 END) as silver_customers,
            COUNT(CASE WHEN segment = 'Bronze' THEN 1 END) as bronze_customers,
            COUNT(CASE WHEN is_high_value = true THEN 1 END) as high_value_customers
        FROM customer_data
        """
        
        insights_df = conn.execute(insights_sql).fetchdf()
        insights = insights_df.iloc[0].to_dict()
        
        # Add business insights to profile
        profile['business_insights'] = insights
        
        if print_summary:
            self._print_customer_profile_summary(customer_artifact.name, profile)
        
        LOG.info("Customer profiling complete")
        return profile
    
    def profile_sales_patterns(
        self,
        sales_artifact: DuckDBArtifact,
        print_summary: bool = True
    ) -> Dict[str, Any]:
        """
        Profile sales data for patterns and trends.
        
        Args:
            sales_artifact: Sales data artifact
            print_summary: Whether to print profile summary to console
            
        Returns:
            Dictionary containing sales profile information
        """
        LOG.info("Profiling sales patterns and trends")
        
        # Get connection and register data
        conn = DuckDBSession.get_or_create()
        df = sales_artifact.as_dataframe()
        conn.register('sales_data', df)
        
        # Basic profile
        profile = sales_artifact.profile()
        
        # Get sales-specific insights
        patterns_sql = """
        SELECT 
            COUNT(*) as total_orders,
            COUNT(DISTINCT customer_id) as unique_customers,
            COUNT(DISTINCT product_id) as unique_products,
            COUNT(DISTINCT order_date::DATE) as unique_order_dates,
            SUM(total_amount) as total_revenue,
            AVG(total_amount) as avg_order_value,
            AVG(quantity) as avg_quantity_per_order,
            AVG(discount) as avg_discount_rate,
            MIN(order_date) as earliest_order,
            MAX(order_date) as latest_order,
            COUNT(CASE WHEN region = 'North' THEN 1 END) as north_orders,
            COUNT(CASE WHEN region = 'South' THEN 1 END) as south_orders,
            COUNT(CASE WHEN region = 'East' THEN 1 END) as east_orders,
            COUNT(CASE WHEN region = 'West' THEN 1 END) as west_orders,
            COUNT(CASE WHEN category = 'Electronics' THEN 1 END) as electronics_orders,
            COUNT(CASE WHEN category = 'Clothing' THEN 1 END) as clothing_orders,
            COUNT(CASE WHEN category = 'Food' THEN 1 END) as food_orders,
            COUNT(CASE WHEN category = 'Books' THEN 1 END) as books_orders
        FROM sales_data
        """
        
        patterns_df = conn.execute(patterns_sql).fetchdf()
        patterns = patterns_df.iloc[0].to_dict()
        
        # Add sales patterns to profile
        profile['sales_patterns'] = patterns
        
        if print_summary:
            self._print_sales_profile_summary(sales_artifact.name, profile)
        
        LOG.info("Sales profiling complete")
        return profile
    
    def compare_data_quality(
        self,
        artifacts: List[DuckDBArtifact],
        print_comparison: bool = True
    ) -> Dict[str, Dict[str, Any]]:
        """
        Compare data quality metrics across multiple artifacts.
        
        Args:
            artifacts: List of artifacts to compare
            print_comparison: Whether to print comparison summary
            
        Returns:
            Dictionary containing comparison results
        """
        LOG.info(f"Comparing data quality across {len(artifacts)} artifacts")
        
        comparison = {}
        
        for artifact in artifacts:
            profile = artifact.profile()
            comparison[artifact.name] = {
                'row_count': profile.get('row_count', 0),
                'column_count': len(profile.get('columns', [])),
                'null_columns': len([c for c in profile.get('columns', []) if c.get('null_count', 0) > 0]),
                'numeric_columns': len([c for c in profile.get('columns', []) if c.get('type') in ['int64', 'float64']]),
                'text_columns': len([c for c in profile.get('columns', []) if c.get('type') == 'object']),
                'date_columns': len([c for c in profile.get('columns', []) if 'datetime' in str(c.get('type', ''))]),
            }
        
        if print_comparison:
            self._print_quality_comparison(comparison)
        
        LOG.info("Data quality comparison complete")
        return comparison
    
    def _print_profile_summary(self, name: str, profile: Dict[str, Any]):
        """Print a formatted profile summary."""
        print(f"\n{'='*60}")
        print(f"DATA PROFILE: {name.upper()}")
        print(f"{'='*60}")
        print(f"Total Rows: {profile.get('row_count', 'N/A')}")
        print(f"Total Columns: {len(profile.get('columns', []))}")
        
        if 'numeric_statistics' in profile and profile['numeric_statistics']:
            print(f"\nNumeric Column Statistics (showing first 3):")
            for stat in profile['numeric_statistics'][:3]:
                print(f"\n{stat['column']}:")
                print(f"  Min: {stat.get('min', 'N/A'):.2f}")
                print(f"  Max: {stat.get('max', 'N/A'):.2f}")
                print(f"  Mean: {stat.get('mean', 'N/A'):.2f}")
                print(f"  Distinct: {stat.get('distinct_count', 'N/A')}")
    
    def _print_customer_profile_summary(self, name: str, profile: Dict[str, Any]):
        """Print a formatted customer profile summary."""
        self._print_profile_summary(name, profile)
        
        if 'business_insights' in profile:
            insights = profile['business_insights']
            print(f"\nCUSTOMER BUSINESS INSIGHTS:")
            print(f"Total Customers: {insights.get('total_customers', 'N/A')}")
            print(f"Average Lifetime Value: ${insights.get('avg_lifetime_value', 0):.2f}")
            print(f"Average Orders per Customer: {insights.get('avg_orders_per_customer', 0):.1f}")
            print(f"\nSegment Distribution:")
            print(f"  Platinum: {insights.get('platinum_customers', 0)}")
            print(f"  Gold: {insights.get('gold_customers', 0)}")
            print(f"  Silver: {insights.get('silver_customers', 0)}")
            print(f"  Bronze: {insights.get('bronze_customers', 0)}")
            print(f"  High Value: {insights.get('high_value_customers', 0)}")
    
    def _print_sales_profile_summary(self, name: str, profile: Dict[str, Any]):
        """Print a formatted sales profile summary."""
        self._print_profile_summary(name, profile)
        
        if 'sales_patterns' in profile:
            patterns = profile['sales_patterns']
            print(f"\nSALES PATTERNS:")
            print(f"Total Orders: {patterns.get('total_orders', 'N/A')}")
            print(f"Unique Customers: {patterns.get('unique_customers', 'N/A')}")
            print(f"Total Revenue: ${patterns.get('total_revenue', 0):,.2f}")
            print(f"Average Order Value: ${patterns.get('avg_order_value', 0):.2f}")
            print(f"\nRegional Distribution:")
            print(f"  North: {patterns.get('north_orders', 0)}")
            print(f"  South: {patterns.get('south_orders', 0)}")
            print(f"  East: {patterns.get('east_orders', 0)}")
            print(f"  West: {patterns.get('west_orders', 0)}")
    
    def _print_quality_comparison(self, comparison: Dict[str, Dict[str, Any]]):
        """Print a formatted data quality comparison."""
        print(f"\n{'='*60}")
        print(f"DATA QUALITY COMPARISON")
        print(f"{'='*60}")
        
        for name, metrics in comparison.items():
            print(f"\n{name}:")
            print(f"  Rows: {metrics.get('row_count', 0):,}")
            print(f"  Columns: {metrics.get('column_count', 0)}")
            print(f"  Numeric: {metrics.get('numeric_columns', 0)}")
            print(f"  Text: {metrics.get('text_columns', 0)}")
            print(f"  Date: {metrics.get('date_columns', 0)}")
            print(f"  Null Columns: {metrics.get('null_columns', 0)}")