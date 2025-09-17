"""
Filter Transformer for DuckDB Analytics.

Provides SQL-based filtering operations for analytical workflows.
"""

import logging
from typing import Dict, List, Any, Optional

from airpipe.utils.duckdb import DuckDBArtifact, DuckDBSession
from airpipe.artifacts.data_artifact import ArtifactMetadata

LOG = logging.getLogger(__name__)


class FilterTransformer:
    """SQL-based filtering operations for DuckDB data."""
    
    def __init__(self):
        """Initialize filter transformer."""
        pass
    
    def filter_high_value_sales(
        self, 
        sales_artifact: DuckDBArtifact, 
        threshold: float = 500.0,
        regions: Optional[List[str]] = None
    ) -> DuckDBArtifact:
        """
        Filter sales data for high-value transactions.
        
        Args:
            sales_artifact: Input sales data artifact
            threshold: Minimum total_amount threshold
            regions: Optional list of regions to include
            
        Returns:
            DuckDBArtifact with filtered high-value sales
        """
        LOG.info(f"Filtering sales data with threshold ${threshold}")
        
        # Get connection and register data
        conn = DuckDBSession.get_or_create()
        df = sales_artifact.as_dataframe()
        conn.register('sales_input', df)
        
        # Build SQL query
        where_conditions = [f"total_amount > {threshold}"]
        
        if regions:
            regions_str = "', '".join(regions)
            where_conditions.append(f"region IN ('{regions_str}')")
        
        sql = f"""
        SELECT * FROM sales_input 
        WHERE {' AND '.join(where_conditions)}
        ORDER BY total_amount DESC
        """
        
        LOG.info(f"Executing filter query: {sql}")
        result_df = conn.execute(sql).fetchdf()
        
        # Create result artifact
        metadata = ArtifactMetadata(
            source_component='FilterTransformer.filter_high_value_sales',
            row_count=len(result_df),
            column_count=len(result_df.columns),
            tags={
                'original_records': len(df),
                'filtered_records': len(result_df),
                'threshold': threshold,
                'regions': regions,
                'transformer': 'FilterTransformer.filter_high_value_sales'
            }
        )
        
        artifact = DuckDBArtifact(
            result_df,
            name="high_value_sales",
            metadata=metadata
        )
        
        LOG.info(f"Filtered {len(df)} records to {len(result_df)} high-value sales")
        return artifact
    
    def filter_by_date_range(
        self,
        artifact: DuckDBArtifact,
        date_column: str,
        start_date: str,
        end_date: str,
        table_name: str = "input_data"
    ) -> DuckDBArtifact:
        """
        Filter data by date range.
        
        Args:
            artifact: Input data artifact
            date_column: Name of the date column
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            table_name: Name for temporary table registration
            
        Returns:
            DuckDBArtifact with date-filtered data
        """
        LOG.info(f"Filtering by date range: {start_date} to {end_date}")
        
        # Get connection and register data
        conn = DuckDBSession.get_or_create()
        df = artifact.as_dataframe()
        conn.register(table_name, df)
        
        sql = f"""
        SELECT * FROM {table_name}
        WHERE {date_column} >= '{start_date}' 
        AND {date_column} <= '{end_date}'
        ORDER BY {date_column}
        """
        
        LOG.info(f"Executing date filter query: {sql}")
        result_df = conn.execute(sql).fetchdf()
        
        # Create result artifact
        metadata = ArtifactMetadata(
            source_component='FilterTransformer.filter_by_date_range',
            row_count=len(result_df),
            column_count=len(result_df.columns),
            tags={
                'original_records': len(df),
                'filtered_records': len(result_df),
                'date_column': date_column,
                'start_date': start_date,
                'end_date': end_date,
                'transformer': 'FilterTransformer.filter_by_date_range'
            }
        )
        
        result_artifact = DuckDBArtifact(
            result_df,
            name=f"{artifact.name}_date_filtered",
            metadata=metadata
        )
        
        LOG.info(f"Date filtered {len(df)} records to {len(result_df)} records")
        return result_artifact
    
    def filter_by_category(
        self,
        artifact: DuckDBArtifact,
        category_column: str,
        categories: List[str],
        table_name: str = "input_data"
    ) -> DuckDBArtifact:
        """
        Filter data by category values.
        
        Args:
            artifact: Input data artifact
            category_column: Name of the category column
            categories: List of categories to include
            table_name: Name for temporary table registration
            
        Returns:
            DuckDBArtifact with category-filtered data
        """
        LOG.info(f"Filtering by categories: {categories}")
        
        # Get connection and register data
        conn = DuckDBSession.get_or_create()
        df = artifact.as_dataframe()
        conn.register(table_name, df)
        
        categories_str = "', '".join(categories)
        sql = f"""
        SELECT * FROM {table_name}
        WHERE {category_column} IN ('{categories_str}')
        """
        
        LOG.info(f"Executing category filter query: {sql}")
        result_df = conn.execute(sql).fetchdf()
        
        # Create result artifact
        metadata = ArtifactMetadata(
            source_component='FilterTransformer.filter_by_category',
            row_count=len(result_df),
            column_count=len(result_df.columns),
            tags={
                'original_records': len(df),
                'filtered_records': len(result_df),
                'category_column': category_column,
                'categories': categories,
                'transformer': 'FilterTransformer.filter_by_category'
            }
        )
        
        result_artifact = DuckDBArtifact(
            result_df,
            name=f"{artifact.name}_category_filtered",
            metadata=metadata
        )
        
        LOG.info(f"Category filtered {len(df)} records to {len(result_df)} records")
        return result_artifact
    
    def get_sql_for_high_value_filter(
        self, 
        table_name: str, 
        threshold: float = 500.0, 
        regions: Optional[List[str]] = None
    ) -> str:
        """
        Generate SQL for high-value sales filtering.
        
        Args:
            table_name: Name of the input table/artifact
            threshold: Minimum total_amount threshold
            regions: Optional list of regions to include
            
        Returns:
            SQL query string
        """
        where_conditions = [f"total_amount > {threshold}"]
        
        if regions:
            regions_str = "', '".join(regions)
            where_conditions.append(f"region IN ('{regions_str}')")
        
        return f"""
        SELECT * FROM {{{table_name}}}
        WHERE {' AND '.join(where_conditions)}
        ORDER BY total_amount DESC
        """