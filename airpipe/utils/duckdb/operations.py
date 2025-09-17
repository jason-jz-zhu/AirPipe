"""
DuckDB Operations Utilities for AirPipe.

Provides helper functions for common DuckDB operations like data profiling,
format conversions, query optimization, and analytical functions.
"""

import duckdb
from typing import Dict, List, Any, Optional, Union, Tuple
import pandas as pd
from pathlib import Path
import logging

from .session import DuckDBSession

LOG = logging.getLogger(__name__)


class DuckDBOperations:
    """
    Collection of DuckDB operation utilities.
    
    These operations use the DuckDBSession to perform various
    analytical and data manipulation tasks.
    """
    
    @staticmethod
    def profile_data(
        table_or_query: str,
        database: str = ':memory:',
        include_advanced: bool = False
    ) -> Dict[str, Any]:
        """
        Profile data to understand its characteristics.
        
        Args:
            table_or_query: Table name or SQL query
            database: Database to use
            include_advanced: Include advanced statistics
            
        Returns:
            Dictionary with profiling results
        """
        conn = DuckDBSession.get_or_create({'database': database})
        
        # Determine if input is table or query
        if ' ' in table_or_query or '(' in table_or_query:
            # It's a query
            source = f"({table_or_query}) as data"
        else:
            # It's a table name
            source = table_or_query
        
        profile = {}
        
        # Basic statistics
        try:
            # Row count
            count_result = conn.execute(f"SELECT COUNT(*) FROM {source}").fetchone()
            profile['row_count'] = count_result[0] if count_result else 0
            
            # Column information
            columns_result = conn.execute(f"DESCRIBE {source}").fetchall()
            profile['columns'] = [
                {
                    'name': col[0],
                    'type': col[1],
                    'nullable': col[2] if len(col) > 2 else None
                }
                for col in columns_result
            ]
            
            # Memory usage
            if not database.startswith(':'):
                size_result = conn.execute(
                    f"SELECT SUM(estimated_size) FROM duckdb_tables() WHERE table_name = '{table_or_query}'"
                ).fetchone()
                profile['estimated_size_bytes'] = size_result[0] if size_result and size_result[0] else 0
            
            if include_advanced:
                # Numeric column statistics
                numeric_stats = []
                for col in profile['columns']:
                    if 'INT' in col['type'].upper() or 'FLOAT' in col['type'].upper() or 'DOUBLE' in col['type'].upper():
                        stats = conn.execute(f"""
                            SELECT 
                                MIN({col['name']}) as min,
                                MAX({col['name']}) as max,
                                AVG({col['name']}) as mean,
                                MEDIAN({col['name']}) as median,
                                STDDEV({col['name']}) as stddev,
                                COUNT(DISTINCT {col['name']}) as distinct_count,
                                COUNT(*) - COUNT({col['name']}) as null_count
                            FROM {source}
                        """).fetchone()
                        
                        numeric_stats.append({
                            'column': col['name'],
                            'min': stats[0],
                            'max': stats[1],
                            'mean': stats[2],
                            'median': stats[3],
                            'stddev': stats[4],
                            'distinct_count': stats[5],
                            'null_count': stats[6]
                        })
                
                profile['numeric_statistics'] = numeric_stats
                
                # String column statistics
                string_stats = []
                for col in profile['columns']:
                    if 'VARCHAR' in col['type'].upper() or 'TEXT' in col['type'].upper():
                        stats = conn.execute(f"""
                            SELECT 
                                MIN(LENGTH({col['name']})) as min_length,
                                MAX(LENGTH({col['name']})) as max_length,
                                AVG(LENGTH({col['name']})) as avg_length,
                                COUNT(DISTINCT {col['name']}) as distinct_count,
                                COUNT(*) - COUNT({col['name']}) as null_count
                            FROM {source}
                        """).fetchone()
                        
                        string_stats.append({
                            'column': col['name'],
                            'min_length': stats[0],
                            'max_length': stats[1],
                            'avg_length': stats[2],
                            'distinct_count': stats[3],
                            'null_count': stats[4]
                        })
                
                profile['string_statistics'] = string_stats
        
        except Exception as e:
            LOG.error(f"Error profiling data: {e}")
            raise
        
        return profile
    
    @staticmethod
    def convert_format(
        source_path: str,
        target_path: str,
        source_format: str = 'auto',
        target_format: str = 'parquet',
        database: str = ':memory:',
        **kwargs
    ) -> bool:
        """
        Convert data between different formats.
        
        Args:
            source_path: Path to source file
            target_path: Path to target file
            source_format: Source format (csv, json, parquet, auto)
            target_format: Target format (csv, json, parquet)
            database: Database to use
            **kwargs: Additional format-specific options
            
        Returns:
            True if successful
        """
        conn = DuckDBSession.get_or_create({'database': database})
        
        try:
            # Auto-detect source format
            if source_format == 'auto':
                source_ext = Path(source_path).suffix.lower()
                format_map = {
                    '.csv': 'csv',
                    '.json': 'json',
                    '.parquet': 'parquet',
                    '.pq': 'parquet',
                    '.jsonl': 'json'
                }
                source_format = format_map.get(source_ext, 'csv')
            
            # Read source data
            if source_format == 'csv':
                data = conn.read_csv(source_path, **kwargs)
            elif source_format == 'json':
                data = conn.read_json(source_path, **kwargs)
            elif source_format == 'parquet':
                data = conn.read_parquet(source_path, **kwargs)
            else:
                raise ValueError(f"Unsupported source format: {source_format}")
            
            # Write target data
            if target_format == 'csv':
                data.to_csv(target_path)
            elif target_format == 'json':
                data.to_json(target_path)
            elif target_format == 'parquet':
                data.to_parquet(target_path)
            else:
                raise ValueError(f"Unsupported target format: {target_format}")
            
            LOG.info(f"Converted {source_path} ({source_format}) to {target_path} ({target_format})")
            return True
            
        except Exception as e:
            LOG.error(f"Error converting format: {e}")
            raise
    
    @staticmethod
    def optimize_query(
        query: str,
        database: str = ':memory:',
        explain: bool = True
    ) -> Dict[str, Any]:
        """
        Analyze and optimize a SQL query.
        
        Args:
            query: SQL query to optimize
            database: Database to use
            explain: Include execution plan
            
        Returns:
            Dictionary with optimization results
        """
        conn = DuckDBSession.get_or_create({'database': database})
        
        result = {
            'original_query': query,
            'suggestions': []
        }
        
        try:
            if explain:
                # Get execution plan
                plan = conn.execute(f"EXPLAIN {query}").fetchall()
                result['execution_plan'] = [row[0] for row in plan]
                
                # Analyze plan for optimization opportunities
                plan_text = ' '.join(result['execution_plan'])
                
                # Check for common issues
                if 'SEQ_SCAN' in plan_text and 'FILTER' in plan_text:
                    result['suggestions'].append(
                        "Consider adding indexes on filtered columns"
                    )
                
                if 'HASH_JOIN' in plan_text:
                    result['suggestions'].append(
                        "Hash joins detected - ensure sufficient memory"
                    )
                
                if 'SORT' in plan_text:
                    result['suggestions'].append(
                        "Sorting detected - consider pre-sorted data or indexes"
                    )
            
            # Get query timing
            import time
            start = time.time()
            conn.execute(query).fetchall()
            result['execution_time_seconds'] = time.time() - start
            
        except Exception as e:
            LOG.error(f"Error optimizing query: {e}")
            result['error'] = str(e)
        
        return result
    
    @staticmethod
    def aggregate_window(
        table: str,
        partition_by: List[str],
        order_by: List[str],
        aggregations: Dict[str, str],
        database: str = ':memory:'
    ) -> pd.DataFrame:
        """
        Perform window aggregations on data.
        
        Args:
            table: Table name
            partition_by: Columns to partition by
            order_by: Columns to order by
            aggregations: Dict of {new_column: aggregation_expression}
            database: Database to use
            
        Returns:
            DataFrame with window aggregations
        """
        conn = DuckDBSession.get_or_create({'database': database})
        
        # Build window clause
        window_clause = "OVER ("
        if partition_by:
            window_clause += f"PARTITION BY {', '.join(partition_by)} "
        if order_by:
            window_clause += f"ORDER BY {', '.join(order_by)}"
        window_clause += ")"
        
        # Build select clause
        select_parts = ["*"]
        for col_name, agg_expr in aggregations.items():
            select_parts.append(f"{agg_expr} {window_clause} AS {col_name}")
        
        query = f"SELECT {', '.join(select_parts)} FROM {table}"
        
        try:
            result = conn.execute(query).fetchdf()
            LOG.info(f"Applied window aggregations to {table}")
            return result
        except Exception as e:
            LOG.error(f"Error in window aggregation: {e}")
            raise
    
    @staticmethod
    def pivot_data(
        table: str,
        index: List[str],
        columns: str,
        values: str,
        agg_func: str = 'SUM',
        database: str = ':memory:'
    ) -> pd.DataFrame:
        """
        Pivot data using DuckDB's PIVOT functionality.
        
        Args:
            table: Table name
            index: Columns to use as index
            columns: Column to pivot
            values: Column with values
            agg_func: Aggregation function
            database: Database to use
            
        Returns:
            Pivoted DataFrame
        """
        conn = DuckDBSession.get_or_create({'database': database})
        
        # Get unique values for pivot columns
        unique_vals = conn.execute(
            f"SELECT DISTINCT {columns} FROM {table} ORDER BY {columns}"
        ).fetchall()
        
        # Build pivot query
        pivot_cols = []
        for val in unique_vals:
            col_val = val[0]
            safe_name = str(col_val).replace(' ', '_').replace('-', '_')
            pivot_cols.append(
                f"{agg_func}(CASE WHEN {columns} = '{col_val}' THEN {values} END) AS {safe_name}"
            )
        
        query = f"""
        SELECT 
            {', '.join(index)},
            {', '.join(pivot_cols)}
        FROM {table}
        GROUP BY {', '.join(index)}
        ORDER BY {', '.join(index)}
        """
        
        try:
            result = conn.execute(query).fetchdf()
            LOG.info(f"Pivoted data from {table}")
            return result
        except Exception as e:
            LOG.error(f"Error pivoting data: {e}")
            raise
    
    @staticmethod
    def sample_data(
        table_or_query: str,
        n: Optional[int] = None,
        fraction: Optional[float] = None,
        method: str = 'random',
        seed: Optional[int] = None,
        database: str = ':memory:'
    ) -> pd.DataFrame:
        """
        Sample data from table or query.
        
        Args:
            table_or_query: Table name or SQL query
            n: Number of rows to sample
            fraction: Fraction of data to sample (0-1)
            method: Sampling method (random, systematic, stratified)
            seed: Random seed for reproducibility
            database: Database to use
            
        Returns:
            Sampled DataFrame
        """
        conn = DuckDBSession.get_or_create({'database': database})
        
        # Determine if input is table or query
        if ' ' in table_or_query or '(' in table_or_query:
            source = f"({table_or_query}) as data"
        else:
            source = table_or_query
        
        # Build sampling query
        if method == 'random':
            if seed:
                conn.execute(f"SET random_seed = {seed}")
            
            if n:
                query = f"SELECT * FROM {source} USING SAMPLE {n}"
            elif fraction:
                query = f"SELECT * FROM {source} USING SAMPLE {int(fraction * 100)} PERCENT"
            else:
                query = f"SELECT * FROM {source} USING SAMPLE 1000"  # Default
        
        elif method == 'systematic':
            # Systematic sampling - every nth row
            if n:
                query = f"""
                WITH numbered AS (
                    SELECT *, ROW_NUMBER() OVER () as rn
                    FROM {source}
                )
                SELECT * FROM numbered
                WHERE MOD(rn, (SELECT COUNT(*) / {n} FROM {source})) = 0
                """
            else:
                raise ValueError("Systematic sampling requires 'n' parameter")
        
        else:
            raise ValueError(f"Unsupported sampling method: {method}")
        
        try:
            result = conn.execute(query).fetchdf()
            LOG.info(f"Sampled {len(result)} rows from {table_or_query}")
            return result
        except Exception as e:
            LOG.error(f"Error sampling data: {e}")
            raise
    
    @staticmethod
    def merge_tables(
        left_table: str,
        right_table: str,
        on: Union[str, List[str]],
        how: str = 'inner',
        database: str = ':memory:',
        output_table: Optional[str] = None
    ) -> Union[pd.DataFrame, str]:
        """
        Merge two tables using SQL JOIN.
        
        Args:
            left_table: Left table name
            right_table: Right table name
            on: Column(s) to join on
            how: Join type (inner, left, right, full)
            database: Database to use
            output_table: Optional output table name
            
        Returns:
            Merged DataFrame or output table name
        """
        conn = DuckDBSession.get_or_create({'database': database})
        
        # Handle join columns
        if isinstance(on, str):
            join_condition = f"l.{on} = r.{on}"
        else:
            conditions = [f"l.{col} = r.{col}" for col in on]
            join_condition = " AND ".join(conditions)
        
        # Map join type
        join_map = {
            'inner': 'INNER JOIN',
            'left': 'LEFT JOIN',
            'right': 'RIGHT JOIN',
            'full': 'FULL OUTER JOIN'
        }
        join_type = join_map.get(how, 'INNER JOIN')
        
        query = f"""
        SELECT l.*, r.*
        FROM {left_table} l
        {join_type} {right_table} r
        ON {join_condition}
        """
        
        try:
            if output_table:
                conn.execute(f"CREATE TABLE {output_table} AS {query}")
                LOG.info(f"Merged tables into {output_table}")
                return output_table
            else:
                result = conn.execute(query).fetchdf()
                LOG.info(f"Merged {left_table} and {right_table}")
                return result
        except Exception as e:
            LOG.error(f"Error merging tables: {e}")
            raise
    
    @staticmethod
    def detect_outliers(
        table: str,
        column: str,
        method: str = 'iqr',
        threshold: float = 1.5,
        database: str = ':memory:'
    ) -> pd.DataFrame:
        """
        Detect outliers in numeric columns.
        
        Args:
            table: Table name
            column: Column to analyze
            method: Detection method (iqr, zscore, isolation)
            threshold: Threshold for outlier detection
            database: Database to use
            
        Returns:
            DataFrame with outlier flags
        """
        conn = DuckDBSession.get_or_create({'database': database})
        
        if method == 'iqr':
            # Interquartile range method
            query = f"""
            WITH stats AS (
                SELECT 
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {column}) AS q1,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {column}) AS q3
                FROM {table}
            ),
            iqr_calc AS (
                SELECT 
                    q1,
                    q3,
                    q3 - q1 AS iqr,
                    q1 - {threshold} * (q3 - q1) AS lower_bound,
                    q3 + {threshold} * (q3 - q1) AS upper_bound
                FROM stats
            )
            SELECT 
                t.*,
                CASE 
                    WHEN t.{column} < i.lower_bound OR t.{column} > i.upper_bound 
                    THEN true 
                    ELSE false 
                END AS is_outlier,
                CASE
                    WHEN t.{column} < i.lower_bound THEN 'below'
                    WHEN t.{column} > i.upper_bound THEN 'above'
                    ELSE 'normal'
                END AS outlier_type
            FROM {table} t
            CROSS JOIN iqr_calc i
            """
        
        elif method == 'zscore':
            # Z-score method
            query = f"""
            WITH stats AS (
                SELECT 
                    AVG({column}) AS mean,
                    STDDEV({column}) AS std
                FROM {table}
            )
            SELECT 
                t.*,
                ABS((t.{column} - s.mean) / s.std) AS zscore,
                CASE 
                    WHEN ABS((t.{column} - s.mean) / s.std) > {threshold}
                    THEN true 
                    ELSE false 
                END AS is_outlier
            FROM {table} t
            CROSS JOIN stats s
            """
        
        else:
            raise ValueError(f"Unsupported outlier detection method: {method}")
        
        try:
            result = conn.execute(query).fetchdf()
            outlier_count = result['is_outlier'].sum()
            LOG.info(f"Detected {outlier_count} outliers in {column} using {method} method")
            return result
        except Exception as e:
            LOG.error(f"Error detecting outliers: {e}")
            raise
    
    @staticmethod
    def time_series_resample(
        table: str,
        date_column: str,
        value_columns: List[str],
        frequency: str,
        agg_func: str = 'AVG',
        database: str = ':memory:'
    ) -> pd.DataFrame:
        """
        Resample time series data to different frequency.
        
        Args:
            table: Table name
            date_column: Date/timestamp column
            value_columns: Columns to aggregate
            frequency: Resampling frequency (day, week, month, quarter, year)
            agg_func: Aggregation function
            database: Database to use
            
        Returns:
            Resampled DataFrame
        """
        conn = DuckDBSession.get_or_create({'database': database})
        
        # Map frequency to DuckDB date truncation
        freq_map = {
            'day': 'day',
            'week': 'week',
            'month': 'month',
            'quarter': 'quarter',
            'year': 'year',
            'hour': 'hour',
            'minute': 'minute'
        }
        
        date_trunc = freq_map.get(frequency.lower(), 'day')
        
        # Build aggregation expressions
        agg_exprs = [f"{agg_func}({col}) AS {col}" for col in value_columns]
        
        query = f"""
        SELECT 
            DATE_TRUNC('{date_trunc}', {date_column}) AS period,
            {', '.join(agg_exprs)}
        FROM {table}
        GROUP BY DATE_TRUNC('{date_trunc}', {date_column})
        ORDER BY period
        """
        
        try:
            result = conn.execute(query).fetchdf()
            LOG.info(f"Resampled time series to {frequency} frequency")
            return result
        except Exception as e:
            LOG.error(f"Error resampling time series: {e}")
            raise