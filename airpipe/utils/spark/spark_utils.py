"""
Spark DataFrame utilities for AirPipe.

Provides common operations and conversions for working with Spark DataFrames.
"""

import pandas as pd
from typing import Optional, List, Dict, Any, Union
import logging
from pathlib import Path

LOG = logging.getLogger(__name__)


def pandas_to_spark(df: pd.DataFrame, spark_session=None, schema=None):
    """
    Convert Pandas DataFrame to Spark DataFrame.
    
    Args:
        df: Pandas DataFrame to convert
        spark_session: Optional Spark session (uses SparkSessionManager if not provided)
        schema: Optional Spark schema for the DataFrame
        
    Returns:
        Spark DataFrame
    """
    if spark_session is None:
        from .spark_session import SparkSessionManager
        spark_session = SparkSessionManager.get_or_create()
    
    LOG.info(f"Converting Pandas DataFrame ({len(df)} rows) to Spark DataFrame")
    
    # Use Arrow optimization if available
    spark_df = spark_session.createDataFrame(df, schema=schema)
    
    return spark_df


def spark_to_pandas(spark_df, max_records: Optional[int] = None) -> pd.DataFrame:
    """
    Convert Spark DataFrame to Pandas DataFrame.
    
    Args:
        spark_df: Spark DataFrame to convert
        max_records: Optional limit on number of records to convert
        
    Returns:
        Pandas DataFrame
    """
    LOG.info("Converting Spark DataFrame to Pandas DataFrame")
    
    if max_records:
        spark_df = spark_df.limit(max_records)
        LOG.warning(f"Limited to {max_records} records for conversion")
    
    # Use Arrow optimization if available (configured in SparkSessionManager)
    pandas_df = spark_df.toPandas()
    
    LOG.info(f"Converted to Pandas DataFrame with {len(pandas_df)} rows")
    return pandas_df


def read_parquet(path: str, spark_session=None, columns: Optional[List[str]] = None):
    """
    Read Parquet file(s) into Spark DataFrame.
    
    Args:
        path: Path to Parquet file or directory
        spark_session: Optional Spark session
        columns: Optional list of columns to read
        
    Returns:
        Spark DataFrame
    """
    if spark_session is None:
        from .spark_session import SparkSessionManager
        spark_session = SparkSessionManager.get_or_create()
    
    LOG.info(f"Reading Parquet from: {path}")
    
    if columns:
        df = spark_session.read.parquet(path).select(*columns)
    else:
        df = spark_session.read.parquet(path)
    
    count = df.count()
    LOG.info(f"Read {count} records from Parquet")
    
    return df


def write_parquet(spark_df, path: str, mode: str = 'overwrite', 
                  partition_by: Optional[List[str]] = None,
                  num_partitions: Optional[int] = None):
    """
    Write Spark DataFrame to Parquet.
    
    Args:
        spark_df: Spark DataFrame to write
        path: Output path
        mode: Write mode ('overwrite', 'append', 'ignore', 'error')
        partition_by: Optional columns to partition by
        num_partitions: Optional number of partitions to coalesce to
        
    Returns:
        None
    """
    LOG.info(f"Writing Spark DataFrame to Parquet: {path}")
    
    # Optionally repartition
    if num_partitions:
        spark_df = spark_df.coalesce(num_partitions)
        LOG.info(f"Coalesced to {num_partitions} partitions")
    
    writer = spark_df.write.mode(mode)
    
    if partition_by:
        writer = writer.partitionBy(*partition_by)
        LOG.info(f"Partitioning by: {partition_by}")
    
    writer.parquet(path)
    LOG.info(f"Successfully wrote Parquet to: {path}")


def read_csv(path: str, spark_session=None, 
             header: bool = True, 
             infer_schema: bool = True,
             delimiter: str = ',',
             **options):
    """
    Read CSV file(s) into Spark DataFrame.
    
    Args:
        path: Path to CSV file or directory
        spark_session: Optional Spark session
        header: Whether CSV has header row
        infer_schema: Whether to infer schema automatically
        delimiter: CSV delimiter
        **options: Additional Spark CSV options
        
    Returns:
        Spark DataFrame
    """
    if spark_session is None:
        from .spark_session import SparkSessionManager
        spark_session = SparkSessionManager.get_or_create()
    
    LOG.info(f"Reading CSV from: {path}")
    
    reader = spark_session.read \
        .option("header", header) \
        .option("inferSchema", infer_schema) \
        .option("delimiter", delimiter)
    
    # Add any additional options
    for key, value in options.items():
        reader = reader.option(key, value)
    
    df = reader.csv(path)
    
    count = df.count()
    LOG.info(f"Read {count} records from CSV")
    
    return df


def write_csv(spark_df, path: str, 
              mode: str = 'overwrite',
              header: bool = True,
              delimiter: str = ',',
              num_partitions: Optional[int] = 1,
              **options):
    """
    Write Spark DataFrame to CSV.
    
    Args:
        spark_df: Spark DataFrame to write
        path: Output path
        mode: Write mode ('overwrite', 'append', 'ignore', 'error')
        header: Whether to write header row
        delimiter: CSV delimiter
        num_partitions: Number of partitions (files) to write
        **options: Additional Spark CSV options
        
    Returns:
        None
    """
    LOG.info(f"Writing Spark DataFrame to CSV: {path}")
    
    # Coalesce to control number of output files
    if num_partitions:
        spark_df = spark_df.coalesce(num_partitions)
        LOG.info(f"Coalesced to {num_partitions} partitions")
    
    writer = spark_df.write \
        .mode(mode) \
        .option("header", header) \
        .option("delimiter", delimiter)
    
    # Add any additional options
    for key, value in options.items():
        writer = writer.option(key, value)
    
    writer.csv(path)
    LOG.info(f"Successfully wrote CSV to: {path}")


def read_json(path: str, spark_session=None, multiline: bool = False, **options):
    """
    Read JSON file(s) into Spark DataFrame.
    
    Args:
        path: Path to JSON file or directory
        spark_session: Optional Spark session
        multiline: Whether JSON spans multiple lines
        **options: Additional Spark JSON options
        
    Returns:
        Spark DataFrame
    """
    if spark_session is None:
        from .spark_session import SparkSessionManager
        spark_session = SparkSessionManager.get_or_create()
    
    LOG.info(f"Reading JSON from: {path}")
    
    reader = spark_session.read.option("multiline", multiline)
    
    for key, value in options.items():
        reader = reader.option(key, value)
    
    df = reader.json(path)
    
    count = df.count()
    LOG.info(f"Read {count} records from JSON")
    
    return df


def optimize_dataframe(spark_df, cache: bool = False, 
                      repartition: Optional[int] = None,
                      broadcast_hint: bool = False):
    """
    Optimize Spark DataFrame for performance.
    
    Args:
        spark_df: Spark DataFrame to optimize
        cache: Whether to cache the DataFrame in memory
        repartition: Optional number of partitions to repartition to
        broadcast_hint: Whether to hint for broadcast join (for small DataFrames)
        
    Returns:
        Optimized Spark DataFrame
    """
    LOG.info("Optimizing Spark DataFrame")
    
    if repartition:
        spark_df = spark_df.repartition(repartition)
        LOG.info(f"Repartitioned to {repartition} partitions")
    
    if cache:
        spark_df = spark_df.cache()
        LOG.info("Cached DataFrame in memory")
    
    if broadcast_hint:
        from pyspark.sql.functions import broadcast
        spark_df = broadcast(spark_df)
        LOG.info("Added broadcast hint for joins")
    
    return spark_df


def get_dataframe_info(spark_df) -> Dict[str, Any]:
    """
    Get information about a Spark DataFrame.
    
    Args:
        spark_df: Spark DataFrame to analyze
        
    Returns:
        Dict with DataFrame information
    """
    info = {
        'columns': spark_df.columns,
        'schema': str(spark_df.schema),
        'num_partitions': spark_df.rdd.getNumPartitions(),
        'is_cached': spark_df.is_cached,
        'storage_level': str(spark_df.storageLevel),
    }
    
    # Count can be expensive, make it optional
    try:
        info['count'] = spark_df.count()
    except Exception as e:
        LOG.warning(f"Could not get count: {e}")
        info['count'] = None
    
    return info


def create_temp_view(spark_df, view_name: str):
    """
    Create a temporary view for SQL queries.
    
    Args:
        spark_df: Spark DataFrame
        view_name: Name for the temporary view
        
    Returns:
        None
    """
    spark_df.createOrReplaceTempView(view_name)
    LOG.info(f"Created temporary view: {view_name}")


def execute_sql(query: str, spark_session=None):
    """
    Execute SQL query on registered views/tables.
    
    Args:
        query: SQL query string
        spark_session: Optional Spark session
        
    Returns:
        Spark DataFrame with query results
    """
    if spark_session is None:
        from .spark_session import SparkSessionManager
        spark_session = SparkSessionManager.get_or_create()
    
    LOG.info(f"Executing SQL query: {query[:100]}...")
    result = spark_session.sql(query)
    
    return result


def sample_dataframe(spark_df, fraction: float = 0.01, seed: Optional[int] = None):
    """
    Sample a fraction of the DataFrame.
    
    Args:
        spark_df: Spark DataFrame to sample
        fraction: Fraction of rows to sample (0.0 to 1.0)
        seed: Optional random seed
        
    Returns:
        Sampled Spark DataFrame
    """
    LOG.info(f"Sampling {fraction*100}% of DataFrame")
    
    if seed:
        sampled = spark_df.sample(withReplacement=False, fraction=fraction, seed=seed)
    else:
        sampled = spark_df.sample(withReplacement=False, fraction=fraction)
    
    return sampled


def show_dataframe(spark_df, n: int = 20, truncate: bool = True):
    """
    Display DataFrame rows (for debugging).
    
    Args:
        spark_df: Spark DataFrame to display
        n: Number of rows to show
        truncate: Whether to truncate long strings
        
    Returns:
        None
    """
    LOG.info(f"Showing {n} rows of DataFrame")
    spark_df.show(n=n, truncate=truncate)


def get_distinct_values(spark_df, column: str) -> List:
    """
    Get distinct values from a column.
    
    Args:
        spark_df: Spark DataFrame
        column: Column name
        
    Returns:
        List of distinct values
    """
    LOG.info(f"Getting distinct values from column: {column}")
    
    distinct_values = spark_df.select(column).distinct().collect()
    values = [row[column] for row in distinct_values]
    
    LOG.info(f"Found {len(values)} distinct values")
    return values