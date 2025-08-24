#!/usr/bin/env python3
"""
Spark-based log analysis workflow.

Demonstrates using Apache Spark to process large log files locally.
This example shows how to leverage Spark's distributed processing capabilities
even on a single machine for handling files too large for pandas.
"""

from pathlib import Path
import sys
import logging
from datetime import datetime
import re

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from airpipe.core.task import TaskPipeline
from airpipe.utils.spark import (
    SparkSessionManager,
    read_csv,
    write_parquet,
    write_csv,
    execute_sql,
    create_temp_view
)
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Setup logging
LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create pipeline
pipeline = TaskPipeline("spark_log_analysis")

# Spark configuration for local processing
SPARK_CONFIG = {
    'app_name': 'Log Analysis Pipeline',
    'master': 'local[*]',  # Use all available cores
    'config': {
        'spark.driver.memory': '4g',
        'spark.sql.shuffle.partitions': '50',  # Reduced for local
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true'
    }
}


@pipeline.task(produces="raw_logs")
def extract_logs():
    """Extract log data using Spark for large files."""
    LOG.info("Initializing Spark session...")
    
    # Get or create Spark session
    spark = SparkSessionManager.get_or_create(SPARK_CONFIG)
    
    # For demo, we'll create sample log data
    # In production, this would read from actual log files
    LOG.info("Generating sample log data...")
    
    # Sample log data schema
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("level", StringType(), True),
        StructField("source", StringType(), True),
        StructField("message", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("response_time_ms", IntegerType(), True),
        StructField("status_code", IntegerType(), True)
    ])
    
    # Generate sample data
    from datetime import datetime, timedelta
    import random
    
    # Create sample log entries
    log_entries = []
    levels = ["INFO", "WARNING", "ERROR", "DEBUG"]
    sources = ["web-server", "api-gateway", "database", "cache", "auth-service"]
    ips = [f"192.168.1.{i}" for i in range(1, 50)]
    users = [f"user_{i:04d}" for i in range(1, 1000)]
    
    # Generate 100K log entries (simulating large log file)
    base_time = datetime.now() - timedelta(hours=24)
    for i in range(100000):
        timestamp = (base_time + timedelta(seconds=i * 0.864)).isoformat()
        level = random.choice(levels)
        source = random.choice(sources)
        ip = random.choice(ips)
        user = random.choice(users) if random.random() > 0.1 else None
        
        # Response time - errors tend to be slower
        if level == "ERROR":
            response_time = random.randint(1000, 10000)
            status_code = random.choice([500, 502, 503, 504])
            message = f"Error processing request: {random.choice(['timeout', 'connection failed', 'invalid data'])}"
        else:
            response_time = random.randint(10, 1000)
            status_code = random.choice([200, 201, 204, 301, 302, 400, 401, 404])
            message = f"Request processed successfully"
        
        log_entries.append((
            timestamp, level, source, message, 
            ip, user, response_time, status_code
        ))
    
    # Create Spark DataFrame
    logs_df = spark.createDataFrame(log_entries, schema=schema)
    
    # Parse timestamp to proper type
    logs_df = logs_df.withColumn(
        "timestamp", 
        F.to_timestamp("timestamp")
    )
    
    LOG.info(f"Loaded {logs_df.count()} log entries")
    
    # Create artifact with Spark DataFrame
    return pipeline.create_artifact(logs_df, "raw_logs")


@pipeline.task(
    depends_on=["extract_logs"],
    consumes="raw_logs",
    produces="parsed_logs"
)
def parse_and_enrich():
    """Parse and enrich log data with additional fields."""
    LOG.info("Parsing and enriching logs...")
    
    # Get Spark DataFrame from artifact
    logs_artifact = pipeline.get_artifact("raw_logs")
    logs_df = logs_artifact.as_spark_dataframe()
    
    # Add derived fields
    enriched_df = logs_df \
        .withColumn("hour", F.hour("timestamp")) \
        .withColumn("date", F.to_date("timestamp")) \
        .withColumn("is_error", F.when(F.col("level") == "ERROR", 1).otherwise(0)) \
        .withColumn("is_slow", F.when(F.col("response_time_ms") > 1000, 1).otherwise(0)) \
        .withColumn("is_success", F.when(F.col("status_code") < 400, 1).otherwise(0))
    
    # Extract IP subnet for grouping
    enriched_df = enriched_df.withColumn(
        "ip_subnet",
        F.regexp_extract("ip_address", r"(\d+\.\d+\.\d+)\.\d+", 1)
    )
    
    # Categorize response times
    enriched_df = enriched_df.withColumn(
        "response_category",
        F.when(F.col("response_time_ms") < 100, "fast")
        .when(F.col("response_time_ms") < 500, "normal")
        .when(F.col("response_time_ms") < 1000, "slow")
        .otherwise("very_slow")
    )
    
    LOG.info(f"Enriched {enriched_df.count()} log entries")
    
    return pipeline.create_artifact(enriched_df, "parsed_logs")


@pipeline.task(
    depends_on=["parse_and_enrich"],
    consumes="parsed_logs",
    produces="hourly_metrics"
)
def aggregate_hourly_metrics():
    """Aggregate metrics by hour using Spark SQL."""
    LOG.info("Calculating hourly metrics...")
    
    # Get Spark DataFrame
    logs_artifact = pipeline.get_artifact("parsed_logs")
    logs_df = logs_artifact.as_spark_dataframe()
    
    # Register as temp view for SQL
    create_temp_view(logs_df, "logs")
    
    # Use Spark SQL for complex aggregations
    hourly_sql = """
    SELECT 
        date,
        hour,
        source,
        COUNT(*) as total_requests,
        SUM(is_error) as error_count,
        SUM(is_success) as success_count,
        AVG(response_time_ms) as avg_response_time,
        PERCENTILE_APPROX(response_time_ms, 0.5) as median_response_time,
        PERCENTILE_APPROX(response_time_ms, 0.95) as p95_response_time,
        PERCENTILE_APPROX(response_time_ms, 0.99) as p99_response_time,
        MAX(response_time_ms) as max_response_time,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT ip_address) as unique_ips,
        SUM(is_slow) as slow_requests,
        ROUND(100.0 * SUM(is_error) / COUNT(*), 2) as error_rate,
        ROUND(100.0 * SUM(is_success) / COUNT(*), 2) as success_rate
    FROM logs
    GROUP BY date, hour, source
    ORDER BY date, hour, source
    """
    
    hourly_df = execute_sql(hourly_sql)
    
    # Cache for reuse
    hourly_df.cache()
    
    LOG.info(f"Generated {hourly_df.count()} hourly metric records")
    
    return pipeline.create_artifact(hourly_df, "hourly_metrics")


@pipeline.task(
    depends_on=["parse_and_enrich"],
    consumes="parsed_logs",
    produces="error_analysis"
)
def analyze_errors():
    """Analyze error patterns in the logs."""
    LOG.info("Analyzing error patterns...")
    
    # Get Spark DataFrame
    logs_artifact = pipeline.get_artifact("parsed_logs")
    logs_df = logs_artifact.as_spark_dataframe()
    
    # Filter errors only
    errors_df = logs_df.filter(F.col("level") == "ERROR")
    
    # Analyze error patterns
    error_analysis = errors_df.groupBy("source", "status_code", "hour") \
        .agg(
            F.count("*").alias("error_count"),
            F.avg("response_time_ms").alias("avg_error_response_time"),
            F.collect_set("ip_address").alias("affected_ips"),
            F.collect_set("user_id").alias("affected_users")
        ) \
        .withColumn("num_affected_ips", F.size("affected_ips")) \
        .withColumn("num_affected_users", F.size("affected_users")) \
        .drop("affected_ips", "affected_users")
    
    # Find error spikes (hours with unusually high errors)
    window_spec = Window.partitionBy("source").orderBy("hour")
    error_analysis = error_analysis.withColumn(
        "rolling_avg_errors",
        F.avg("error_count").over(window_spec.rowsBetween(-3, 3))
    )
    
    error_analysis = error_analysis.withColumn(
        "is_spike",
        F.when(F.col("error_count") > F.col("rolling_avg_errors") * 2, 1).otherwise(0)
    )
    
    LOG.info(f"Analyzed {error_analysis.count()} error patterns")
    
    return pipeline.create_artifact(error_analysis, "error_analysis")


@pipeline.task(
    depends_on=["parse_and_enrich"],
    consumes="parsed_logs", 
    produces="user_behavior"
)
def analyze_user_behavior():
    """Analyze user behavior patterns."""
    LOG.info("Analyzing user behavior...")
    
    # Get Spark DataFrame
    logs_artifact = pipeline.get_artifact("parsed_logs")
    logs_df = logs_artifact.as_spark_dataframe()
    
    # Filter to logged-in users only
    user_logs = logs_df.filter(F.col("user_id").isNotNull())
    
    # User behavior analysis
    user_stats = user_logs.groupBy("user_id") \
        .agg(
            F.count("*").alias("total_requests"),
            F.countDistinct("date").alias("active_days"),
            F.countDistinct("hour").alias("active_hours"),
            F.avg("response_time_ms").alias("avg_response_time"),
            F.sum("is_error").alias("error_count"),
            F.countDistinct("source").alias("services_used"),
            F.min("timestamp").alias("first_seen"),
            F.max("timestamp").alias("last_seen")
        )
    
    # Categorize users based on activity
    user_stats = user_stats.withColumn(
        "user_category",
        F.when(F.col("total_requests") > 1000, "power_user")
        .when(F.col("total_requests") > 100, "regular_user")
        .otherwise("light_user")
    )
    
    LOG.info(f"Analyzed behavior for {user_stats.count()} users")
    
    return pipeline.create_artifact(user_stats, "user_behavior")


@pipeline.task(
    depends_on=["hourly_metrics", "error_analysis", "user_behavior"],
    consumes=["hourly_metrics", "error_analysis", "user_behavior"],
    produces="summary_report"
)
def generate_summary_report():
    """Generate summary report combining all analyses."""
    LOG.info("Generating summary report...")
    
    # Get all analysis DataFrames
    hourly_df = pipeline.get_artifact("hourly_metrics").as_spark_dataframe()
    error_df = pipeline.get_artifact("error_analysis").as_spark_dataframe()
    user_df = pipeline.get_artifact("user_behavior").as_spark_dataframe()
    
    # Overall statistics
    total_requests = hourly_df.agg(F.sum("total_requests")).collect()[0][0]
    total_errors = hourly_df.agg(F.sum("error_count")).collect()[0][0]
    avg_response = hourly_df.agg(F.avg("avg_response_time")).collect()[0][0]
    
    # Error spike detection
    error_spikes = error_df.filter(F.col("is_spike") == 1).count()
    
    # User statistics
    user_categories = user_df.groupBy("user_category").count().collect()
    
    # Create summary dictionary
    summary = {
        "analysis_timestamp": datetime.now().isoformat(),
        "total_requests": int(total_requests),
        "total_errors": int(total_errors),
        "error_rate": round(100.0 * total_errors / total_requests, 2),
        "avg_response_time_ms": round(avg_response, 2),
        "error_spikes_detected": int(error_spikes),
        "unique_users": int(user_df.count()),
        "user_categories": {row["user_category"]: row["count"] for row in user_categories}
    }
    
    LOG.info(f"Summary Report:")
    LOG.info(f"  Total Requests: {summary['total_requests']:,}")
    LOG.info(f"  Error Rate: {summary['error_rate']}%")
    LOG.info(f"  Avg Response Time: {summary['avg_response_time_ms']}ms")
    LOG.info(f"  Error Spikes: {summary['error_spikes_detected']}")
    
    return pipeline.create_artifact(summary, "summary_report")


@pipeline.task(
    depends_on=["generate_summary_report"],
    consumes=["hourly_metrics", "error_analysis", "user_behavior", "summary_report"]
)
def save_results():
    """Save analysis results to files."""
    LOG.info("Saving analysis results...")
    
    output_dir = Path("output/spark_log_analysis")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save hourly metrics as Parquet (efficient for Spark)
    hourly_df = pipeline.get_artifact("hourly_metrics").as_spark_dataframe()
    write_parquet(
        hourly_df, 
        str(output_dir / "hourly_metrics.parquet"),
        num_partitions=1  # Single file for demo
    )
    LOG.info(f"Saved hourly metrics to {output_dir}/hourly_metrics.parquet")
    
    # Save error analysis as CSV for easy viewing
    error_df = pipeline.get_artifact("error_analysis").as_spark_dataframe()
    write_csv(
        error_df,
        str(output_dir / "error_analysis.csv"),
        num_partitions=1
    )
    LOG.info(f"Saved error analysis to {output_dir}/error_analysis.csv")
    
    # Save user behavior as Parquet
    user_df = pipeline.get_artifact("user_behavior").as_spark_dataframe()
    write_parquet(
        user_df,
        str(output_dir / "user_behavior.parquet"),
        num_partitions=1
    )
    LOG.info(f"Saved user behavior to {output_dir}/user_behavior.parquet")
    
    # Save summary as JSON
    import json
    summary = pipeline.get_artifact("summary_report").data
    with open(output_dir / "summary_report.json", "w") as f:
        json.dump(summary, f, indent=2)
    LOG.info(f"Saved summary report to {output_dir}/summary_report.json")
    
    LOG.info("All results saved successfully!")


def run():
    """Run the Spark log analysis pipeline."""
    LOG.info("=" * 60)
    LOG.info("SPARK LOG ANALYSIS PIPELINE")
    LOG.info("=" * 60)
    
    # Execute all tasks
    extract_logs()
    parse_and_enrich()
    aggregate_hourly_metrics()
    analyze_errors()
    analyze_user_behavior()
    generate_summary_report()
    save_results()
    
    # Execute pipeline
    results = pipeline.execute()
    
    # Stop Spark session when done
    SparkSessionManager.stop()
    
    return results


def main():
    """Main entry point."""
    try:
        results = run()
        LOG.info("\n" + "=" * 60)
        LOG.info("Pipeline completed successfully!")
        LOG.info(f"Tasks executed: {results.get('tasks_executed', 0)}")
        LOG.info(f"Artifacts created: {results.get('artifacts_created', 0)}")
        LOG.info("=" * 60)
    except Exception as e:
        LOG.error(f"Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        # Make sure to stop Spark even on error
        SparkSessionManager.stop()
        sys.exit(1)


if __name__ == "__main__":
    # Add missing import for Window
    from pyspark.sql.window import Window
    main()