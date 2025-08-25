"""
DuckDB Analytics Workflow.

Demonstrates how to use DuckDB integration with AirPipe for
high-performance analytical pipelines. This workflow showcases:
- SQL-enabled pipeline execution
- High-performance analytical operations
- DuckDB artifact management
- Complex SQL queries and data profiling
"""

from pathlib import Path
import sys
# Add both the project root and the pipelines directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

from airpipe.utils.duckdb import (
    DuckDBSession,
    DuckDBOperations,
    DuckDBArtifact,
    SQLPipeline
)

# Import pipeline components
from pipelines.duckdb.extractors.sample_data_extractor import SalesDataExtractor
from pipelines.duckdb.transformers.filter_transformer import FilterTransformer
from pipelines.duckdb.transformers.aggregation_transformer import AggregationTransformer
from pipelines.duckdb.transformers.analytics_transformer import AnalyticsTransformer
from pipelines.duckdb.transformers.profile_transformer import ProfileTransformer
from pipelines.duckdb.transformers.pivot_transformer import PivotTransformer
from pipelines.duckdb.loaders.analytics_loader import AnalyticsLoader

# Setup logging
LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Initialize SQL-enabled pipeline
pipeline = SQLPipeline("duckdb_analytics", database=':memory:')

# Initialize pipeline components
extractor = SalesDataExtractor(seed=42)
filter_transformer = FilterTransformer()
aggregation_transformer = AggregationTransformer()
analytics_transformer = AnalyticsTransformer()
profile_transformer = ProfileTransformer()
pivot_transformer = PivotTransformer()
loader = AnalyticsLoader("output")


# ============================================================================
# DATA GENERATION TASKS - Create sample datasets
# ============================================================================

# Example 1: Data extraction using organized extractor
@pipeline.task(produces="sales_data")
def generate_sales_data():
    """Generate sample sales data using SalesDataExtractor."""
    LOG.info("Generating sample sales data using extractor...")
    
    # Use the organized extractor
    artifact = extractor.extract_sales_data(n_records=10000)
    
    return artifact


# ============================================================================
# SQL TRANSFORMATION TASKS - High-performance data processing
# ============================================================================

# Example 2: Filtering using organized transformer
@pipeline.task(
    depends_on=["generate_sales_data"],
    consumes="sales_data",
    produces="high_value_sales"
)
def filter_high_value_sales():
    """Filter high-value sales using FilterTransformer."""
    LOG.info("Filtering high-value sales using transformer...")
    
    # Get input artifact
    sales_artifact = pipeline.get_artifact("sales_data")
    
    # Use the organized transformer
    filtered_artifact = filter_transformer.filter_high_value_sales(
        sales_artifact=sales_artifact,
        threshold=500.0,
        regions=['North', 'East']
    )
    
    return filtered_artifact


# Example 3: Aggregation using organized transformer
@pipeline.task(
    depends_on=["filter_high_value_sales"],
    consumes="high_value_sales",
    produces="monthly_summary"
)
def aggregate_monthly():
    """Aggregate sales by month using AggregationTransformer."""
    LOG.info("Aggregating monthly sales using transformer...")
    
    # Get input artifact
    high_value_artifact = pipeline.get_artifact("high_value_sales")
    
    # Use the organized transformer
    monthly_artifact = aggregation_transformer.aggregate_monthly_sales(
        sales_artifact=high_value_artifact
    )
    
    return monthly_artifact


# ============================================================================
# ADVANCED ANALYTICS TASKS - Complex data operations
# ============================================================================

# Example 4: Pivot operations using organized transformer
@pipeline.task(
    depends_on=["aggregate_monthly"],
    consumes="monthly_summary",
    produces="monthly_pivot"
)
def create_pivot_report():
    """Create pivot report using PivotTransformer."""
    LOG.info("Creating pivot report using transformer...")
    
    # Get input artifact
    monthly_summary_artifact = pipeline.get_artifact("monthly_summary")
    
    # Use the organized transformer
    pivot_artifact = pivot_transformer.create_monthly_revenue_pivot(
        monthly_summary_artifact=monthly_summary_artifact
    )
    
    return pivot_artifact


# Example 5: Customer analytics using organized transformer
@pipeline.task(
    depends_on=["generate_sales_data", "filter_high_value_sales"],
    consumes=["sales_data", "high_value_sales"],
    produces="customer_analysis"
)
def analyze_customers():
    """Analyze customer behavior using AnalyticsTransformer."""
    LOG.info("Analyzing customer segments using transformer...")
    
    # Get input artifacts
    sales_artifact = pipeline.get_artifact("sales_data")
    high_value_artifact = pipeline.get_artifact("high_value_sales")
    
    # Use the organized transformer
    customer_analysis_artifact = analytics_transformer.analyze_customer_segments(
        sales_artifact=sales_artifact,
        high_value_artifact=high_value_artifact
    )
    
    return customer_analysis_artifact


# ============================================================================
# DATA PROFILING AND QUALITY TASKS - Understand data characteristics
# ============================================================================

# Example 6: Data profiling using organized transformer
@pipeline.task(
    depends_on=["analyze_customers"],
    consumes="customer_analysis"
)
def profile_customer_data():
    """Profile customer analysis data using ProfileTransformer."""
    LOG.info("Profiling customer data using transformer...")
    
    # Get input artifact
    customer_artifact = pipeline.get_artifact("customer_analysis")
    
    # Use the organized transformer
    profile = profile_transformer.profile_customer_data(
        customer_artifact=customer_artifact,
        print_summary=True
    )
    
    LOG.info("Profiling complete")
    return profile


# ============================================================================
# EXPORT TASKS - Save results to files
# ============================================================================

# Example 7: Export results using organized loader
@pipeline.task(
    depends_on=["create_pivot_report", "analyze_customers"],
    consumes=["monthly_pivot", "customer_analysis"]
)
def export_results():
    """Export analysis results using AnalyticsLoader."""
    LOG.info("Exporting results using loader...")
    
    # Get artifacts to export
    monthly_pivot_artifact = pipeline.get_artifact("monthly_pivot")
    customer_analysis_artifact = pipeline.get_artifact("customer_analysis")
    
    # Use the organized loader to export
    pivot_export = loader.export_pivot_reports(
        monthly_pivot_artifact=monthly_pivot_artifact,
        output_format="parquet"
    )
    
    customer_export = loader.export_customer_analysis(
        customer_analysis_artifact=customer_analysis_artifact,
        export_segments=True
    )
    
    # Create comprehensive report
    all_artifacts = [
        monthly_pivot_artifact,
        customer_analysis_artifact,
        pipeline.get_artifact("sales_data"),
        pipeline.get_artifact("high_value_sales"),
        pipeline.get_artifact("monthly_summary")
    ]
    
    report = loader.create_analytics_report(
        artifacts=all_artifacts,
        pipeline_name="duckdb_analytics"
    )
    
    LOG.info("Export complete using organized loader")
    return report


def run():
    """
    Execute the DuckDB analytics workflow.
    
    The pipeline automatically:
    1. Generates sample sales data using traditional Python tasks
    2. Processes data using high-performance SQL operations
    3. Performs advanced analytics and customer segmentation
    4. Profiles data quality and exports results
    """
    LOG.info("Starting DuckDB analytics workflow")
    LOG.info("This workflow demonstrates high-performance SQL operations")
    
    # Execute pipeline with parallel processing
    results = pipeline.execute(parallel=True, max_workers=4)
    
    LOG.info(f"\nWorkflow complete!")
    LOG.info(f"Tasks executed: {results['tasks_executed']}")
    LOG.info(f"Artifacts created: {results['artifacts_created']}")
    LOG.info(f"Artifacts: {', '.join(results['artifacts'])}")
    
    # Show DuckDB session info
    config = DuckDBSession.get_config()
    LOG.info(f"\nDuckDB Configuration:")
    LOG.info(f"  - Memory Limit: {config.get('memory_limit', 'default')}")
    LOG.info(f"  - Threads: {config.get('threads', 'default')}")
    
    return results


def visualize():
    """
    Visualize the DuckDB workflow DAG using different formats.
    """
    # ASCII visualization
    print("DuckDB Analytics Pipeline DAG:")
    print("=" * 60)
    print(pipeline.visualize_dag(format='ascii'))
    
    # Save as Mermaid diagram
    mermaid_output = pipeline.visualize_dag(format='mermaid', output_file='output/duckdb_workflow_dag.md')
    
    # Get pipeline statistics
    stats = pipeline.get_task_statistics()
    print("\n" + "=" * 60)
    print("Pipeline Statistics:")
    print("=" * 60)
    print(f"Total tasks: {stats['total_tasks']}")
    print(f"Total dependencies: {stats['total_dependencies']}")
    print(f"Task types: {stats['task_types']}")
    print(f"Root tasks: {stats['root_tasks']}")
    print(f"Critical path length: {stats['critical_path_length']}")
    print(f"Average dependencies per task: {stats['average_dependencies']:.2f}")
    
    # Count SQL tasks
    sql_tasks = sum(1 for t in pipeline.tasks.values() if hasattr(t, '_is_sql_task'))
    print(f"SQL tasks: {sql_tasks}")
    
    # Validate DAG
    try:
        pipeline.validate_dag()
        print("\n✓ DAG validation passed - no cycles detected")
    except RuntimeError as e:
        print(f"\n✗ DAG validation failed: {e}")
    
    return mermaid_output


if __name__ == "__main__":
    import sys
    import os
    
    # Create output directory
    os.makedirs("output", exist_ok=True)
    
    # Check for visualization flag
    if len(sys.argv) > 1 and sys.argv[1] in ['--visualize', '-v']:
        # Just visualize without running
        visualize()
    else:
        # Visualize and then run
        print("Visualizing DuckDB workflow DAG...")
        visualize()
        
        print("\n" + "=" * 70)
        print(" " * 20 + "EXECUTING DUCKDB ANALYTICS PIPELINE")
        print("=" * 70)
        
        # Run the workflow
        run()
        
        # Clean up DuckDB session
        pipeline.cleanup()