"""
Advanced task-based workflow with custom logic.

This demonstrates more complex patterns including:
- Multiple extractors
- Parallel transformers
- Conditional logic
- Error handling
"""

from pathlib import Path
import sys
# Add both the project root and the pipelines directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))
sys.path.append(str(Path(__file__).parent.parent))

import logging
from airpipe.core.task import TaskPipeline
from sales.extractors.sample_extractor import SalesDataExtractor
from sales.transformers.regional_transformer import RegionalTransformer
from sales.transformers.product_transformer import ProductTransformer
from sales.transformers.customer_transformer import CustomerTransformer
from sales.transformers.insights_transformer import InsightsTransformer
from sales.loaders.report_loader import ReportLoader

# Setup
LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Create pipeline and component instances
pipeline = TaskPipeline("advanced_analytics")
sales_extractor = SalesDataExtractor()
regional_transformer = RegionalTransformer()
product_transformer = ProductTransformer()
customer_transformer = CustomerTransformer()
insights_transformer = InsightsTransformer()
report_loader = ReportLoader()

@pipeline.task(produces="sales_data")
def extract_sales_data():
    """Extract sales data."""
    data = sales_extractor.extract_sales_data(n_records=1000)
    return pipeline.create_artifact(data, "sales_data")

@pipeline.task(produces="customer_data")
def extract_customer_data():
    """Extract customer data."""
    data = sales_extractor.extract_customer_data(n_customers=200)
    return pipeline.create_artifact(data, "customer_data")

@pipeline.task(
    depends_on=["extract_sales_data"],
    consumes="sales_data",
    produces="regional_metrics"
)
def calculate_regional_metrics():
    """Calculate metrics by region."""
    sales_data = pipeline.get_artifact("sales_data")
    df = sales_data.as_dataframe()
    regional = regional_transformer.calculate_regional_metrics(df)
    return pipeline.create_artifact(regional, "regional_metrics")

@pipeline.task(
    depends_on=["extract_sales_data"],
    consumes="sales_data",
    produces="top_products"
)
def identify_top_products():
    """Identify top selling products."""
    sales_data = pipeline.get_artifact("sales_data")
    df = sales_data.as_dataframe()
    products = product_transformer.identify_top_products(df)
    return pipeline.create_artifact(products, "top_products")

@pipeline.task(
    depends_on=["extract_sales_data", "extract_customer_data"],
    consumes=["sales_data", "customer_data"],
    produces="customer_segments"
)
def analyze_customer_segments():
    """Analyze customer segments."""
    sales_data = pipeline.get_artifact("sales_data")
    customer_data = pipeline.get_artifact("customer_data")
    sales_df = sales_data.as_dataframe()
    customer_df = customer_data.as_dataframe()
    segments = customer_transformer.analyze_customer_segments(sales_df, customer_df)
    return pipeline.create_artifact(segments, "customer_segments")

@pipeline.task(
    depends_on=["calculate_regional_metrics", "identify_top_products", "analyze_customer_segments"],
    consumes=["regional_metrics", "top_products", "customer_segments"],
    produces="business_insights"
)
def generate_insights():
    """Generate business insights from all analyses."""
    regional_metrics = pipeline.get_artifact("regional_metrics")
    top_products = pipeline.get_artifact("top_products")
    customer_segments = pipeline.get_artifact("customer_segments")
    
    regional_df = regional_metrics.as_dataframe()
    products_df = top_products.as_dataframe()
    segments_df = customer_segments.as_dataframe()
    
    insights_df = insights_transformer.generate_insights(regional_df, products_df, segments_df)
    return pipeline.create_artifact(insights_df, "business_insights")

@pipeline.task(
    depends_on=["generate_insights"],
    consumes=["regional_metrics", "top_products", "customer_segments", "business_insights"]
)
def save_all_results():
    """Save all analysis results."""
    artifacts = {
        'regional_metrics': pipeline.get_artifact("regional_metrics"),
        'top_products': pipeline.get_artifact("top_products"),
        'customer_segments': pipeline.get_artifact("customer_segments"),
        'business_insights': pipeline.get_artifact("business_insights")
    }
    report_loader.save_all_results(artifacts)

@pipeline.task(
    depends_on=["generate_insights"],
    consumes=["business_insights", "top_products", "regional_metrics"]
)
def print_executive_summary():
    """Print executive summary."""
    business_insights = pipeline.get_artifact("business_insights")
    top_products = pipeline.get_artifact("top_products")
    regional_metrics = pipeline.get_artifact("regional_metrics")
    
    insights_df = business_insights.as_dataframe()
    products_df = top_products.as_dataframe()
    regional_df = regional_metrics.as_dataframe()
    
    report_loader.print_executive_summary(insights_df, products_df, regional_df)

def run():
    """Execute the advanced workflow."""
    LOG.info("Starting advanced analytics workflow")
    
    # Execute pipeline - framework handles everything!
    results = pipeline.execute(parallel=True, max_workers=4)
    
    LOG.info(f"\nAdvanced workflow complete!")
    LOG.info(f"Executed {results['tasks_executed']} tasks")
    LOG.info(f"Created {results['artifacts_created']} artifacts")
    LOG.info(f"Artifacts: {', '.join(results['artifacts'])}")
    
    return results

if __name__ == "__main__":
    run()