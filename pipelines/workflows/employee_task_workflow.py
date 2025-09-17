"""
Employee data processing workflow using task-based approach.

This demonstrates clean separation where workflow handles orchestration
and business logic resides in extractors, transformers, and loaders.
"""

from pathlib import Path
import sys
# Add both the project root and the pipelines directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))
sys.path.append(str(Path(__file__).parent.parent))

import logging
from airpipe.core.task import TaskPipeline
from employee.extractors.csv_extractor import EmployeeCSVExtractor
from employee.transformers.salary_transformer import SalaryTransformer
from employee.transformers.department_transformer import EmployeeDepartmentTransformer
from employee.loaders.report_loader import EmployeeReportLoader

# Setup logging
LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create pipeline instance
pipeline = TaskPipeline("employee_analysis")

# Initialize components
csv_extractor = EmployeeCSVExtractor()
salary_transformer = SalaryTransformer()
dept_transformer = EmployeeDepartmentTransformer()
report_loader = EmployeeReportLoader()

@pipeline.task(produces="raw_employee_data")
def extractor():
    """Extract employee data from CSV file."""
    LOG.info("Extracting employee data")
    
    # Use extractor component
    df = csv_extractor.extract_current_employees("examples/sample_data.csv")
    
    return pipeline.create_artifact(df, "raw_employee_data")

@pipeline.task(
    depends_on=["extractor"],
    consumes="raw_employee_data",
    produces="high_earners"
)
def filter_high_earners():
    """Filter employees with high salaries."""
    LOG.info("Filtering high earners")
    
    raw_employee_data = pipeline.get_artifact("raw_employee_data")
    df = raw_employee_data.as_dataframe()
    
    # Use transformer component
    filtered = salary_transformer.filter_high_earners(df, threshold=70000)
    
    return pipeline.create_artifact(filtered, "high_earners")

@pipeline.task(
    depends_on=["extractor"],
    consumes="raw_employee_data",
    produces="department_stats"
)
def aggregate_by_department():
    """Calculate statistics by department."""
    LOG.info("Aggregating by department")
    
    raw_employee_data = pipeline.get_artifact("raw_employee_data")
    df = raw_employee_data.as_dataframe()
    
    # Use transformer component
    stats = dept_transformer.aggregate_by_department(df)
    
    return pipeline.create_artifact(stats, "department_stats")

@pipeline.task(
    depends_on=["filter_high_earners"],
    consumes="high_earners"
)
def save_high_earners_csv():
    """Save high earners to CSV file."""
    LOG.info("Saving high earners to CSV")
    
    high_earners = pipeline.get_artifact("high_earners")
    df = high_earners.as_dataframe()
    
    # Use loader component
    report_loader.save_high_earners(df)

@pipeline.task(
    depends_on=["aggregate_by_department"],
    consumes="department_stats"
)
def save_department_stats_json():
    """Save department statistics to JSON file."""
    LOG.info("Saving department stats to JSON")
    
    department_stats = pipeline.get_artifact("department_stats")
    df = department_stats.as_dataframe()
    
    # Use loader component
    report_loader.save_department_stats(df)

@pipeline.task(
    depends_on=["filter_high_earners", "aggregate_by_department"],
    consumes=["high_earners", "department_stats"]
)
def print_summary():
    """Print summary of the analysis."""
    LOG.info("Printing analysis summary")
    
    high_earners = pipeline.get_artifact("high_earners")
    high_earners_df = high_earners.as_dataframe()
    
    department_stats = pipeline.get_artifact("department_stats")
    dept_stats_df = department_stats.as_dataframe()
    
    # Use loader component for printing
    report_loader.print_analysis_summary(high_earners_df, dept_stats_df)

def run():
    """Execute the workflow."""
    LOG.info("Starting employee analysis workflow")
    
    # Execute pipeline - framework handles everything!
    results = pipeline.execute(parallel=True, max_workers=4)
    
    LOG.info(f"\nWorkflow complete!")
    LOG.info(f"Tasks executed: {results['tasks_executed']}")
    LOG.info(f"Artifacts created: {results['artifacts_created']}")
    LOG.info(f"Artifacts: {', '.join(results['artifacts'])}")
    
    return results

if __name__ == "__main__":
    run()