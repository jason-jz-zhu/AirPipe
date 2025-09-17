"""
Enhanced employee workflow using new architecture with explicit dependencies.

This demonstrates:
- Multiple extractors from different sources
- Explicit dependency management using depends_on, produces, consumes
- Reusable utilities from utils folder
- Pipeline-specific business logic
"""

from pathlib import Path
import sys
# Add both the project root and the pipelines directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
import logging
from airpipe.core.task import TaskPipeline

# Import extractors
from employee.extractors.csv_extractor import EmployeeCSVExtractor

# Import transformers
from employee.transformers.salary_transformer import SalaryTransformer

# Import loaders
from employee.loaders.report_loader import EmployeeReportLoader

# Setup logging
LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Initialize pipeline
pipeline = TaskPipeline("employee_enhanced_analysis")

# Initialize components
csv_extractor = EmployeeCSVExtractor()
salary_transformer = SalaryTransformer()
report_loader = EmployeeReportLoader()


# ============================================================================
# EXTRACTION TASKS - No dependencies, can run in parallel
# ============================================================================

@pipeline.task(produces="raw_employee_data")
def extract_employees():
    """Extract employee data from CSV."""
    LOG.info("Extracting employee data from CSV")
    df = csv_extractor.extract_current_employees()
    return pipeline.create_artifact(df, "raw_employee_data")


# ============================================================================
# TRANSFORMATION TASKS - Depend on extraction tasks
# ============================================================================

@pipeline.task(
    depends_on=["extract_employees"],
    consumes="raw_employee_data",
    produces="high_earners"
)
def filter_high_earners():
    """Filter high earning employees."""
    LOG.info("Filtering high earning employees")
    
    # Get artifact using the enhanced system
    raw_data = pipeline.get_artifact("raw_employee_data")
    df = raw_data.as_dataframe()
    
    # Apply transformation
    filtered = salary_transformer.filter_high_earners(df, threshold=70000)
    
    LOG.info(f"Found {len(filtered)} high earners out of {len(df)} employees")
    return pipeline.create_artifact(filtered, "high_earners")


@pipeline.task(
    depends_on=["extract_employees"],
    consumes="raw_employee_data",
    produces="salary_statistics"
)
def calculate_salary_stats():
    """Calculate comprehensive salary statistics."""
    LOG.info("Calculating salary statistics")
    
    raw_data = pipeline.get_artifact("raw_employee_data")
    df = raw_data.as_dataframe()
    
    stats = salary_transformer.calculate_salary_statistics(df)
    return pipeline.create_artifact(stats, "salary_statistics")


@pipeline.task(
    depends_on=["extract_employees"],
    consumes="raw_employee_data",
    produces="department_analysis"
)
def analyze_departments():
    """Analyze salary by department."""
    LOG.info("Analyzing departments")
    
    raw_data = pipeline.get_artifact("raw_employee_data")
    df = raw_data.as_dataframe()
    
    dept_analysis = salary_transformer.analyze_salary_by_department(df)
    
    LOG.info(f"Analyzed {len(dept_analysis)} departments")
    return pipeline.create_artifact(dept_analysis, "department_analysis")


@pipeline.task(
    depends_on=["extract_employees"],
    consumes="raw_employee_data",
    produces="salary_bands"
)
def create_salary_bands():
    """Create salary band assignments."""
    LOG.info("Creating salary bands")
    
    raw_data = pipeline.get_artifact("raw_employee_data")
    df = raw_data.as_dataframe()
    
    banded = salary_transformer.create_salary_bands(df, num_bands=5)
    return pipeline.create_artifact(banded, "salary_bands")


@pipeline.task(
    depends_on=["extract_employees"],
    consumes="raw_employee_data",
    produces="pay_equity_metrics"
)
def calculate_pay_equity():
    """Calculate pay equity metrics."""
    LOG.info("Calculating pay equity metrics")
    
    raw_data = pipeline.get_artifact("raw_employee_data")
    df = raw_data.as_dataframe()
    
    equity = salary_transformer.calculate_pay_equity_metrics(df, group_column='department')
    return pipeline.create_artifact(equity, "pay_equity_metrics")


# ============================================================================
# AGGREGATION TASKS - Combine multiple analyses
# ============================================================================

@pipeline.task(
    depends_on=["filter_high_earners", "calculate_salary_stats", "analyze_departments"],
    consumes=["high_earners", "salary_statistics", "department_analysis"],
    produces="comprehensive_report"
)
def generate_comprehensive_report():
    """Generate comprehensive HR report."""
    LOG.info("Generating comprehensive report")
    
    # Get multiple artifacts
    high_earners = pipeline.get_artifact("high_earners").as_dataframe()
    salary_stats = pipeline.get_artifact("salary_statistics").as_dataframe()
    dept_analysis = pipeline.get_artifact("department_analysis").as_dataframe()
    
    # Generate combined report
    # For now, just return the high earners as the comprehensive report
    # since generate_full_report expects only 2 dataframes
    report = high_earners.copy()
    
    return pipeline.create_artifact(report, "comprehensive_report")


# ============================================================================
# LOADING TASKS - Save results to files
# ============================================================================

@pipeline.task(
    depends_on=["filter_high_earners"],
    consumes="high_earners"
)
def save_high_earners():
    """Save high earners to file."""
    LOG.info("Saving high earners report")
    
    high_earners = pipeline.get_artifact("high_earners").as_dataframe()
    report_loader.save_high_earners(high_earners, "output/high_earners.csv")
    filepath = "output/high_earners.csv"
    
    LOG.info(f"Saved high earners to: {filepath}")


@pipeline.task(
    depends_on=["analyze_departments"],
    consumes="department_analysis"
)
def save_department_stats():
    """Save department statistics."""
    LOG.info("Saving department statistics")
    
    dept_analysis = pipeline.get_artifact("department_analysis").as_dataframe()
    report_loader.save_department_stats(dept_analysis, "output/department_stats.json")
    filepath = "output/department_stats.json"
    
    LOG.info(f"Saved department stats to: {filepath}")


@pipeline.task(
    depends_on=["generate_comprehensive_report"],
    consumes="comprehensive_report"
)
def save_comprehensive_report():
    """Save comprehensive report in multiple formats."""
    LOG.info("Saving comprehensive report")
    
    report = pipeline.get_artifact("comprehensive_report").as_dataframe()
    # Save comprehensive report as CSV
    from pathlib import Path
    Path("output").mkdir(parents=True, exist_ok=True)
    report.to_csv("output/comprehensive_hr_report.csv", index=False)
    report.to_json("output/comprehensive_hr_report.json", orient='records', indent=2)
    LOG.info(f"Saved comprehensive report to output/comprehensive_hr_report.csv and .json")


@pipeline.task(
    depends_on=["calculate_pay_equity"],
    consumes="pay_equity_metrics"
)
def save_equity_analysis():
    """Save pay equity analysis."""
    LOG.info("Saving pay equity analysis")
    
    equity = pipeline.get_artifact("pay_equity_metrics").as_dataframe()
    from pathlib import Path
    Path("output").mkdir(parents=True, exist_ok=True)
    equity.to_csv("output/pay_equity_analysis.csv", index=False)
    equity.to_json("output/pay_equity_analysis.json", orient='records', indent=2)
    LOG.info(f"Saved pay equity analysis to output/pay_equity_analysis.csv and .json")


# ============================================================================
# SUMMARY TASK - Final reporting
# ============================================================================

@pipeline.task(
    depends_on=[
        "save_high_earners",
        "save_department_stats",
        "save_comprehensive_report",
        "save_equity_analysis"
    ]
)
def print_summary():
    """Print execution summary."""
    LOG.info("=" * 60)
    LOG.info("PIPELINE EXECUTION SUMMARY")
    LOG.info("=" * 60)
    
    # Get artifacts for summary
    if "high_earners" in pipeline.named_artifacts:
        high_earners = pipeline.get_artifact("high_earners").as_dataframe()
        LOG.info(f"High Earners: {len(high_earners)} employees")
    
    if "department_analysis" in pipeline.named_artifacts:
        dept_analysis = pipeline.get_artifact("department_analysis").as_dataframe()
        LOG.info(f"Departments Analyzed: {len(dept_analysis)}")
        
        if 'avg_salary' in dept_analysis.columns:
            top_dept = dept_analysis.loc[dept_analysis['avg_salary'].idxmax()]
            LOG.info(f"Highest Paying Dept: {top_dept.get('department', 'N/A')} "
                    f"(${top_dept['avg_salary']:,.2f})")
    
    if "pay_equity_metrics" in pipeline.named_artifacts:
        equity = pipeline.get_artifact("pay_equity_metrics").as_dataframe()
        LOG.info(f"Pay Equity: Analyzed {len(equity)} groups")
    
    LOG.info("=" * 60)


def run():
    """
    Execute the enhanced workflow.
    
    The pipeline automatically:
    1. Resolves dependencies based on depends_on parameters
    2. Executes tasks in parallel where possible
    3. Manages artifact flow using produces/consumes
    """
    LOG.info("Starting enhanced employee analysis workflow")
    LOG.info("This workflow demonstrates explicit dependency management")
    
    # Simply execute - no manual orchestration needed!
    # The pipeline will:
    # - Run extract_employees first (no dependencies)
    # - Run all transformation tasks in parallel (they all depend only on extract_employees)
    # - Run generate_comprehensive_report after its dependencies complete
    # - Run all save tasks in parallel after their dependencies
    # - Run print_summary last
    
    results = pipeline.execute(parallel=True, max_workers=6)
    
    LOG.info(f"\nWorkflow complete!")
    LOG.info(f"Tasks executed: {results['tasks_executed']}")
    LOG.info(f"Artifacts created: {results['artifacts_created']}")
    LOG.info(f"Artifacts: {', '.join(results['artifacts'])}")
    
    return results


def visualize():
    """
    Visualize the workflow DAG using different formats.
    """
    # ASCII visualization
    print(pipeline.visualize_dag(format='ascii'))
    
    # Save as Mermaid diagram
    mermaid_output = pipeline.visualize_dag(format='mermaid', output_file='output/workflow_dag.md')
    
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
    
    # Validate DAG
    try:
        pipeline.validate_dag()
        print("\n✓ DAG validation passed - no cycles detected")
    except RuntimeError as e:
        print(f"\n✗ DAG validation failed: {e}")
    
    return mermaid_output


if __name__ == "__main__":
    import sys
    
    # Check for visualization flag
    if len(sys.argv) > 1 and sys.argv[1] in ['--visualize', '-v']:
        # Just visualize without running
        visualize()
    else:
        # Visualize and then run
        print("Visualizing workflow DAG...")
        visualize()
        
        print("\n" + "=" * 60)
        print("Now executing workflow...")
        print("=" * 60)
        
        # Run the workflow
        run()