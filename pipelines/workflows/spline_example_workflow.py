"""
Example workflow demonstrating Apache Spline lineage tracking integration.

This workflow shows how to:
1. Enable Spline lineage tracking for a pipeline
2. Track task execution and data flow
3. View lineage in Spline UI

Prerequisites:
- Apache Spline server running (default: http://localhost:8080)
- Or use Docker: docker-compose up from spline-getting-started repo
"""

from pathlib import Path
import sys
# Add both the project root and the pipelines directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
import logging
from datetime import datetime
import argparse

from airpipe.core.task import TaskPipeline
from airpipe.lineage.spline_tracker import SplineLineageTracker
from airpipe.lineage.config import SplineConfig

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
LOG = logging.getLogger(__name__)


def create_sample_data():
    """Create sample data for demonstration."""
    # Create sample employee data
    employees = pd.DataFrame({
        'employee_id': range(1, 101),
        'name': [f'Employee_{i}' for i in range(1, 101)],
        'department': np.random.choice(['Engineering', 'Sales', 'Marketing', 'HR', 'Finance'], 100),
        'salary': np.random.randint(40000, 150000, 100),
        'hire_date': pd.date_range(start='2015-01-01', periods=100, freq='W'),
        'performance_rating': np.random.uniform(1, 5, 100)
    })
    
    # Create sample sales data
    sales = pd.DataFrame({
        'sale_id': range(1, 501),
        'employee_id': np.random.randint(1, 101, 500),
        'product': np.random.choice(['Product_A', 'Product_B', 'Product_C', 'Product_D'], 500),
        'amount': np.random.uniform(100, 10000, 500),
        'date': pd.date_range(start='2023-01-01', periods=500, freq='D')
    })
    
    return employees, sales


def setup_spline_tracking(enable_spline: bool = True, spline_url: str = None):
    """
    Setup Spline lineage tracking.
    
    Args:
        enable_spline: Whether to enable Spline tracking
        spline_url: Optional Spline server URL (defaults to http://localhost:8080)
    
    Returns:
        SplineLineageTracker instance or None
    """
    if not enable_spline:
        LOG.info("Spline lineage tracking disabled")
        return None
    
    # Configure Spline
    config = SplineConfig(
        spline_url=spline_url or "http://localhost:8080",
        producer_api_path="/producer/v1/lineage",
        enabled=True,
        capture_schemas=True,
        capture_row_counts=True,
        capture_execution_time=True,
        application_name="AirPipe Example",
        application_version="1.0.0",
        environment="development",
        custom_metadata={
            "team": "Data Engineering",
            "project": "Spline Integration Demo"
        }
    )
    
    # Create lineage tracker
    tracker = SplineLineageTracker(config)
    LOG.info(f"Spline lineage tracking enabled. Server: {config.spline_url}")
    
    return tracker


# Initialize pipeline with lineage tracking
def create_pipeline_with_lineage(enable_spline: bool = True, spline_url: str = None):
    """Create pipeline with optional Spline lineage tracking."""
    tracker = setup_spline_tracking(enable_spline, spline_url)
    return TaskPipeline("spline_demo_pipeline", lineage_tracker=tracker)


# Create the pipeline
pipeline = None  # Will be initialized in main()


def define_workflow():
    """Define the workflow tasks."""
    
    @pipeline.task(produces="employee_data")
    def extract_employees():
        """Extract employee data."""
        LOG.info("Extracting employee data")
        employees, _ = create_sample_data()
        
        # Add some data quality metrics
        LOG.info(f"Extracted {len(employees)} employee records")
        LOG.info(f"Departments: {employees['department'].unique().tolist()}")
        
        return pipeline.create_artifact(employees, "employee_data")
    
    @pipeline.task(produces="sales_data")
    def extract_sales():
        """Extract sales data."""
        LOG.info("Extracting sales data")
        _, sales = create_sample_data()
        
        LOG.info(f"Extracted {len(sales)} sales records")
        LOG.info(f"Date range: {sales['date'].min()} to {sales['date'].max()}")
        
        return pipeline.create_artifact(sales, "sales_data")
    
    @pipeline.task(
        depends_on=["extract_employees"],
        consumes="employee_data",
        produces="high_performers"
    )
    def identify_high_performers():
        """Identify high-performing employees."""
        LOG.info("Identifying high performers")
        
        employees = pipeline.get_artifact("employee_data").as_dataframe()
        
        # Filter high performers (rating > 4.0)
        high_performers = employees[employees['performance_rating'] > 4.0].copy()
        high_performers['performer_category'] = 'High'
        
        LOG.info(f"Found {len(high_performers)} high performers")
        LOG.info(f"Average salary of high performers: ${high_performers['salary'].mean():,.2f}")
        
        return pipeline.create_artifact(high_performers, "high_performers")
    
    @pipeline.task(
        depends_on=["extract_employees", "extract_sales"],
        consumes=["employee_data", "sales_data"],
        produces="employee_sales"
    )
    def join_employee_sales():
        """Join employee and sales data."""
        LOG.info("Joining employee and sales data")
        
        employees = pipeline.get_artifact("employee_data").as_dataframe()
        sales = pipeline.get_artifact("sales_data").as_dataframe()
        
        # Join on employee_id
        employee_sales = sales.merge(
            employees[['employee_id', 'name', 'department', 'salary']],
            on='employee_id',
            how='left'
        )
        
        LOG.info(f"Created {len(employee_sales)} employee-sales records")
        
        return pipeline.create_artifact(employee_sales, "employee_sales")
    
    @pipeline.task(
        depends_on=["join_employee_sales"],
        consumes="employee_sales",
        produces="department_performance"
    )
    def analyze_department_performance():
        """Analyze sales performance by department."""
        LOG.info("Analyzing department performance")
        
        employee_sales = pipeline.get_artifact("employee_sales").as_dataframe()
        
        # Aggregate by department
        dept_performance = employee_sales.groupby('department').agg({
            'amount': ['sum', 'mean', 'count'],
            'employee_id': 'nunique'
        }).round(2)
        
        dept_performance.columns = ['total_sales', 'avg_sale', 'num_sales', 'num_employees']
        dept_performance = dept_performance.reset_index()
        
        LOG.info("Department Performance Summary:")
        LOG.info(f"\n{dept_performance.to_string()}")
        
        return pipeline.create_artifact(dept_performance, "department_performance")
    
    @pipeline.task(
        depends_on=["identify_high_performers", "analyze_department_performance"],
        consumes=["high_performers", "department_performance"],
        produces="final_report"
    )
    def generate_final_report():
        """Generate final analysis report."""
        LOG.info("Generating final report")
        
        high_performers = pipeline.get_artifact("high_performers").as_dataframe()
        dept_performance = pipeline.get_artifact("department_performance").as_dataframe()
        
        # Create summary report
        report = {
            'report_date': datetime.now().isoformat(),
            'high_performers_count': len(high_performers),
            'high_performers_avg_salary': high_performers['salary'].mean(),
            'top_department': dept_performance.nlargest(1, 'total_sales')['department'].values[0],
            'total_company_sales': dept_performance['total_sales'].sum(),
            'departments_analyzed': len(dept_performance)
        }
        
        LOG.info("Final Report Summary:")
        for key, value in report.items():
            if isinstance(value, float):
                LOG.info(f"  {key}: {value:,.2f}")
            else:
                LOG.info(f"  {key}: {value}")
        
        return pipeline.create_artifact(report, "final_report")
    
    @pipeline.task(
        depends_on=["generate_final_report"],
        consumes="final_report"
    )
    def save_results():
        """Save results to files."""
        LOG.info("Saving results")
        
        # Create output directory
        output_dir = Path("output") / "spline_demo"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save final report
        report = pipeline.get_artifact("final_report").data
        
        # Save as JSON
        import json
        with open(output_dir / "final_report.json", 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        LOG.info(f"Results saved to {output_dir}")
        
        return f"Results saved to {output_dir}"


def run():
    """Execute the pipeline."""
    # Define the workflow
    define_workflow()
    
    # Execute pipeline
    LOG.info("=" * 60)
    LOG.info("Starting Spline Demo Pipeline")
    LOG.info("=" * 60)
    
    results = pipeline.execute(parallel=True, max_workers=4)
    
    LOG.info("=" * 60)
    LOG.info("Pipeline Execution Complete")
    LOG.info(f"Tasks executed: {results['tasks_executed']}")
    LOG.info(f"Artifacts created: {results['artifacts_created']}")
    LOG.info("=" * 60)
    
    return results


def main():
    """Main entry point with CLI arguments."""
    parser = argparse.ArgumentParser(
        description="AirPipe workflow with Apache Spline lineage tracking"
    )
    parser.add_argument(
        '--no-spline',
        action='store_true',
        help='Disable Spline lineage tracking'
    )
    parser.add_argument(
        '--spline-url',
        type=str,
        default=None,
        help='Spline server URL (default: http://localhost:8080)'
    )
    parser.add_argument(
        '--visualize',
        action='store_true',
        help='Visualize the DAG without executing'
    )
    
    args = parser.parse_args()
    
    # Initialize pipeline globally
    global pipeline
    pipeline = create_pipeline_with_lineage(
        enable_spline=not args.no_spline,
        spline_url=args.spline_url
    )
    
    # Define workflow
    define_workflow()
    
    if args.visualize:
        # Just visualize the DAG
        print(pipeline.visualize_dag(format='ascii'))
        print("\nTo view lineage in Spline UI:")
        print(f"1. Open http://localhost:8080 (or your Spline server URL)")
        print("2. Navigate to the Lineage section")
        print("3. Look for 'spline_demo_pipeline' executions")
    else:
        # Run the pipeline
        results = run()
        
        if not args.no_spline:
            print("\n" + "=" * 60)
            print("VIEWING LINEAGE IN SPLINE UI:")
            print("=" * 60)
            print(f"1. Open your Spline UI: {args.spline_url or 'http://localhost:8080'}")
            print("2. Navigate to 'Executions' or 'Lineage' section")
            print("3. Look for 'spline_demo_pipeline' execution")
            print("4. Click to view the complete data lineage graph")
            print("=" * 60)
        
        return results


if __name__ == "__main__":
    main()