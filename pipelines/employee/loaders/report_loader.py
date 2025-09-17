"""
Employee report loading and output logic.
"""

import pandas as pd
from pathlib import Path
import json
import logging
from typing import Dict, Any, Optional
from airpipe.utils.loaders.file_utils import FileUtils

logger = logging.getLogger(__name__)


class EmployeeReportLoader:
    """Load employee analysis results and generate reports."""
    
    def __init__(self):
        self.file_utils = FileUtils()
    
    def save_high_earners(self,
                         df: pd.DataFrame,
                         output_path: str = "output/high_earners.csv") -> None:
        """
        Save high earners data to CSV.
        
        Args:
            df: DataFrame with high earner data
            output_path: Output file path
        """
        self.file_utils.save_to_csv(df, output_path)
        logger.info(f"Saved {len(df)} high earners to {output_path}")
    
    def save_department_stats(self,
                             df: pd.DataFrame,
                             output_path: str = "output/department_stats.json",
                             format: str = 'json') -> None:
        """
        Save department statistics in specified format.
        
        Args:
            df: Department statistics DataFrame
            output_path: Output file path
            format: Output format ('json' or 'csv')
        """
        if format == 'json':
            self.file_utils.save_to_json(df, output_path, orient='records')
            logger.info(f"Saved department statistics to {output_path} (JSON)")
        else:
            csv_path = output_path.replace('.json', '.csv')
            self.file_utils.save_to_csv(df, csv_path)
            logger.info(f"Saved department statistics to {csv_path} (CSV)")
    
    def print_analysis_summary(self,
                              high_earners_df: pd.DataFrame,
                              dept_stats_df: pd.DataFrame) -> None:
        """
        Print comprehensive analysis summary.
        
        Args:
            high_earners_df: High earners DataFrame
            dept_stats_df: Department statistics DataFrame
        """
        print("=" * 60)
        print("EMPLOYEE ANALYSIS SUMMARY")
        print("=" * 60)
        
        # High earners summary
        if not high_earners_df.empty:
            print(f"\nHigh Earners Analysis:")
            print(f"  • Total high earners: {len(high_earners_df)}")
            if 'salary' in high_earners_df.columns:
                print(f"  • Average salary: ${high_earners_df['salary'].mean():,.2f}")
                print(f"  • Salary range: ${high_earners_df['salary'].min():,.2f} - ${high_earners_df['salary'].max():,.2f}")
            
            if 'department' in high_earners_df.columns:
                top_dept = high_earners_df['department'].value_counts().iloc[0]
                print(f"  • Department with most high earners: {high_earners_df['department'].value_counts().index[0]} ({top_dept} employees)")
        
        # Department statistics summary
        if not dept_stats_df.empty:
            print(f"\nDepartment Analysis:")
            print(f"  • Total departments: {len(dept_stats_df)}")
            
            if 'salary_mean' in dept_stats_df.columns:
                top_dept = dept_stats_df.loc[dept_stats_df['salary_mean'].idxmax()]
                print(f"  • Highest average salary: {top_dept['department']} (${top_dept['salary_mean']:,.2f})")
                
                lowest_dept = dept_stats_df.loc[dept_stats_df['salary_mean'].idxmin()]
                print(f"  • Lowest average salary: {lowest_dept['department']} (${lowest_dept['salary_mean']:,.2f})")
            
            if 'employee_count' in dept_stats_df.columns:
                largest_dept = dept_stats_df.loc[dept_stats_df['employee_count'].idxmax()]
                print(f"  • Largest department: {largest_dept['department']} ({int(largest_dept['employee_count'])} employees)")
        
        print("=" * 60)
        logger.info("Printed analysis summary")
    
    def generate_full_report(self,
                            high_earners_df: pd.DataFrame,
                            dept_stats_df: pd.DataFrame,
                            output_dir: str = "output/reports") -> Dict[str, str]:
        """
        Generate comprehensive report files.
        
        Args:
            high_earners_df: High earners DataFrame
            dept_stats_df: Department statistics DataFrame
            output_dir: Output directory for reports
            
        Returns:
            Dictionary with paths to generated files
        """
        # Create output directory
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        generated_files = {}
        
        # Save high earners
        high_earners_path = f"{output_dir}/high_earners.csv"
        self.save_high_earners(high_earners_df, high_earners_path)
        generated_files['high_earners'] = high_earners_path
        
        # Save department stats
        dept_stats_path = f"{output_dir}/department_stats.json"
        self.save_department_stats(dept_stats_df, dept_stats_path)
        generated_files['department_stats'] = dept_stats_path
        
        # Generate summary report
        summary = self._create_summary_dict(high_earners_df, dept_stats_df)
        summary_path = f"{output_dir}/analysis_summary.json"
        self.file_utils.save_to_json(summary, summary_path)
        generated_files['summary'] = summary_path
        
        logger.info(f"Generated full report with {len(generated_files)} files in {output_dir}")
        return generated_files
    
    def _create_summary_dict(self,
                           high_earners_df: pd.DataFrame,
                           dept_stats_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Create summary dictionary for reporting.
        
        Args:
            high_earners_df: High earners DataFrame
            dept_stats_df: Department statistics DataFrame
            
        Returns:
            Summary dictionary
        """
        summary = {
            'analysis_date': pd.Timestamp.now().isoformat(),
            'high_earners': {
                'count': len(high_earners_df),
                'avg_salary': high_earners_df['salary'].mean() if 'salary' in high_earners_df.columns else None,
                'departments': high_earners_df['department'].nunique() if 'department' in high_earners_df.columns else None
            },
            'departments': {
                'count': len(dept_stats_df),
                'total_employees': dept_stats_df['employee_count'].sum() if 'employee_count' in dept_stats_df.columns else None,
                'avg_department_size': dept_stats_df['employee_count'].mean() if 'employee_count' in dept_stats_df.columns else None
            }
        }
        
        return summary