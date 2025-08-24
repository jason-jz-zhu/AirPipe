"""Tests for employee CSV extractor business logic."""

import unittest
from unittest.mock import Mock, patch, mock_open
import pandas as pd
from pathlib import Path

from pipelines.employee.extractors.csv_extractor import EmployeeCSVExtractor
from tests.base import BaseTestCase
from tests.fixtures.factories import DataFactory


class TestEmployeeCSVExtractor(BaseTestCase):
    """Test EmployeeCSVExtractor business logic."""
    
    def setUp(self):
        """Set up test fixtures."""
        super().setUp()
        self.extractor = EmployeeCSVExtractor()
        
        # Create test employee data
        self.employee_df = DataFactory.create_employee_dataframe(100)
        
    def test_extract_current_employees(self):
        """Test extracting only current active employees."""
        # Create test data with mix of active/inactive
        test_df = self.employee_df.copy()
        test_df.loc[0:20, 'is_active'] = False  # Make 20 employees inactive
        
        # Mock file reading
        with patch.object(self.extractor, '_read_employee_file', return_value=test_df):
            result = self.extractor.extract_current_employees()
            
        # Should only return active employees
        self.assertTrue(all(result['is_active']))
        self.assertEqual(len(result), test_df['is_active'].sum())
        
    def test_extract_by_department(self):
        """Test extracting employees by specific department."""
        test_df = self.employee_df.copy()
        
        with patch.object(self.extractor, '_read_employee_file', return_value=test_df):
            # Extract only Engineering department
            result = self.extractor.extract_by_department('Engineering')
            
        # All should be from Engineering
        self.assertTrue(all(result['department'] == 'Engineering'))
        
        # Test with multiple departments
        with patch.object(self.extractor, '_read_employee_file', return_value=test_df):
            result = self.extractor.extract_by_department(['Engineering', 'Sales'])
            
        self.assertTrue(all(result['department'].isin(['Engineering', 'Sales'])))
        
    def test_extract_recent_hires(self):
        """Test extracting employees hired within a date range."""
        test_df = self.employee_df.copy()
        
        with patch.object(self.extractor, '_read_employee_file', return_value=test_df):
            # Extract employees hired in last 30 days
            cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=30)
            result = self.extractor.extract_recent_hires(days=30)
            
        # All hire dates should be recent
        if len(result) > 0:
            self.assertTrue(all(result['hire_date'] >= cutoff_date))
            
    def test_extract_salary_range(self):
        """Test extracting employees within salary range."""
        test_df = self.employee_df.copy()
        
        with patch.object(self.extractor, '_read_employee_file', return_value=test_df):
            result = self.extractor.extract_by_salary_range(
                min_salary=60000,
                max_salary=90000
            )
            
        # All salaries should be in range
        self.assertTrue(all((result['salary'] >= 60000) & (result['salary'] <= 90000)))
        
    def test_data_validation(self):
        """Test data validation in extraction."""
        # Test with missing required columns
        invalid_df = pd.DataFrame({'name': ['John', 'Jane']})
        
        with patch.object(self.extractor, '_read_employee_file', return_value=invalid_df):
            with self.assertRaises(ValueError) as ctx:
                self.extractor.extract_current_employees()
                
        self.assertIn("required column", str(ctx.exception).lower())
        
    def test_handle_missing_data(self):
        """Test handling of missing data in employee records."""
        test_df = self.employee_df.copy()
        # Introduce some missing values
        test_df.loc[0:5, 'department'] = None
        test_df.loc[10:15, 'salary'] = None
        
        with patch.object(self.extractor, '_read_employee_file', return_value=test_df):
            result = self.extractor.extract_with_complete_data()
            
        # Should exclude records with missing critical data
        self.assertFalse(result[['department', 'salary']].isnull().any().any())
        
    def test_extract_managers(self):
        """Test extracting employees with manager role."""
        test_df = self.employee_df.copy()
        # Add role column
        test_df['role'] = ['Manager' if i % 10 == 0 else 'Employee' 
                          for i in range(len(test_df))]
        
        with patch.object(self.extractor, '_read_employee_file', return_value=test_df):
            result = self.extractor.extract_managers()
            
        # All should be managers
        self.assertTrue(all(result['role'] == 'Manager'))
        
    def test_extract_with_experience(self):
        """Test extracting employees based on years of experience."""
        test_df = self.employee_df.copy()
        
        with patch.object(self.extractor, '_read_employee_file', return_value=test_df):
            # Extract employees with 2+ years experience
            result = self.extractor.extract_by_experience(min_years=2)
            
        # Calculate experience
        today = pd.Timestamp.now()
        years_exp = (today - result['hire_date']).dt.days / 365.25
        self.assertTrue(all(years_exp >= 2))
        
    def test_extract_for_payroll(self):
        """Test extracting data specifically for payroll processing."""
        test_df = self.employee_df.copy()
        
        with patch.object(self.extractor, '_read_employee_file', return_value=test_df):
            result = self.extractor.extract_for_payroll()
            
        # Should only include necessary columns
        required_cols = ['employee_id', 'name', 'salary', 'department']
        for col in required_cols:
            self.assertIn(col, result.columns)
            
        # Should only include active employees
        self.assertTrue(all(result['is_active']))
        
    def test_performance_with_large_dataset(self):
        """Test extraction performance with large dataset."""
        # Create large dataset
        large_df = DataFactory.create_employee_dataframe(10000)
        
        with patch.object(self.extractor, '_read_employee_file', return_value=large_df):
            import time
            start = time.time()
            result = self.extractor.extract_current_employees()
            elapsed = time.time() - start
            
        # Should handle 10k records quickly
        self.assertLess(elapsed, 1.0)  # Less than 1 second
        self.assertGreater(len(result), 0)


class TestEmployeeExtractorIntegration(BaseTestCase):
    """Integration tests for employee extractor with real file operations."""
    
    def test_extract_from_csv_file(self):
        """Test extracting from actual CSV file."""
        # Create test CSV
        df = DataFactory.create_employee_dataframe(50)
        csv_path = self.create_temp_csv("employees.csv", df)
        
        extractor = EmployeeCSVExtractor(file_path=str(csv_path))
        result = extractor.extract_current_employees()
        
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)
        
    def test_extract_from_multiple_sources(self):
        """Test combining data from multiple employee sources."""
        # Create multiple CSV files
        df1 = DataFactory.create_employee_dataframe(30)
        df2 = DataFactory.create_employee_dataframe(20)
        
        csv1 = self.create_temp_csv("employees1.csv", df1)
        csv2 = self.create_temp_csv("employees2.csv", df2)
        
        extractor = EmployeeCSVExtractor()
        result = extractor.extract_from_multiple_files([str(csv1), str(csv2)])
        
        # Should combine both sources
        self.assertEqual(len(result), 50)
        
    def test_incremental_extraction(self):
        """Test incremental extraction based on last update."""
        df = DataFactory.create_employee_dataframe(100)
        # Add update timestamp
        df['last_updated'] = pd.date_range(
            start='2023-01-01',
            periods=100,
            freq='D'
        )
        
        csv_path = self.create_temp_csv("employees_inc.csv", df)
        extractor = EmployeeCSVExtractor(file_path=str(csv_path))
        
        # Extract only recently updated records
        cutoff = pd.Timestamp('2023-03-01')
        result = extractor.extract_incremental(since=cutoff)
        
        self.assertTrue(all(result['last_updated'] >= cutoff))


if __name__ == "__main__":
    unittest.main()