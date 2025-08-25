"""
CSV loading logic for simple workflows.
"""

import pandas as pd
from pathlib import Path
import logging
from typing import Optional, Dict, Any
from airpipe.utils.loaders.file_utils import FileUtils

logger = logging.getLogger(__name__)


class SimpleCSVLoader:
    """Load data to CSV files for simple workflows."""
    
    def __init__(self):
        self.file_utils = FileUtils()
    
    def save_results(self,
                    df: pd.DataFrame,
                    output_path: str = "output/simple_output.csv",
                    include_metadata: bool = True) -> None:
        """
        Save results to CSV file.
        
        Args:
            df: DataFrame to save
            output_path: Output file path
            include_metadata: Whether to add metadata before saving
        """
        # Add metadata if requested
        if include_metadata:
            df = self._add_save_metadata(df)
        
        # Use file utils to save
        self.file_utils.save_to_csv(df, output_path)
        
        logger.info(f"Saved {len(df)} records to {output_path}")
    
    def save_with_summary(self,
                         df: pd.DataFrame,
                         output_path: str = "output/simple_output.csv",
                         summary_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Save results and generate summary statistics.
        
        Args:
            df: DataFrame to save
            output_path: Output file path
            summary_path: Optional path for summary file
            
        Returns:
            Dictionary with summary statistics
        """
        # Save main data
        self.save_results(df, output_path, include_metadata=False)
        
        # Generate summary
        summary = self._generate_summary(df)
        
        # Save summary if path provided
        if summary_path:
            summary_df = pd.DataFrame([summary])
            self.file_utils.save_to_csv(summary_df, summary_path)
            logger.info(f"Saved summary to {summary_path}")
        
        return summary
    
    def _add_save_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add metadata columns before saving.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with metadata columns
        """
        df_copy = df.copy()
        df_copy['saved_at'] = pd.Timestamp.now()
        df_copy['record_count'] = len(df)
        
        return df_copy
    
    def _generate_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate summary statistics for the data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary with summary statistics
        """
        summary = {
            'total_records': len(df),
            'columns': list(df.columns),
            'column_count': len(df.columns)
        }
        
        # Add numeric column statistics
        numeric_cols = df.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            summary[f'{col}_mean'] = df[col].mean()
            summary[f'{col}_min'] = df[col].min()
            summary[f'{col}_max'] = df[col].max()
        
        # Add categorical column statistics
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns
        for col in categorical_cols:
            summary[f'{col}_unique'] = df[col].nunique()
        
        logger.info(f"Generated summary with {len(summary)} metrics")
        return summary