"""
Analytics Loader for DuckDB Analytics.

Provides data export and persistence operations for analytical workflows,
supporting multiple output formats and storage options.
"""

import logging
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

from airpipe.utils.duckdb import DuckDBArtifact

LOG = logging.getLogger(__name__)


class AnalyticsLoader:
    """Export and persistence operations for DuckDB analytics results."""
    
    def __init__(self, output_dir: str = "output"):
        """
        Initialize analytics loader.
        
        Args:
            output_dir: Base directory for output files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
    def export_pivot_reports(
        self,
        monthly_pivot_artifact: DuckDBArtifact,
        output_format: str = "parquet"
    ) -> Dict[str, str]:
        """
        Export pivot report to file.
        
        Args:
            monthly_pivot_artifact: Pivot data artifact to export
            output_format: Output format ('parquet', 'csv', 'excel')
            
        Returns:
            Dictionary with export information
        """
        LOG.info(f"Exporting pivot report in {output_format} format")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_filename = f"monthly_revenue_pivot_{timestamp}"
        
        # Export based on format
        if output_format.lower() == "parquet":
            filepath = self.output_dir / f"{base_filename}.parquet"
            monthly_pivot_artifact.to_parquet(str(filepath))
        elif output_format.lower() == "csv":
            filepath = self.output_dir / f"{base_filename}.csv"
            monthly_pivot_artifact.to_csv(str(filepath))
        elif output_format.lower() == "excel":
            filepath = self.output_dir / f"{base_filename}.xlsx"
            df = monthly_pivot_artifact.as_dataframe()
            df.to_excel(str(filepath), index=False)
        else:
            raise ValueError(f"Unsupported output format: {output_format}")
        
        export_info = {
            'file_path': str(filepath),
            'format': output_format,
            'timestamp': timestamp,
            'records_exported': len(monthly_pivot_artifact.as_dataframe()),
            'artifact_name': monthly_pivot_artifact.name
        }
        
        LOG.info(f"Exported pivot report to: {filepath}")
        return export_info
    
    def export_customer_analysis(
        self,
        customer_analysis_artifact: DuckDBArtifact,
        export_segments: bool = True
    ) -> Dict[str, str]:
        """
        Export customer analysis results.
        
        Args:
            customer_analysis_artifact: Customer analysis data to export
            export_segments: Whether to export segment summaries separately
            
        Returns:
            Dictionary with export information
        """
        LOG.info("Exporting customer analysis results")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        exports = {}
        
        # Export full customer analysis
        csv_path = self.output_dir / f"customer_segments_{timestamp}.csv"
        customer_analysis_artifact.to_csv(str(csv_path))
        exports['customer_analysis_csv'] = str(csv_path)
        
        # Export as JSON for programmatic access
        json_path = self.output_dir / f"customer_segments_{timestamp}.json"
        df = customer_analysis_artifact.as_dataframe()
        df.to_json(str(json_path), orient='records', date_format='iso', indent=2)
        exports['customer_analysis_json'] = str(json_path)
        
        # Export segment summaries if requested
        if export_segments:
            segment_summary = df.groupby('segment').agg({
                'customer_id': 'count',
                'lifetime_value': ['mean', 'sum', 'min', 'max'],
                'total_orders': 'mean',
                'avg_order_value': 'mean'
            }).round(2)
            
            segment_path = self.output_dir / f"segment_summary_{timestamp}.csv"
            segment_summary.to_csv(str(segment_path))
            exports['segment_summary'] = str(segment_path)
        
        LOG.info(f"Exported customer analysis: {len(exports)} files")
        return exports
    
    def export_multiple_artifacts(
        self,
        artifacts: List[DuckDBArtifact],
        formats: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, str]]:
        """
        Export multiple artifacts in specified formats.
        
        Args:
            artifacts: List of artifacts to export
            formats: List of formats to export ('csv', 'parquet', 'json')
            
        Returns:
            Dictionary mapping artifact names to their export info
        """
        if formats is None:
            formats = ['csv']
        
        LOG.info(f"Exporting {len(artifacts)} artifacts in formats: {formats}")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        all_exports = {}
        
        for artifact in artifacts:
            artifact_exports = {}
            
            for format_type in formats:
                filename = f"{artifact.name}_{timestamp}.{format_type}"
                filepath = self.output_dir / filename
                
                try:
                    if format_type == 'csv':
                        artifact.to_csv(str(filepath))
                    elif format_type == 'parquet':
                        artifact.to_parquet(str(filepath))
                    elif format_type == 'json':
                        df = artifact.as_dataframe()
                        df.to_json(str(filepath), orient='records', date_format='iso', indent=2)
                    else:
                        LOG.warning(f"Unsupported format: {format_type}")
                        continue
                    
                    artifact_exports[format_type] = str(filepath)
                    LOG.info(f"Exported {artifact.name} to {filepath}")
                    
                except Exception as e:
                    LOG.error(f"Failed to export {artifact.name} as {format_type}: {e}")
            
            all_exports[artifact.name] = artifact_exports
        
        return all_exports
    
    def create_analytics_report(
        self,
        artifacts: List[DuckDBArtifact],
        pipeline_name: str = "duckdb_analytics"
    ) -> Dict[str, Any]:
        """
        Create a comprehensive analytics report.
        
        Args:
            artifacts: List of artifacts to include in report
            pipeline_name: Name of the pipeline
            
        Returns:
            Dictionary with report summary
        """
        LOG.info("Creating comprehensive analytics report")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create report summary
        report = {
            'pipeline': pipeline_name,
            'execution_time': str(datetime.now()),
            'timestamp': timestamp,
            'artifacts_created': len(artifacts),
            'artifacts': [],
            'output_files': [],
            'summary_statistics': {}
        }
        
        # Process each artifact
        for artifact in artifacts:
            df = artifact.as_dataframe()
            
            artifact_info = {
                'name': artifact.name,
                'records': len(df),
                'columns': list(df.columns),
                'memory_usage': df.memory_usage(deep=True).sum(),
                'metadata': artifact.metadata
            }
            
            report['artifacts'].append(artifact_info)
            
            # Export artifact
            csv_path = self.output_dir / f"{artifact.name}_{timestamp}.csv"
            artifact.to_csv(str(csv_path))
            report['output_files'].append(str(csv_path))
        
        # Calculate summary statistics
        total_records = sum(len(a.as_dataframe()) for a in artifacts)
        total_memory = sum(a.as_dataframe().memory_usage(deep=True).sum() for a in artifacts)
        
        report['summary_statistics'] = {
            'total_records_processed': total_records,
            'total_memory_usage_bytes': total_memory,
            'avg_records_per_artifact': total_records // len(artifacts) if artifacts else 0
        }
        
        # Save report as JSON
        report_path = self.output_dir / f"analytics_report_{timestamp}.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        report['report_file'] = str(report_path)
        
        # Print summary
        self._print_report_summary(report)
        
        LOG.info(f"Analytics report created: {report_path}")
        return report
    
    def export_for_visualization(
        self,
        artifacts: List[DuckDBArtifact],
        viz_format: str = "json"
    ) -> Dict[str, str]:
        """
        Export artifacts in formats suitable for visualization tools.
        
        Args:
            artifacts: List of artifacts to export for visualization
            viz_format: Format for visualization ('json', 'csv')
            
        Returns:
            Dictionary with visualization export paths
        """
        LOG.info(f"Exporting {len(artifacts)} artifacts for visualization")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        viz_dir = self.output_dir / "visualization"
        viz_dir.mkdir(exist_ok=True)
        
        viz_exports = {}
        
        for artifact in artifacts:
            df = artifact.as_dataframe()
            
            if viz_format == 'json':
                # Export as JSON with date formatting for viz tools
                viz_path = viz_dir / f"{artifact.name}_viz_{timestamp}.json"
                df.to_json(str(viz_path), orient='records', date_format='iso')
                viz_exports[artifact.name] = str(viz_path)
                
            elif viz_format == 'csv':
                # Export as CSV for general visualization tools
                viz_path = viz_dir / f"{artifact.name}_viz_{timestamp}.csv"
                df.to_csv(str(viz_path), index=False)
                viz_exports[artifact.name] = str(viz_path)
        
        LOG.info(f"Visualization exports created in: {viz_dir}")
        return viz_exports
    
    def cleanup_old_exports(self, days_to_keep: int = 7) -> Dict[str, int]:
        """
        Clean up old export files based on age.
        
        Args:
            days_to_keep: Number of days of exports to keep
            
        Returns:
            Dictionary with cleanup statistics
        """
        LOG.info(f"Cleaning up exports older than {days_to_keep} days")
        
        cutoff_time = datetime.now().timestamp() - (days_to_keep * 24 * 60 * 60)
        
        deleted_files = 0
        total_files = 0
        freed_bytes = 0
        
        for file_path in self.output_dir.rglob('*'):
            if file_path.is_file():
                total_files += 1
                if file_path.stat().st_mtime < cutoff_time:
                    file_size = file_path.stat().st_size
                    file_path.unlink()
                    deleted_files += 1
                    freed_bytes += file_size
                    LOG.debug(f"Deleted old export: {file_path}")
        
        cleanup_stats = {
            'total_files_checked': total_files,
            'deleted_files': deleted_files,
            'freed_bytes': freed_bytes,
            'freed_mb': round(freed_bytes / (1024 * 1024), 2)
        }
        
        LOG.info(f"Cleanup complete: deleted {deleted_files} files, freed {cleanup_stats['freed_mb']} MB")
        return cleanup_stats
    
    def _print_report_summary(self, report: Dict[str, Any]):
        """Print a formatted report summary."""
        print(f"\n{'='*60}")
        print(f"ANALYTICS EXPORT SUMMARY")
        print(f"{'='*60}")
        print(f"Pipeline: {report['pipeline']}")
        print(f"Execution Time: {report['execution_time']}")
        print(f"Artifacts Created: {report['artifacts_created']}")
        print(f"Output Files: {len(report['output_files'])}")
        
        if 'summary_statistics' in report:
            stats = report['summary_statistics']
            print(f"\nStatistics:")
            print(f"  Total Records: {stats.get('total_records_processed', 0):,}")
            print(f"  Memory Usage: {stats.get('total_memory_usage_bytes', 0):,} bytes")
            print(f"  Avg Records/Artifact: {stats.get('avg_records_per_artifact', 0):,}")
        
        print(f"\nOutput Directory: {self.output_dir}")
        print(f"{'='*60}")