"""Sales report loader."""

import pandas as pd
from pathlib import Path
import logging

LOG = logging.getLogger(__name__)


class ReportLoader:
    """Load and save sales analysis reports."""
    
    def save_all_results(self, artifacts: dict, output_dir: str = "output/advanced_analysis"):
        """Save all analysis results to files.
        
        Args:
            artifacts: Dictionary of artifact name to DataArtifact objects
            output_dir: Directory to save results to
        """
        LOG.info("Saving all results...")
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        for name, artifact in artifacts.items():
            df = artifact.as_dataframe()
            
            # Save as CSV
            csv_path = output_path / f"{name}.csv"
            df.to_csv(csv_path, index=False)
            
            # Save as JSON for insights
            if name == 'business_insights':
                json_path = output_path / f"{name}.json"
                df.to_json(json_path, orient='records', indent=2, date_format='iso')
            
            LOG.info(f"Saved {name} ({len(df)} records)")
        
        LOG.info(f"All results saved to {output_path}")
        return str(output_path)
    
    def print_executive_summary(self, insights_df: pd.DataFrame, products_df: pd.DataFrame, 
                               regional_df: pd.DataFrame):
        """Print executive summary of analysis.
        
        Args:
            insights_df: Business insights dataframe
            products_df: Product rankings dataframe  
            regional_df: Regional metrics dataframe
        """
        print("\n" + "="*70)
        print("EXECUTIVE SUMMARY - ADVANCED ANALYTICS PIPELINE")
        print("="*70)
        
        # Print insights
        print("\nKey Insights:")
        for _, row in insights_df.iterrows():
            print(f"  • {row['insight']}")
        
        # Top products
        print("\nTop 5 Products by Revenue:")
        for _, row in products_df.head(5).iterrows():
            print(f"  {row['rank']}. {row['product']}: ${row['revenue']:,.2f} ({row['revenue_percentage']}%)")
        
        # Regional performance
        print("\nRegional Performance:")
        for _, row in regional_df.iterrows():
            print(f"  • {row['region']}: ${row['total_sum']:,.2f} "
                  f"({row['num_transactions']} transactions)")
        
        print("="*70 + "\n")