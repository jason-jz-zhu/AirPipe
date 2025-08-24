"""
Simple task-based workflow example.

This demonstrates clean separation of orchestration logic from business logic.
All business logic resides in the extractors, transformers, and loaders.
"""

from pathlib import Path
import sys
# Add both the project root and the pipelines directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))
sys.path.append(str(Path(__file__).parent.parent))

import logging
from airpipe.core.task import TaskPipeline
from examples.simple.extractors.sample_data_extractor import SampleDataExtractor
from examples.simple.transformers.value_transformer import ValueTransformer
from examples.simple.loaders.csv_loader import SimpleCSVLoader

# Setup
LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create pipeline
pipeline = TaskPipeline("simple_etl")

# Initialize components
extractor = SampleDataExtractor()
transformer = ValueTransformer()
loader = SimpleCSVLoader()

@pipeline.task(produces="raw_data")
def extract():
    """Extract data from source."""
    LOG.info("Extracting data...")
    
    # Use extractor component
    data = extractor.extract_sample_data(num_records=100)
    
    return pipeline.create_artifact(data, "raw_data")

@pipeline.task(
    depends_on=["extract"],
    consumes="raw_data",
    produces="transformed_data"
)
def transform():
    """Transform the data."""
    LOG.info("Transforming data...")
    
    raw_data = pipeline.get_artifact("raw_data")
    df = raw_data.as_dataframe()
    
    # Use transformer component
    transformed = transformer.filter_and_transform(df, threshold=500, transformation='square')
    
    return pipeline.create_artifact(transformed, "transformed_data")

@pipeline.task(
    depends_on=["transform"],
    consumes="transformed_data"
)
def load():
    """Load data to destination."""
    LOG.info("Loading data...")
    
    transformed_data = pipeline.get_artifact("transformed_data")
    df = transformed_data.as_dataframe()
    
    # Use loader component
    loader.save_results(df, output_path="output/simple_output.csv")

def main():
    """Run the simple workflow."""
    LOG.info("Starting simple ETL workflow")
    
    # Execute pipeline - framework handles everything!
    results = pipeline.execute(parallel=True)
    
    LOG.info(f"\nPipeline complete!")
    LOG.info(f"Tasks executed: {results['tasks_executed']}")
    LOG.info(f"Artifacts created: {results['artifacts_created']}")
    
    return results

if __name__ == "__main__":
    main()