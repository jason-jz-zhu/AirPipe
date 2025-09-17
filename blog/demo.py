"""
Blog Demo: Complete example showing the evolution from monolithic to AirPipe.

This demo shows:
1. Traditional monolithic approach (what NOT to do)
2. AirPipe modular approach 
3. AI-powered natural language approach
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import asyncio
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

# ============================================================================
# PART 1: The Old Way (Monolithic Script) - DON'T DO THIS!
# ============================================================================

def monolithic_etl_pipeline():
    """
    Traditional approach: Everything in one giant function.
    This is what we're trying to avoid!
    """
    print("\n" + "="*60)
    print("MONOLITHIC APPROACH (The Problem)")
    print("="*60)
    
    try:
        # --- EXTRACTION PHASE (lines 1-500 in real pipelines) ---
        print("\nExtracting data...")
        
        # Read customers
        customers_df = pd.read_csv("data/customers.csv")
        print(f"Loaded {len(customers_df)} customers")
        
        # Read orders
        orders_df = pd.read_csv("data/orders.csv")
        print(f"Loaded {len(orders_df)} orders")
        
        # Read products
        products_df = pd.read_csv("data/products.csv")
        print(f"Loaded {len(products_df)} products")
        
    except Exception as e:
        print(f"ERROR in extraction: {e}")
        return  # Entire pipeline fails!
    
    try:
        # --- TRANSFORMATION PHASE (lines 501-2000 in real pipelines) ---
        print("\nTransforming data...")
        
        # Join everything together
        merged_df = pd.merge(orders_df, customers_df, on='customer_id', how='left')
        merged_df = pd.merge(merged_df, products_df, on='product_id', how='left')
        
        # Clean data
        merged_df = merged_df.dropna(subset=['order_amount'])
        merged_df['order_date'] = pd.to_datetime(merged_df['order_date'])
        
        # Business logic - calculate metrics
        merged_df['profit'] = merged_df['order_amount'] * 0.3  # 30% margin
        merged_df['days_since_order'] = (datetime.now() - merged_df['order_date']).dt.days
        
        # Filter recent orders
        recent_df = merged_df[merged_df['days_since_order'] <= 30]
        
        # Aggregate by customer
        customer_summary = recent_df.groupby('customer_id').agg({
            'order_amount': ['sum', 'mean', 'count'],
            'profit': 'sum'
        })
        
        # Flatten column names
        customer_summary.columns = ['_'.join(col).strip() for col in customer_summary.columns]
        
        # Add customer segments
        def segment_customer(row):
            if row['order_amount_sum'] > 10000:
                return 'Gold'
            elif row['order_amount_sum'] > 5000:
                return 'Silver'
            else:
                return 'Bronze'
        
        customer_summary['segment'] = customer_summary.apply(segment_customer, axis=1)
        
        # Aggregate by product
        product_summary = recent_df.groupby('product_name').agg({
            'order_amount': 'sum',
            'profit': 'sum',
            'order_id': 'count'
        }).rename(columns={'order_id': 'order_count'})
        
    except Exception as e:
        print(f"ERROR in transformation: {e}")
        return  # Can't debug easily - which transformation failed?
    
    try:
        # --- LOADING PHASE (lines 2001-3000 in real pipelines) ---
        print("\nLoading data...")
        
        # Save to CSV
        customer_summary.to_csv("output/monolithic_customer_summary.csv")
        product_summary.to_csv("output/monolithic_product_summary.csv")
        
        # Print summary
        print("\nCustomer Segment Summary:")
        print(customer_summary.groupby('segment').size())
        
        print("\nTop 5 Products:")
        print(product_summary.nlargest(5, 'order_amount')[['order_amount', 'order_count']])
        
    except Exception as e:
        print(f"ERROR in loading: {e}")
        return
    
    print("\n❌ Problems with this approach:")
    print("  - Can't test individual components")
    print("  - No parallelization possible")
    print("  - Hard to debug when it fails")
    print("  - No visibility into data flow")
    print("  - Everything fails if one part fails")


# ============================================================================
# PART 2: The AirPipe Way (Modular and Clear)
# ============================================================================

def airpipe_modular_pipeline():
    """
    AirPipe approach: Clean, modular, testable tasks.
    """
    print("\n" + "="*60)
    print("AIRPIPE MODULAR APPROACH (The Solution)")
    print("="*60)
    
    from airpipe.core.task import TaskPipeline
    
    # Create pipeline with clear name
    pipeline = TaskPipeline("customer_analytics")
    
    # Task 1: Extract customers
    @pipeline.task(produces="customers")
    def extract_customers():
        """Extract customer data."""
        LOG.info("Extracting customers...")
        df = pd.read_csv("data/customers.csv")
        LOG.info(f"Extracted {len(df)} customers")
        return pipeline.create_artifact(df, "customers")
    
    # Task 2: Extract orders (runs in parallel with Task 1)
    @pipeline.task(produces="orders")
    def extract_orders():
        """Extract order data."""
        LOG.info("Extracting orders...")
        df = pd.read_csv("data/orders.csv")
        LOG.info(f"Extracted {len(df)} orders")
        return pipeline.create_artifact(df, "orders")
    
    # Task 3: Extract products (runs in parallel with Tasks 1 & 2)
    @pipeline.task(produces="products")
    def extract_products():
        """Extract product data."""
        LOG.info("Extracting products...")
        df = pd.read_csv("data/products.csv")
        LOG.info(f"Extracted {len(df)} products")
        return pipeline.create_artifact(df, "products")
    
    # Task 4: Join data (waits for all extractions)
    @pipeline.task(
        depends_on=["extract_customers", "extract_orders", "extract_products"],
        consumes=["customers", "orders", "products"],
        produces="merged_data"
    )
    def join_data():
        """Join all data sources."""
        LOG.info("Joining data...")
        customers = pipeline.get_artifact("customers").as_dataframe()
        orders = pipeline.get_artifact("orders").as_dataframe()
        products = pipeline.get_artifact("products").as_dataframe()
        
        merged = pd.merge(orders, customers, on='customer_id', how='left')
        merged = pd.merge(merged, products, on='product_id', how='left')
        
        LOG.info(f"Joined data: {len(merged)} records")
        return pipeline.create_artifact(merged, "merged_data")
    
    # Task 5: Clean and filter
    @pipeline.task(
        consumes="merged_data",
        produces="clean_data"
    )
    def clean_and_filter():
        """Clean data and filter recent orders."""
        LOG.info("Cleaning and filtering...")
        df = pipeline.get_artifact("merged_data").as_dataframe()
        
        # Clean
        df = df.dropna(subset=['order_amount'])
        df['order_date'] = pd.to_datetime(df['order_date'])
        
        # Calculate metrics
        df['profit'] = df['order_amount'] * 0.3
        df['days_since_order'] = (datetime.now() - df['order_date']).dt.days
        
        # Filter recent
        recent = df[df['days_since_order'] <= 30]
        
        LOG.info(f"Cleaned data: {len(recent)} recent orders")
        return pipeline.create_artifact(recent, "clean_data")
    
    # Task 6: Customer segmentation (can run in parallel with Task 7)
    @pipeline.task(
        consumes="clean_data",
        produces="customer_segments"
    )
    def segment_customers():
        """Calculate customer segments."""
        LOG.info("Segmenting customers...")
        df = pipeline.get_artifact("clean_data").as_dataframe()
        
        # Aggregate by customer
        summary = df.groupby('customer_id').agg({
            'order_amount': ['sum', 'mean', 'count'],
            'profit': 'sum'
        })
        
        summary.columns = ['_'.join(col).strip() for col in summary.columns]
        
        # Add segments
        def segment(row):
            if row['order_amount_sum'] > 10000:
                return 'Gold'
            elif row['order_amount_sum'] > 5000:
                return 'Silver'
            else:
                return 'Bronze'
        
        summary['segment'] = summary.apply(segment, axis=1)
        
        LOG.info(f"Segmented {len(summary)} customers")
        return pipeline.create_artifact(summary, "customer_segments")
    
    # Task 7: Product analysis (can run in parallel with Task 6)
    @pipeline.task(
        consumes="clean_data",
        produces="product_analysis"
    )
    def analyze_products():
        """Analyze product performance."""
        LOG.info("Analyzing products...")
        df = pipeline.get_artifact("clean_data").as_dataframe()
        
        summary = df.groupby('product_name').agg({
            'order_amount': 'sum',
            'profit': 'sum',
            'order_id': 'count'
        }).rename(columns={'order_id': 'order_count'})
        
        LOG.info(f"Analyzed {len(summary)} products")
        return pipeline.create_artifact(summary, "product_analysis")
    
    # Task 8: Generate reports (waits for both analyses)
    @pipeline.task(
        depends_on=["segment_customers", "analyze_products"],
        consumes=["customer_segments", "product_analysis"]
    )
    def generate_reports():
        """Generate and save reports."""
        LOG.info("Generating reports...")
        
        customers = pipeline.get_artifact("customer_segments").as_dataframe()
        products = pipeline.get_artifact("product_analysis").as_dataframe()
        
        # Save files
        customers.to_csv("output/airpipe_customer_segments.csv")
        products.to_csv("output/airpipe_product_analysis.csv")
        
        # Print summaries
        print("\n✅ Customer Segments:")
        print(customers.groupby('segment').size())
        
        print("\n✅ Top 5 Products:")
        print(products.nlargest(5, 'order_amount')[['order_amount', 'order_count']])
        
        LOG.info("Reports generated successfully")
    
    # Execute pipeline
    print("\n🚀 Executing pipeline with automatic parallelization...")
    results = pipeline.execute(parallel=True)
    
    # Show DAG
    print("\n📊 Pipeline DAG Structure:")
    print(pipeline.visualize_dag(format='ascii'))
    
    # Show statistics
    stats = pipeline.get_task_statistics()
    print(f"\n📈 Pipeline Statistics:")
    print(f"  - Total tasks: {stats['total_tasks']}")
    print(f"  - Critical path length: {stats['critical_path_length']}")
    print(f"  - Parallelization achieved: {stats['total_tasks'] - stats['critical_path_length']} tasks")
    
    print("\n✅ Benefits of AirPipe approach:")
    print("  - Each task is testable in isolation")
    print("  - Automatic parallelization (3 extractions run simultaneously)")
    print("  - Clear dependencies and data flow")
    print("  - Easy to debug individual tasks")
    print("  - Partial failures don't break everything")


# ============================================================================
# PART 3: The AI-Powered Way (Natural Language)
# ============================================================================

async def airpipe_ai_pipeline():
    """
    AI-powered approach: Create pipeline from natural language.
    """
    print("\n" + "="*60)
    print("AI-POWERED APPROACH (The Future)")
    print("="*60)
    
    from airpipe.agents import OrchestratorAgent
    
    # Create orchestrator agent
    orchestrator = OrchestratorAgent()
    
    # Describe what you want in plain English
    pipeline_description = """
    Create a customer analytics pipeline that:
    1. Extracts data from three CSV files: customers.csv, orders.csv, and products.csv
    2. Joins all three datasets together
    3. Cleans the data by removing null order amounts
    4. Filters for orders from the last 30 days
    5. Calculates profit as 30% of order amount
    6. Segments customers into Bronze (<$5000), Silver ($5000-10000), and Gold (>$10000) based on total spending
    7. Analyzes product performance by calculating total sales and order count
    8. Generates reports showing customer segments and top 5 products
    9. Saves results to CSV files
    """
    
    print("\n🤖 Creating pipeline from natural language description...")
    print("\nDescription:")
    print(pipeline_description)
    
    # Create pipeline from description
    pipeline = await orchestrator.create_pipeline_from_description(
        description=pipeline_description,
        name="ai_customer_analytics"
    )
    
    print(f"\n✨ AI created pipeline with {len(pipeline.tasks)} tasks!")
    
    # Show what AI created
    print("\n📋 Tasks created by AI:")
    for task_name, task in pipeline.tasks.items():
        deps = f" (depends on: {', '.join(task.dependencies)})" if task.dependencies else ""
        print(f"  - {task_name}{deps}")
    
    # Execute the AI-generated pipeline
    print("\n🚀 Executing AI-generated pipeline...")
    results = pipeline.execute(parallel=True)
    
    print(f"\n✅ AI Pipeline Results:")
    print(f"  - Tasks executed: {results['tasks_executed']}")
    print(f"  - Artifacts created: {results['artifacts_created']}")
    
    print("\n🎯 Benefits of AI-powered approach:")
    print("  - No coding required for standard pipelines")
    print("  - Natural language understanding")
    print("  - Automatic optimization")
    print("  - Self-documenting")
    print("  - Adapts to your description")


# ============================================================================
# Helper: Create sample data
# ============================================================================

def create_sample_data():
    """Create sample CSV files for demo."""
    import os
    
    # Create directories
    os.makedirs("data", exist_ok=True)
    os.makedirs("output", exist_ok=True)
    
    # Create customers
    customers = pd.DataFrame({
        'customer_id': range(1, 101),
        'customer_name': [f'Customer_{i}' for i in range(1, 101)],
        'email': [f'customer{i}@example.com' for i in range(1, 101)],
        'join_date': pd.date_range('2023-01-01', periods=100, freq='D')
    })
    customers.to_csv('data/customers.csv', index=False)
    
    # Create products
    products = pd.DataFrame({
        'product_id': range(1, 21),
        'product_name': [f'Product_{chr(65+i)}' for i in range(20)],
        'category': np.random.choice(['Electronics', 'Clothing', 'Food', 'Books'], 20),
        'price': np.random.uniform(10, 1000, 20)
    })
    products.to_csv('data/products.csv', index=False)
    
    # Create orders
    np.random.seed(42)
    orders = pd.DataFrame({
        'order_id': range(1, 501),
        'customer_id': np.random.randint(1, 101, 500),
        'product_id': np.random.randint(1, 21, 500),
        'order_date': pd.date_range(
            end=datetime.now(), 
            periods=500, 
            freq='H'
        ),
        'order_amount': np.random.uniform(50, 2000, 500)
    })
    orders.to_csv('data/orders.csv', index=False)
    
    print("✓ Sample data created in data/ directory")


# ============================================================================
# Main Demo Runner
# ============================================================================

def main():
    """Run all three approaches for comparison."""
    
    print("\n" + "="*70)
    print(" "*20 + "AIRPIPE DEMO: FROM CHAOS TO CLARITY")
    print("="*70)
    
    # Create sample data
    print("\n📁 Setting up sample data...")
    create_sample_data()
    
    # Run demos
    print("\n" + "="*70)
    print(" "*15 + "DEMO 1: The Problem (Monolithic Approach)")
    print("="*70)
    
    try:
        monolithic_etl_pipeline()
    except Exception as e:
        print(f"\n❌ Monolithic pipeline failed: {e}")
    
    print("\n" + "="*70)
    print(" "*15 + "DEMO 2: The Solution (AirPipe Modular)")
    print("="*70)
    
    try:
        airpipe_modular_pipeline()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("Make sure AirPipe is installed: pip install -e .")
    
    print("\n" + "="*70)
    print(" "*15 + "DEMO 3: The Future (AI-Powered)")
    print("="*70)
    
    try:
        asyncio.run(airpipe_ai_pipeline())
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("Note: AI features require the agent system to be running")
    
    print("\n" + "="*70)
    print(" "*20 + "DEMO COMPLETE")
    print("="*70)
    print("\n🎉 You've seen the evolution from chaos to clarity!")
    print("\n📚 Learn more at: github.com/yourusername/airpipe")


if __name__ == "__main__":
    main()