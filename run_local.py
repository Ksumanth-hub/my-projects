"""
Run the data pipeline locally without Docker.
This script demonstrates the full pipeline using local Spark.
"""
import sys
import json
import time
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from pyspark.sql import SparkSession
from config.settings import Settings
from utils.data_generator import EcommerceDataGenerator
from spark.transformations import (
    TRANSACTION_SCHEMA,
    prepare_for_analytics,
    aggregate_by_category,
    aggregate_by_time
)
from utils.validators import DataValidator


def create_spark_session():
    """Create local Spark session."""
    print("Creating Spark session...")
    return (
        SparkSession.builder
        .appName("EcommerceDataPipeline-Local")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def generate_data(num_records: int = 1000) -> list:
    """Generate sample transaction data."""
    print(f"\n[1/5] Generating {num_records} sample transactions...")
    generator = EcommerceDataGenerator(seed=42)
    transactions = generator.generate_batch(num_records)
    print(f"      Generated {len(transactions)} transactions")
    return transactions


def validate_data(transactions: list) -> dict:
    """Validate the generated data."""
    print("\n[2/5] Validating data quality...")
    validator = DataValidator()
    result = validator.validate_batch(transactions)

    print(f"      Total records: {result.metrics['total_records']}")
    print(f"      Valid records: {result.metrics['valid_records']}")
    print(f"      Validity rate: {result.metrics['validity_rate']:.2%}")

    return result.to_dict()


def process_with_spark(spark: SparkSession, transactions: list):
    """Process data with Spark transformations."""
    print("\n[3/5] Processing with Spark...")
    start_time = time.time()

    # Create DataFrame
    df = spark.createDataFrame(transactions, TRANSACTION_SCHEMA)
    print(f"      Created DataFrame with {df.count()} records")

    # Apply transformations
    processed_df = prepare_for_analytics(df)
    processed_df.cache()

    record_count = processed_df.count()
    elapsed = time.time() - start_time
    print(f"      Processed {record_count} records in {elapsed:.2f} seconds")
    print(f"      Throughput: {record_count/elapsed:.0f} records/second")

    return processed_df


def generate_analytics(spark: SparkSession, df):
    """Generate analytics reports."""
    print("\n[4/5] Generating analytics...")

    # Category aggregation
    print("      Aggregating by category...")
    category_agg = aggregate_by_category(df)

    print("\n      === Revenue by Category ===")
    category_agg.orderBy("total_revenue", ascending=False).show(10, truncate=False)

    # Daily aggregation
    print("      Aggregating by date...")
    daily_agg = aggregate_by_time(df, "transaction_date")

    print("\n      === Daily Summary ===")
    daily_agg.show(10, truncate=False)

    # Status breakdown
    print("      Calculating status breakdown...")
    status_agg = df.groupBy("status").count()

    print("\n      === Transaction Status ===")
    status_agg.show()

    return {
        "category": category_agg,
        "daily": daily_agg,
        "status": status_agg
    }


def save_results(spark: SparkSession, df, output_dir: Path):
    """Save processed data to local storage."""
    print("\n[5/5] Saving results...")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Save as Parquet
    parquet_path = output_dir / "processed_transactions"
    df.write.mode("overwrite").partitionBy("transaction_date").parquet(str(parquet_path))
    print(f"      Saved Parquet to: {parquet_path}")

    # Save summary as CSV
    summary = df.describe("total_price", "quantity", "unit_price").toPandas()
    csv_path = output_dir / "summary_stats.csv"
    summary.to_csv(csv_path, index=False)
    print(f"      Saved summary to: {csv_path}")

    # Save category report
    category_report = df.groupBy("product_category").agg({
        "total_price": "sum",
        "transaction_id": "count"
    }).toPandas()
    category_path = output_dir / "category_report.csv"
    category_report.to_csv(category_path, index=False)
    print(f"      Saved category report to: {category_path}")


def main():
    """Run the complete local pipeline."""
    print("=" * 60)
    print("  E-Commerce Data Pipeline - Local Execution")
    print("=" * 60)

    # Setup paths
    base_path = Path(__file__).parent
    output_dir = base_path / "data" / "output"

    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Step 1: Generate data
        transactions = generate_data(num_records=5000)

        # Step 2: Validate
        validation = validate_data(transactions)

        # Step 3: Process with Spark
        processed_df = process_with_spark(spark, transactions)

        # Step 4: Generate analytics
        analytics = generate_analytics(spark, processed_df)

        # Step 5: Save results
        save_results(spark, processed_df, output_dir)

        print("\n" + "=" * 60)
        print("  Pipeline completed successfully!")
        print("=" * 60)
        print(f"\n  Output directory: {output_dir}")
        print("\n  Files created:")
        print("    - processed_transactions/ (Parquet)")
        print("    - summary_stats.csv")
        print("    - category_report.csv")

    except Exception as e:
        print(f"\n  ERROR: {e}")
        raise

    finally:
        spark.stop()
        print("\n  Spark session stopped.")


if __name__ == "__main__":
    main()
