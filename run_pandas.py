"""
Run the data pipeline using Pandas (no Java/Spark required).
This demonstrates the same pipeline logic without Spark dependencies.
"""
import sys
import json
import time
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from utils.data_generator import EcommerceDataGenerator


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

    df = pd.DataFrame(transactions)

    total = len(df)
    valid = df.dropna(subset=['transaction_id', 'user_id', 'total_price']).shape[0]
    validity_rate = valid / total if total > 0 else 0

    print(f"      Total records: {total}")
    print(f"      Valid records: {valid}")
    print(f"      Validity rate: {validity_rate:.2%}")

    return {"total": total, "valid": valid, "rate": validity_rate}


def process_data(transactions: list) -> pd.DataFrame:
    """Process data with Pandas transformations."""
    print("\n[3/5] Processing data...")
    start_time = time.time()

    # Create DataFrame
    df = pd.DataFrame(transactions)
    print(f"      Created DataFrame with {len(df)} records")

    # Parse timestamp
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Add time dimensions
    df['transaction_date'] = df['timestamp'].dt.date
    df['transaction_year'] = df['timestamp'].dt.year
    df['transaction_month'] = df['timestamp'].dt.month
    df['transaction_day'] = df['timestamp'].dt.day
    df['transaction_hour'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.dayofweek

    # Flatten shipping address
    df['shipping_city'] = df['shipping_address'].apply(lambda x: x.get('city') if x else None)
    df['shipping_state'] = df['shipping_address'].apply(lambda x: x.get('state') if x else None)
    df['shipping_country'] = df['shipping_address'].apply(lambda x: x.get('country') if x else None)
    df['shipping_zip'] = df['shipping_address'].apply(lambda x: x.get('zip_code') if x else None)
    df = df.drop(columns=['shipping_address'])

    # Remove duplicates
    df = df.drop_duplicates(subset=['transaction_id'])

    elapsed = time.time() - start_time
    print(f"      Processed {len(df)} records in {elapsed:.2f} seconds")
    print(f"      Throughput: {len(df)/elapsed:.0f} records/second")

    return df


def generate_analytics(df: pd.DataFrame):
    """Generate analytics reports."""
    print("\n[4/5] Generating analytics...")

    # Category aggregation
    print("\n      === Revenue by Category ===")
    category_agg = df.groupby('product_category').agg({
        'transaction_id': 'count',
        'quantity': 'sum',
        'total_price': ['sum', 'mean', 'min', 'max'],
        'user_id': 'nunique'
    }).round(2)
    category_agg.columns = ['transactions', 'units_sold', 'total_revenue', 'avg_value', 'min_value', 'max_value', 'unique_customers']
    category_agg = category_agg.sort_values('total_revenue', ascending=False)
    print(category_agg.to_string())

    # Daily aggregation
    print("\n      === Daily Summary ===")
    daily_agg = df.groupby('transaction_date').agg({
        'transaction_id': 'count',
        'total_price': 'sum',
        'user_id': 'nunique'
    }).round(2)
    daily_agg.columns = ['transactions', 'revenue', 'unique_customers']
    print(daily_agg.to_string())

    # Status breakdown
    print("\n      === Transaction Status ===")
    status_agg = df.groupby('status').agg({
        'transaction_id': 'count',
        'total_price': 'sum'
    }).round(2)
    status_agg.columns = ['count', 'total_value']
    status_agg['percentage'] = (status_agg['count'] / status_agg['count'].sum() * 100).round(2)
    print(status_agg.to_string())

    # Payment method breakdown
    print("\n      === Payment Methods ===")
    payment_agg = df.groupby('payment_method')['transaction_id'].count()
    print(payment_agg.sort_values(ascending=False).to_string())

    # Hourly distribution
    print("\n      === Transactions by Hour ===")
    hourly = df.groupby('transaction_hour')['transaction_id'].count()
    print(hourly.to_string())

    return {
        "category": category_agg,
        "daily": daily_agg,
        "status": status_agg
    }


def save_results(df: pd.DataFrame, output_dir: Path):
    """Save processed data to local storage."""
    print("\n[5/5] Saving results...")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Save processed data as CSV
    processed_path = output_dir / "processed_transactions.csv"
    df.to_csv(processed_path, index=False)
    print(f"      Saved processed data to: {processed_path}")

    # Save as Parquet (if pyarrow is available)
    try:
        parquet_path = output_dir / "processed_transactions.parquet"
        df.to_parquet(parquet_path, index=False)
        print(f"      Saved Parquet to: {parquet_path}")
    except Exception as e:
        print(f"      (Parquet save skipped: {e})")

    # Save summary statistics
    summary = df[['total_price', 'quantity', 'unit_price']].describe().round(2)
    summary_path = output_dir / "summary_stats.csv"
    summary.to_csv(summary_path)
    print(f"      Saved summary to: {summary_path}")

    # Save category report
    category_report = df.groupby('product_category').agg({
        'total_price': 'sum',
        'transaction_id': 'count'
    }).round(2)
    category_report.columns = ['total_revenue', 'transaction_count']
    category_path = output_dir / "category_report.csv"
    category_report.to_csv(category_path)
    print(f"      Saved category report to: {category_path}")

    # Save status report
    status_report = df.groupby('status').agg({
        'transaction_id': 'count',
        'total_price': 'sum'
    }).round(2)
    status_path = output_dir / "status_report.csv"
    status_report.to_csv(status_path)
    print(f"      Saved status report to: {status_path}")


def print_summary_stats(df: pd.DataFrame):
    """Print overall summary statistics."""
    print("\n" + "=" * 60)
    print("  PIPELINE SUMMARY")
    print("=" * 60)

    total_revenue = df['total_price'].sum()
    total_transactions = len(df)
    unique_customers = df['user_id'].nunique()
    unique_products = df['product_id'].nunique()
    avg_order_value = df['total_price'].mean()

    completed = len(df[df['status'] == 'completed'])
    completion_rate = completed / total_transactions * 100

    print(f"""
  Total Transactions:    {total_transactions:,}
  Total Revenue:         ${total_revenue:,.2f}
  Unique Customers:      {unique_customers:,}
  Unique Products:       {unique_products:,}
  Avg Order Value:       ${avg_order_value:.2f}
  Completion Rate:       {completion_rate:.1f}%

  Top Category:          {df.groupby('product_category')['total_price'].sum().idxmax()}
  Top Payment Method:    {df['payment_method'].mode().iloc[0]}
  Most Active Hour:      {df['transaction_hour'].mode().iloc[0]}:00
    """)


def main():
    """Run the complete local pipeline."""
    print("=" * 60)
    print("  E-Commerce Data Pipeline (Pandas Version)")
    print("=" * 60)

    # Setup paths
    base_path = Path(__file__).parent
    output_dir = base_path / "data" / "output"

    try:
        # Step 1: Generate data
        transactions = generate_data(num_records=5000)

        # Step 2: Validate
        validation = validate_data(transactions)

        # Step 3: Process
        df = process_data(transactions)

        # Step 4: Generate analytics
        analytics = generate_analytics(df)

        # Step 5: Save results
        save_results(df, output_dir)

        # Print summary
        print_summary_stats(df)

        print("=" * 60)
        print("  Pipeline completed successfully!")
        print("=" * 60)
        print(f"\n  Output directory: {output_dir}")
        print("\n  Files created:")
        for f in output_dir.glob("*"):
            size = f.stat().st_size / 1024
            print(f"    - {f.name} ({size:.1f} KB)")

    except Exception as e:
        print(f"\n  ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
