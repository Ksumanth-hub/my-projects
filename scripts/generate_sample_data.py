"""
Script to generate sample transaction data for testing.
Run this to create test data without needing Kafka.
"""
import sys
import json
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from utils.data_generator import EcommerceDataGenerator


def generate_sample_files(output_dir: str = None, num_records: int = 1000):
    """
    Generate sample transaction data files.

    Args:
        output_dir: Output directory path
        num_records: Number of records to generate
    """
    output_path = Path(output_dir or Path(__file__).parent.parent / "data" / "raw")
    output_path.mkdir(parents=True, exist_ok=True)

    generator = EcommerceDataGenerator(seed=42)

    print(f"Generating {num_records} sample transactions...")

    # Generate transactions
    transactions = generator.generate_batch(num_records)

    # Save as JSON lines file
    output_file = output_path / "generated_transactions.json"
    with open(output_file, 'w') as f:
        for tx in transactions:
            f.write(json.dumps(tx) + '\n')

    print(f"Saved {num_records} transactions to {output_file}")

    # Also save a pretty-printed sample for viewing
    sample_file = output_path / "sample_pretty.json"
    with open(sample_file, 'w') as f:
        json.dump(transactions[:5], f, indent=2)

    print(f"Saved 5 sample transactions (pretty) to {sample_file}")

    # Print summary statistics
    categories = {}
    statuses = {}
    total_revenue = 0

    for tx in transactions:
        cat = tx['product_category']
        status = tx['status']
        categories[cat] = categories.get(cat, 0) + 1
        statuses[status] = statuses.get(status, 0) + 1
        total_revenue += tx['total_price']

    print("\n=== Data Summary ===")
    print(f"Total Records: {num_records}")
    print(f"Total Revenue: ${total_revenue:,.2f}")
    print(f"\nBy Category:")
    for cat, count in sorted(categories.items()):
        print(f"  {cat}: {count}")
    print(f"\nBy Status:")
    for status, count in sorted(statuses.items()):
        print(f"  {status}: {count}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate sample transaction data")
    parser.add_argument("--records", type=int, default=1000, help="Number of records")
    parser.add_argument("--output", type=str, default=None, help="Output directory")

    args = parser.parse_args()
    generate_sample_files(args.output, args.records)
