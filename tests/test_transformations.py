"""
Tests for Spark transformations module.
"""
import pytest
from datetime import datetime


class TestTransformations:
    """Test cases for Spark transformations."""

    @pytest.fixture
    def spark(self):
        """Create SparkSession for testing."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder \
            .appName("TransformationTests") \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()

        yield spark
        spark.stop()

    @pytest.fixture
    def sample_data(self, spark):
        """Create sample transaction data."""
        data = [
            {
                "transaction_id": "tx-001",
                "user_id": "user-001",
                "product_id": "prod-001",
                "product_name": "Test Product",
                "product_category": "Electronics",
                "quantity": 2,
                "unit_price": 99.99,
                "total_price": 199.98,
                "currency": "USD",
                "payment_method": "credit_card",
                "status": "completed",
                "timestamp": "2024-01-15T10:30:00",
                "shipping_address": {
                    "city": "New York",
                    "state": "NY",
                    "country": "US",
                    "zip_code": "10001"
                },
                "device_type": "mobile",
                "browser": "chrome"
            },
            {
                "transaction_id": "tx-002",
                "user_id": "user-002",
                "product_id": "prod-002",
                "product_name": "Another Product",
                "product_category": "Clothing",
                "quantity": 1,
                "unit_price": 49.99,
                "total_price": 49.99,
                "currency": "USD",
                "payment_method": "paypal",
                "status": "pending",
                "timestamp": "2024-01-15T11:00:00",
                "shipping_address": {
                    "city": "Los Angeles",
                    "state": "CA",
                    "country": "US",
                    "zip_code": "90001"
                },
                "device_type": "desktop",
                "browser": "firefox"
            }
        ]

        import sys
        sys.path.insert(0, 'src')
        from spark.transformations import TRANSACTION_SCHEMA

        return spark.createDataFrame(data, TRANSACTION_SCHEMA)

    def test_parse_timestamp(self, sample_data):
        """Test timestamp parsing."""
        import sys
        sys.path.insert(0, 'src')
        from spark.transformations import parse_timestamp

        result = parse_timestamp(sample_data)

        assert result.schema['timestamp'].dataType.simpleString() == 'timestamp'

    def test_add_time_dimensions(self, sample_data):
        """Test time dimension columns."""
        import sys
        sys.path.insert(0, 'src')
        from spark.transformations import parse_timestamp, add_time_dimensions

        result = sample_data.transform(parse_timestamp).transform(add_time_dimensions)

        columns = result.columns
        assert 'transaction_date' in columns
        assert 'transaction_year' in columns
        assert 'transaction_month' in columns
        assert 'transaction_day' in columns
        assert 'transaction_hour' in columns
        assert 'day_of_week' in columns

    def test_flatten_address(self, sample_data):
        """Test address flattening."""
        import sys
        sys.path.insert(0, 'src')
        from spark.transformations import flatten_address

        result = flatten_address(sample_data)

        columns = result.columns
        assert 'shipping_city' in columns
        assert 'shipping_state' in columns
        assert 'shipping_country' in columns
        assert 'shipping_zip' in columns
        assert 'shipping_address' not in columns

    def test_clean_data(self, sample_data):
        """Test data cleaning."""
        import sys
        sys.path.insert(0, 'src')
        from spark.transformations import clean_data

        result = clean_data(sample_data)

        # Should have same number of records (no nulls in sample data)
        assert result.count() == sample_data.count()

    def test_filter_completed_transactions(self, sample_data):
        """Test filtering completed transactions."""
        import sys
        sys.path.insert(0, 'src')
        from spark.transformations import filter_completed_transactions

        result = filter_completed_transactions(sample_data)

        # Only 1 completed transaction in sample data
        assert result.count() == 1
        assert result.first()['status'] == 'completed'

    def test_aggregate_by_category(self, sample_data):
        """Test category aggregation."""
        import sys
        sys.path.insert(0, 'src')
        from spark.transformations import aggregate_by_category

        result = aggregate_by_category(sample_data)

        # Should have 2 categories in sample data
        assert result.count() == 2

        columns = result.columns
        assert 'product_category' in columns
        assert 'total_transactions' in columns
        assert 'total_revenue' in columns

    def test_deduplicate_transactions(self, sample_data, spark):
        """Test deduplication."""
        import sys
        sys.path.insert(0, 'src')
        from spark.transformations import deduplicate_transactions, TRANSACTION_SCHEMA

        # Create data with duplicate
        dup_data = sample_data.union(sample_data.limit(1))
        assert dup_data.count() == 3

        result = deduplicate_transactions(dup_data)
        assert result.count() == 2


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
