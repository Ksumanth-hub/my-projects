"""
Tests for data generator module.
"""
import pytest
import sys
sys.path.insert(0, 'src')

from utils.data_generator import EcommerceDataGenerator, generate_transaction, generate_batch
from kafka.schemas import validate_transaction


class TestEcommerceDataGenerator:
    """Test cases for EcommerceDataGenerator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.generator = EcommerceDataGenerator(seed=42)

    def test_generate_single_transaction(self):
        """Test generating a single transaction."""
        tx = self.generator.generate_transaction()

        assert 'transaction_id' in tx
        assert 'user_id' in tx
        assert 'product_id' in tx
        assert 'product_category' in tx
        assert 'quantity' in tx
        assert 'unit_price' in tx
        assert 'total_price' in tx
        assert 'status' in tx
        assert 'timestamp' in tx

    def test_transaction_validates(self):
        """Test that generated transactions pass validation."""
        tx = self.generator.generate_transaction()
        assert validate_transaction(tx) is True

    def test_generate_batch(self):
        """Test generating a batch of transactions."""
        batch = self.generator.generate_batch(batch_size=100)

        assert len(batch) == 100
        for tx in batch:
            assert 'transaction_id' in tx

    def test_quantity_is_positive(self):
        """Test that quantity is always positive."""
        for _ in range(100):
            tx = self.generator.generate_transaction()
            assert tx['quantity'] >= 1

    def test_prices_are_positive(self):
        """Test that prices are always positive."""
        for _ in range(100):
            tx = self.generator.generate_transaction()
            assert tx['unit_price'] > 0
            assert tx['total_price'] > 0

    def test_valid_product_category(self):
        """Test that product category is from allowed list."""
        valid_categories = [
            "Electronics", "Clothing", "Home & Garden",
            "Sports & Outdoors", "Books", "Toys & Games",
            "Health & Beauty", "Food & Grocery",
            "Automotive", "Office Supplies"
        ]

        for _ in range(100):
            tx = self.generator.generate_transaction()
            assert tx['product_category'] in valid_categories

    def test_valid_status(self):
        """Test that status is from allowed list."""
        valid_statuses = ["completed", "pending", "failed", "refunded"]

        for _ in range(100):
            tx = self.generator.generate_transaction()
            assert tx['status'] in valid_statuses

    def test_reproducibility_with_seed(self):
        """Test that same seed produces same results."""
        gen1 = EcommerceDataGenerator(seed=123)
        gen2 = EcommerceDataGenerator(seed=123)

        tx1 = gen1.generate_transaction()
        tx2 = gen2.generate_transaction()

        assert tx1['transaction_id'] == tx2['transaction_id']


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    def test_generate_transaction_function(self):
        """Test generate_transaction convenience function."""
        tx = generate_transaction()
        assert 'transaction_id' in tx
        assert validate_transaction(tx) is True

    def test_generate_batch_function(self):
        """Test generate_batch convenience function."""
        batch = generate_batch(50)
        assert len(batch) == 50


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
