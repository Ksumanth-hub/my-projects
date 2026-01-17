"""
Tests for data validators module.
"""
import pytest
import sys
sys.path.insert(0, 'src')

from utils.validators import DataValidator, ValidationResult


class TestDataValidator:
    """Test cases for DataValidator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.validator = DataValidator()

    def test_valid_transaction(self):
        """Test validation of a valid transaction."""
        tx = {
            "transaction_id": "tx-001",
            "user_id": "user-001",
            "product_id": "prod-001",
            "product_category": "Electronics",
            "quantity": 2,
            "unit_price": 99.99,
            "total_price": 199.98,
            "payment_method": "credit_card",
            "status": "completed",
            "timestamp": "2024-01-15T10:30:00"
        }

        result = self.validator.validate_transaction(tx)
        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_missing_required_field(self):
        """Test validation fails for missing required field."""
        tx = {
            "user_id": "user-001",
            "product_id": "prod-001",
            "product_category": "Electronics",
            "quantity": 2,
            "unit_price": 99.99,
            "total_price": 199.98,
            "payment_method": "credit_card",
            "status": "completed",
            "timestamp": "2024-01-15T10:30:00"
            # Missing transaction_id
        }

        result = self.validator.validate_transaction(tx)
        assert result.is_valid is False
        assert any("transaction_id" in e for e in result.errors)

    def test_invalid_quantity(self):
        """Test validation fails for invalid quantity."""
        tx = {
            "transaction_id": "tx-001",
            "user_id": "user-001",
            "product_id": "prod-001",
            "product_category": "Electronics",
            "quantity": 0,  # Invalid
            "unit_price": 99.99,
            "total_price": 0,
            "payment_method": "credit_card",
            "status": "completed",
            "timestamp": "2024-01-15T10:30:00"
        }

        result = self.validator.validate_transaction(tx)
        assert result.is_valid is False
        assert any("quantity" in e for e in result.errors)

    def test_invalid_category(self):
        """Test validation fails for invalid category."""
        tx = {
            "transaction_id": "tx-001",
            "user_id": "user-001",
            "product_id": "prod-001",
            "product_category": "InvalidCategory",  # Invalid
            "quantity": 1,
            "unit_price": 99.99,
            "total_price": 99.99,
            "payment_method": "credit_card",
            "status": "completed",
            "timestamp": "2024-01-15T10:30:00"
        }

        result = self.validator.validate_transaction(tx)
        assert result.is_valid is False
        assert any("product_category" in e for e in result.errors)

    def test_invalid_status(self):
        """Test validation fails for invalid status."""
        tx = {
            "transaction_id": "tx-001",
            "user_id": "user-001",
            "product_id": "prod-001",
            "product_category": "Electronics",
            "quantity": 1,
            "unit_price": 99.99,
            "total_price": 99.99,
            "payment_method": "credit_card",
            "status": "invalid_status",  # Invalid
            "timestamp": "2024-01-15T10:30:00"
        }

        result = self.validator.validate_transaction(tx)
        assert result.is_valid is False
        assert any("status" in e for e in result.errors)

    def test_price_mismatch_warning(self):
        """Test price mismatch generates warning."""
        tx = {
            "transaction_id": "tx-001",
            "user_id": "user-001",
            "product_id": "prod-001",
            "product_category": "Electronics",
            "quantity": 2,
            "unit_price": 100.00,
            "total_price": 150.00,  # Should be 200.00
            "payment_method": "credit_card",
            "status": "completed",
            "timestamp": "2024-01-15T10:30:00"
        }

        result = self.validator.validate_transaction(tx)
        assert result.is_valid is True  # Still valid
        assert len(result.warnings) > 0
        assert any("mismatch" in w.lower() for w in result.warnings)

    def test_batch_validation(self):
        """Test batch validation."""
        transactions = [
            {
                "transaction_id": f"tx-{i:03d}",
                "user_id": "user-001",
                "product_id": "prod-001",
                "product_category": "Electronics",
                "quantity": 1,
                "unit_price": 99.99,
                "total_price": 99.99,
                "payment_method": "credit_card",
                "status": "completed",
                "timestamp": "2024-01-15T10:30:00"
            }
            for i in range(100)
        ]

        result = self.validator.validate_batch(transactions)
        assert result.metrics['total_records'] == 100
        assert result.metrics['valid_records'] == 100
        assert result.metrics['validity_rate'] == 1.0

    def test_batch_with_invalid_records(self):
        """Test batch validation with some invalid records."""
        valid_tx = {
            "transaction_id": "tx-001",
            "user_id": "user-001",
            "product_id": "prod-001",
            "product_category": "Electronics",
            "quantity": 1,
            "unit_price": 99.99,
            "total_price": 99.99,
            "payment_method": "credit_card",
            "status": "completed",
            "timestamp": "2024-01-15T10:30:00"
        }

        invalid_tx = {
            "transaction_id": "tx-002",
            "user_id": "user-002",
            # Missing other required fields
        }

        transactions = [valid_tx] * 99 + [invalid_tx]
        result = self.validator.validate_batch(transactions)

        assert result.metrics['valid_records'] == 99
        assert result.metrics['invalid_records'] == 1


class TestValidationResult:
    """Test ValidationResult class."""

    def test_initial_state(self):
        """Test initial state of ValidationResult."""
        result = ValidationResult('test_check')
        assert result.is_valid is True
        assert result.errors == []
        assert result.warnings == []

    def test_add_error(self):
        """Test adding error invalidates result."""
        result = ValidationResult('test_check')
        result.add_error("Test error")

        assert result.is_valid is False
        assert "Test error" in result.errors

    def test_add_warning(self):
        """Test adding warning doesn't invalidate result."""
        result = ValidationResult('test_check')
        result.add_warning("Test warning")

        assert result.is_valid is True
        assert "Test warning" in result.warnings

    def test_to_dict(self):
        """Test conversion to dictionary."""
        result = ValidationResult('test_check')
        result.add_metric('count', 100)

        d = result.to_dict()
        assert d['check_name'] == 'test_check'
        assert d['metrics']['count'] == 100


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
