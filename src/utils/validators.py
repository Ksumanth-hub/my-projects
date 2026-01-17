"""
Data quality validators for e-commerce pipeline.
Provides validation checks for raw and processed data.
"""
import logging
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

from kafka.schemas import TRANSACTION_SCHEMA, validate_transaction

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ValidationResult:
    """Container for validation results."""

    def __init__(self, check_name: str):
        self.check_name = check_name
        self.is_valid = True
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.metrics: Dict[str, Any] = {}
        self.timestamp = datetime.utcnow().isoformat()

    def add_error(self, message: str) -> None:
        """Add an error and mark as invalid."""
        self.errors.append(message)
        self.is_valid = False

    def add_warning(self, message: str) -> None:
        """Add a warning (doesn't affect validity)."""
        self.warnings.append(message)

    def add_metric(self, name: str, value: Any) -> None:
        """Add a metric measurement."""
        self.metrics[name] = value

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'check_name': self.check_name,
            'is_valid': self.is_valid,
            'errors': self.errors,
            'warnings': self.warnings,
            'metrics': self.metrics,
            'timestamp': self.timestamp
        }


class DataValidator:
    """Validates data quality for the e-commerce pipeline."""

    def __init__(self):
        self.required_fields = TRANSACTION_SCHEMA['required']
        self.valid_categories = TRANSACTION_SCHEMA['properties']['product_category']['enum']
        self.valid_statuses = TRANSACTION_SCHEMA['properties']['status']['enum']
        self.valid_payment_methods = TRANSACTION_SCHEMA['properties']['payment_method']['enum']

    def validate_transaction(self, transaction: Dict[str, Any]) -> ValidationResult:
        """
        Validate a single transaction record.

        Args:
            transaction: Transaction dictionary

        Returns:
            ValidationResult with check details
        """
        result = ValidationResult('transaction_validation')

        # Required fields check
        for field in self.required_fields:
            if field not in transaction or transaction[field] is None:
                result.add_error(f"Missing required field: {field}")

        if not result.is_valid:
            return result

        # Data type validations
        if not isinstance(transaction.get('quantity'), int):
            result.add_error("quantity must be an integer")
        elif transaction['quantity'] < 1:
            result.add_error("quantity must be at least 1")

        if not isinstance(transaction.get('unit_price'), (int, float)):
            result.add_error("unit_price must be numeric")
        elif transaction['unit_price'] < 0:
            result.add_error("unit_price cannot be negative")

        if not isinstance(transaction.get('total_price'), (int, float)):
            result.add_error("total_price must be numeric")
        elif transaction['total_price'] < 0:
            result.add_error("total_price cannot be negative")

        # Enum validations
        if transaction.get('product_category') not in self.valid_categories:
            result.add_error(f"Invalid product_category: {transaction.get('product_category')}")

        if transaction.get('status') not in self.valid_statuses:
            result.add_error(f"Invalid status: {transaction.get('status')}")

        if transaction.get('payment_method') not in self.valid_payment_methods:
            result.add_error(f"Invalid payment_method: {transaction.get('payment_method')}")

        # Business logic validations
        expected_total = transaction.get('unit_price', 0) * transaction.get('quantity', 0)
        actual_total = transaction.get('total_price', 0)
        if abs(expected_total - actual_total) > 0.01:
            result.add_warning(f"Price mismatch: expected {expected_total}, got {actual_total}")

        return result

    def validate_batch(self, transactions: List[Dict[str, Any]]) -> ValidationResult:
        """
        Validate a batch of transactions.

        Args:
            transactions: List of transaction dictionaries

        Returns:
            Aggregated ValidationResult
        """
        result = ValidationResult('batch_validation')
        result.add_metric('total_records', len(transactions))

        valid_count = 0
        invalid_count = 0
        error_summary: Dict[str, int] = {}

        for i, tx in enumerate(transactions):
            tx_result = self.validate_transaction(tx)

            if tx_result.is_valid:
                valid_count += 1
            else:
                invalid_count += 1
                for error in tx_result.errors:
                    error_summary[error] = error_summary.get(error, 0) + 1

        result.add_metric('valid_records', valid_count)
        result.add_metric('invalid_records', invalid_count)
        result.add_metric('validity_rate', valid_count / len(transactions) if transactions else 0)
        result.add_metric('error_summary', error_summary)

        # Threshold checks
        validity_rate = valid_count / len(transactions) if transactions else 0
        if validity_rate < 0.99:
            result.add_error(f"Validity rate {validity_rate:.2%} below 99% threshold")

        if validity_rate < 0.999:
            result.add_warning(f"Validity rate {validity_rate:.2%} below 99.9% threshold")

        return result

    def validate_file(self, file_path: str) -> ValidationResult:
        """
        Validate a JSON file containing transactions.

        Args:
            file_path: Path to JSON file (one JSON object per line)

        Returns:
            ValidationResult
        """
        result = ValidationResult('file_validation')
        path = Path(file_path)

        if not path.exists():
            result.add_error(f"File not found: {file_path}")
            return result

        transactions = []
        parse_errors = 0

        with open(path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    tx = json.loads(line)
                    transactions.append(tx)
                except json.JSONDecodeError as e:
                    parse_errors += 1
                    if parse_errors <= 5:
                        result.add_error(f"JSON parse error at line {line_num}: {e}")

        result.add_metric('parse_errors', parse_errors)
        result.add_metric('parsed_records', len(transactions))

        if parse_errors > 0:
            result.add_warning(f"Found {parse_errors} JSON parse errors")

        # Validate parsed transactions
        batch_result = self.validate_batch(transactions)
        result.metrics.update(batch_result.metrics)
        result.errors.extend(batch_result.errors)
        result.warnings.extend(batch_result.warnings)
        result.is_valid = result.is_valid and batch_result.is_valid

        return result

    def validate_directory(self, dir_path: str) -> ValidationResult:
        """
        Validate all JSON files in a directory.

        Args:
            dir_path: Path to directory

        Returns:
            Aggregated ValidationResult
        """
        result = ValidationResult('directory_validation')
        path = Path(dir_path)

        if not path.exists():
            result.add_error(f"Directory not found: {dir_path}")
            return result

        if not path.is_dir():
            result.add_error(f"Path is not a directory: {dir_path}")
            return result

        json_files = list(path.rglob('*.json'))
        result.add_metric('file_count', len(json_files))

        if not json_files:
            result.add_warning("No JSON files found in directory")
            return result

        total_records = 0
        valid_records = 0
        file_results = []

        for json_file in json_files:
            file_result = self.validate_file(str(json_file))
            file_results.append({
                'file': str(json_file),
                'result': file_result.to_dict()
            })

            total_records += file_result.metrics.get('total_records', 0)
            valid_records += file_result.metrics.get('valid_records', 0)

            if not file_result.is_valid:
                result.add_error(f"Validation failed for {json_file.name}")

        result.add_metric('total_records', total_records)
        result.add_metric('valid_records', valid_records)
        result.add_metric('file_results', file_results)

        return result

    def validate_processed_data(self, parquet_path: str) -> Dict[str, Any]:
        """
        Validate processed Parquet data.

        Args:
            parquet_path: Path to Parquet files

        Returns:
            Validation result dictionary
        """
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'metrics': {}
        }

        spark = SparkSession.builder \
            .appName("DataValidation") \
            .master("local[*]") \
            .getOrCreate()

        try:
            df = spark.read.parquet(parquet_path)
            total = df.count()

            result['record_count'] = total

            # Null checks
            critical_cols = ['transaction_id', 'user_id', 'total_price']
            for col in critical_cols:
                if col in df.columns:
                    null_count = df.filter(F.col(col).isNull()).count()
                    null_rate = null_count / total if total > 0 else 0
                    result['metrics'][f'{col}_null_rate'] = null_rate

                    if null_rate > 0.01:
                        result['warnings'].append(f"High null rate for {col}: {null_rate:.2%}")

            # Duplicate check
            distinct_txns = df.select('transaction_id').distinct().count()
            dup_rate = (total - distinct_txns) / total if total > 0 else 0
            result['metrics']['duplicate_rate'] = dup_rate

            if dup_rate > 0.01:
                result['warnings'].append(f"High duplicate rate: {dup_rate:.2%}")

            # Price validation
            invalid_prices = df.filter(
                (F.col('total_price') < 0) |
                (F.col('total_price').isNull())
            ).count()
            result['metrics']['invalid_price_count'] = invalid_prices

            if invalid_prices > 0:
                result['errors'].append(f"Found {invalid_prices} invalid prices")
                result['is_valid'] = False

        except Exception as e:
            result['is_valid'] = False
            result['errors'].append(str(e))

        finally:
            spark.stop()

        return result


# Convenience functions
def validate_transaction(transaction: Dict[str, Any]) -> Dict[str, Any]:
    """Quick validation of a single transaction."""
    validator = DataValidator()
    result = validator.validate_transaction(transaction)
    return result.to_dict()


def validate_batch(transactions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Quick validation of a batch of transactions."""
    validator = DataValidator()
    result = validator.validate_batch(transactions)
    return result.to_dict()
