"""
JSON schemas for Kafka message validation.
"""
from typing import Dict, Any


# Transaction schema for validation
TRANSACTION_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "required": [
        "transaction_id",
        "user_id",
        "product_id",
        "product_category",
        "quantity",
        "unit_price",
        "total_price",
        "payment_method",
        "status",
        "timestamp"
    ],
    "properties": {
        "transaction_id": {"type": "string"},
        "user_id": {"type": "string"},
        "product_id": {"type": "string"},
        "product_name": {"type": "string"},
        "product_category": {
            "type": "string",
            "enum": [
                "Electronics",
                "Clothing",
                "Home & Garden",
                "Sports & Outdoors",
                "Books",
                "Toys & Games",
                "Health & Beauty",
                "Food & Grocery",
                "Automotive",
                "Office Supplies"
            ]
        },
        "quantity": {"type": "integer", "minimum": 1},
        "unit_price": {"type": "number", "minimum": 0},
        "total_price": {"type": "number", "minimum": 0},
        "currency": {"type": "string", "default": "USD"},
        "payment_method": {
            "type": "string",
            "enum": [
                "credit_card",
                "debit_card",
                "paypal",
                "apple_pay",
                "google_pay",
                "bank_transfer"
            ]
        },
        "status": {
            "type": "string",
            "enum": ["completed", "pending", "failed", "refunded"]
        },
        "timestamp": {"type": "string", "format": "date-time"},
        "shipping_address": {
            "type": "object",
            "properties": {
                "city": {"type": "string"},
                "state": {"type": "string"},
                "country": {"type": "string"},
                "zip_code": {"type": "string"}
            }
        },
        "device_type": {
            "type": "string",
            "enum": ["mobile", "desktop", "tablet"]
        },
        "browser": {
            "type": "string",
            "enum": ["chrome", "firefox", "safari", "edge"]
        }
    }
}


def validate_transaction(transaction: Dict[str, Any]) -> bool:
    """
    Basic validation for transaction data.

    Args:
        transaction: Transaction dictionary to validate

    Returns:
        True if valid, raises ValueError if invalid
    """
    required_fields = TRANSACTION_SCHEMA["required"]

    for field in required_fields:
        if field not in transaction:
            raise ValueError(f"Missing required field: {field}")

    # Type validations
    if not isinstance(transaction.get("quantity"), int):
        raise ValueError("quantity must be an integer")

    if transaction.get("quantity", 0) < 1:
        raise ValueError("quantity must be at least 1")

    if not isinstance(transaction.get("unit_price"), (int, float)):
        raise ValueError("unit_price must be a number")

    if not isinstance(transaction.get("total_price"), (int, float)):
        raise ValueError("total_price must be a number")

    # Enum validations
    valid_categories = TRANSACTION_SCHEMA["properties"]["product_category"]["enum"]
    if transaction.get("product_category") not in valid_categories:
        raise ValueError(f"Invalid product_category: {transaction.get('product_category')}")

    valid_methods = TRANSACTION_SCHEMA["properties"]["payment_method"]["enum"]
    if transaction.get("payment_method") not in valid_methods:
        raise ValueError(f"Invalid payment_method: {transaction.get('payment_method')}")

    valid_statuses = TRANSACTION_SCHEMA["properties"]["status"]["enum"]
    if transaction.get("status") not in valid_statuses:
        raise ValueError(f"Invalid status: {transaction.get('status')}")

    return True
