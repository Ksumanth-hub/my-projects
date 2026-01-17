"""
E-commerce transaction data generator.
Generates realistic simulated e-commerce transaction data.
"""
import uuid
import random
from datetime import datetime
from typing import Dict, Any, List
from faker import Faker

from config.settings import settings


class EcommerceDataGenerator:
    """Generates realistic e-commerce transaction data."""

    def __init__(self, seed: int = None):
        """
        Initialize the data generator.

        Args:
            seed: Optional random seed for reproducibility
        """
        self.fake = Faker()
        if seed:
            Faker.seed(seed)
            random.seed(seed)

        # Pre-generate some user IDs and product IDs for realism
        self._user_ids = [f"USR_{uuid.uuid4().hex[:8]}" for _ in range(1000)]
        self._product_ids = self._generate_product_catalog()

    def _generate_product_catalog(self) -> Dict[str, List[Dict[str, Any]]]:
        """Generate a product catalog organized by category."""
        catalog = {}
        for category in settings.PRODUCT_CATEGORIES:
            products = []
            for _ in range(50):  # 50 products per category
                products.append({
                    "product_id": f"PRD_{uuid.uuid4().hex[:8]}",
                    "name": self.fake.catch_phrase(),
                    "base_price": round(random.uniform(5.0, 500.0), 2)
                })
            catalog[category] = products
        return catalog

    def generate_transaction(self) -> Dict[str, Any]:
        """
        Generate a single e-commerce transaction.

        Returns:
            Dictionary containing transaction data
        """
        # Select random category and product
        category = random.choice(settings.PRODUCT_CATEGORIES)
        product = random.choice(self._product_ids[category])

        # Calculate price with small random variation
        price_variation = random.uniform(0.9, 1.1)
        unit_price = round(product["base_price"] * price_variation, 2)
        quantity = random.choices(
            [1, 2, 3, 4, 5],
            weights=[0.5, 0.25, 0.15, 0.07, 0.03]
        )[0]

        # Select status based on weights
        status = random.choices(
            settings.TRANSACTION_STATUSES,
            weights=settings.STATUS_WEIGHTS
        )[0]

        transaction = {
            "transaction_id": str(uuid.uuid4()),
            "user_id": random.choice(self._user_ids),
            "product_id": product["product_id"],
            "product_name": product["name"],
            "product_category": category,
            "quantity": quantity,
            "unit_price": unit_price,
            "total_price": round(unit_price * quantity, 2),
            "currency": "USD",
            "payment_method": random.choice(settings.PAYMENT_METHODS),
            "status": status,
            "timestamp": datetime.utcnow().isoformat(),
            "shipping_address": {
                "city": self.fake.city(),
                "state": self.fake.state_abbr(),
                "country": "US",
                "zip_code": self.fake.zipcode()
            },
            "device_type": random.choice(["mobile", "desktop", "tablet"]),
            "browser": random.choice(["chrome", "firefox", "safari", "edge"]),
        }

        return transaction

    def generate_batch(self, batch_size: int = None) -> List[Dict[str, Any]]:
        """
        Generate a batch of transactions.

        Args:
            batch_size: Number of transactions to generate (default from settings)

        Returns:
            List of transaction dictionaries
        """
        size = batch_size or settings.BATCH_SIZE
        return [self.generate_transaction() for _ in range(size)]


# Module-level instance for convenience
generator = EcommerceDataGenerator()


def generate_transaction() -> Dict[str, Any]:
    """Convenience function to generate a single transaction."""
    return generator.generate_transaction()


def generate_batch(batch_size: int = None) -> List[Dict[str, Any]]:
    """Convenience function to generate a batch of transactions."""
    return generator.generate_batch(batch_size)
