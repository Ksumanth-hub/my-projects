"""
Configuration settings for the e-commerce data pipeline.
"""
import os
from pathlib import Path


class Settings:
    """Central configuration management."""

    # Kafka Settings
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    KAFKA_TOPIC_TRANSACTIONS: str = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "ecommerce-transactions")
    KAFKA_TOPIC_PROCESSED: str = os.getenv("KAFKA_TOPIC_PROCESSED", "processed-transactions")
    KAFKA_TOPIC_DLQ: str = os.getenv("KAFKA_TOPIC_DLQ", "ecommerce-transactions-dlq")
    KAFKA_CONSUMER_GROUP: str = os.getenv("KAFKA_CONSUMER_GROUP", "ecommerce-consumer-group")

    # Spark Settings
    SPARK_MASTER_URL: str = os.getenv("SPARK_MASTER_URL", "local[*]")
    SPARK_APP_NAME: str = os.getenv("SPARK_APP_NAME", "EcommerceDataPipeline")
    SPARK_DRIVER_MEMORY: str = os.getenv("SPARK_DRIVER_MEMORY", "1g")
    SPARK_EXECUTOR_MEMORY: str = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
    SPARK_EXECUTOR_CORES: int = int(os.getenv("SPARK_EXECUTOR_CORES", "2"))

    # Data Paths
    BASE_PATH: Path = Path(os.getenv("BASE_PATH", "/opt/spark-apps"))
    DATA_RAW_PATH: Path = Path(os.getenv("DATA_RAW_PATH", str(BASE_PATH / "data" / "raw")))
    DATA_PROCESSED_PATH: Path = Path(os.getenv("DATA_PROCESSED_PATH", str(BASE_PATH / "data" / "processed")))
    DATA_OUTPUT_PATH: Path = Path(os.getenv("DATA_OUTPUT_PATH", str(BASE_PATH / "data" / "output")))
    DATA_CHECKPOINT_PATH: Path = Path(os.getenv("DATA_CHECKPOINT_PATH", str(BASE_PATH / "data" / "checkpoints")))

    # Data Generation Settings
    EVENTS_PER_MINUTE: int = int(os.getenv("EVENTS_PER_MINUTE", "5000"))
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "100"))

    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # Product Categories for simulation
    PRODUCT_CATEGORIES: list = [
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

    # Payment Methods
    PAYMENT_METHODS: list = [
        "credit_card",
        "debit_card",
        "paypal",
        "apple_pay",
        "google_pay",
        "bank_transfer"
    ]

    # Transaction Statuses
    TRANSACTION_STATUSES: list = [
        "completed",
        "pending",
        "failed",
        "refunded"
    ]

    # Status weights (probability distribution)
    STATUS_WEIGHTS: list = [0.85, 0.08, 0.05, 0.02]  # completed, pending, failed, refunded

    @classmethod
    def ensure_directories(cls) -> None:
        """Create data directories if they don't exist."""
        for path in [cls.DATA_RAW_PATH, cls.DATA_PROCESSED_PATH,
                     cls.DATA_OUTPUT_PATH, cls.DATA_CHECKPOINT_PATH]:
            Path(path).mkdir(parents=True, exist_ok=True)


# Singleton instance
settings = Settings()
