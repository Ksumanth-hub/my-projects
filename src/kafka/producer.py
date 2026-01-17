"""
Kafka producer for e-commerce transaction data.
Generates and sends simulated transaction events to Kafka.
"""
import json
import time
import logging
import signal
import sys
from typing import Optional
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config.settings import settings
from utils.data_generator import EcommerceDataGenerator
from kafka.schemas import validate_transaction

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TransactionProducer:
    """Kafka producer for e-commerce transactions."""

    def __init__(self, bootstrap_servers: str = None):
        """
        Initialize the Kafka producer.

        Args:
            bootstrap_servers: Kafka bootstrap servers (comma-separated)
        """
        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self.topic = settings.KAFKA_TOPIC_TRANSACTIONS
        self.producer: Optional[KafkaProducer] = None
        self.data_generator = EcommerceDataGenerator()
        self.running = False

        # Metrics
        self.messages_sent = 0
        self.messages_failed = 0
        self.start_time: Optional[datetime] = None

    def connect(self) -> None:
        """Connect to Kafka broker."""
        logger.info(f"Connecting to Kafka at {self.bootstrap_servers}")

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            retry_backoff_ms=500,
            max_in_flight_requests_per_connection=5,
            batch_size=16384,  # 16KB batch size
            linger_ms=10,  # Wait up to 10ms for batching
            compression_type='gzip'
        )

        logger.info("Connected to Kafka successfully")

    def send_transaction(self, transaction: dict) -> bool:
        """
        Send a single transaction to Kafka.

        Args:
            transaction: Transaction data dictionary

        Returns:
            True if sent successfully, False otherwise
        """
        if not self.producer:
            raise RuntimeError("Producer not connected. Call connect() first.")

        try:
            # Validate transaction
            validate_transaction(transaction)

            # Use product_category as partition key for ordering
            partition_key = transaction.get("product_category", "unknown")

            # Send message
            future = self.producer.send(
                self.topic,
                key=partition_key,
                value=transaction
            )

            # Wait for confirmation (with timeout)
            record_metadata = future.get(timeout=10)

            logger.debug(
                f"Sent transaction {transaction['transaction_id']} "
                f"to partition {record_metadata.partition} "
                f"at offset {record_metadata.offset}"
            )

            self.messages_sent += 1
            return True

        except KafkaError as e:
            logger.error(f"Failed to send transaction: {e}")
            self.messages_failed += 1
            return False
        except ValueError as e:
            logger.error(f"Invalid transaction data: {e}")
            self.messages_failed += 1
            return False

    def send_batch(self, batch_size: int = None) -> int:
        """
        Generate and send a batch of transactions.

        Args:
            batch_size: Number of transactions to send

        Returns:
            Number of successfully sent messages
        """
        size = batch_size or settings.BATCH_SIZE
        transactions = self.data_generator.generate_batch(size)

        sent_count = 0
        for transaction in transactions:
            if self.send_transaction(transaction):
                sent_count += 1

        return sent_count

    def run_continuous(self, events_per_minute: int = None) -> None:
        """
        Run continuous data generation at specified rate.

        Args:
            events_per_minute: Target events per minute (default from settings)
        """
        rate = events_per_minute or settings.EVENTS_PER_MINUTE
        interval = 60.0 / rate  # Seconds between events

        self.running = True
        self.start_time = datetime.utcnow()

        logger.info(f"Starting continuous production at {rate} events/minute")

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        batch_size = min(100, rate // 10)  # Send in batches for efficiency
        batch_interval = interval * batch_size

        while self.running:
            batch_start = time.time()

            # Generate and send batch
            sent = self.send_batch(batch_size)

            # Log progress every 1000 messages
            if self.messages_sent % 1000 < batch_size:
                self._log_metrics()

            # Calculate sleep time to maintain rate
            elapsed = time.time() - batch_start
            sleep_time = max(0, batch_interval - elapsed)

            if sleep_time > 0:
                time.sleep(sleep_time)

        self._log_metrics()
        logger.info("Producer stopped")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    def _log_metrics(self) -> None:
        """Log current producer metrics."""
        if not self.start_time:
            return

        elapsed = (datetime.utcnow() - self.start_time).total_seconds()
        rate = self.messages_sent / elapsed * 60 if elapsed > 0 else 0

        logger.info(
            f"Producer metrics: "
            f"sent={self.messages_sent}, "
            f"failed={self.messages_failed}, "
            f"rate={rate:.1f} msg/min, "
            f"elapsed={elapsed:.1f}s"
        )

    def close(self) -> None:
        """Close the producer connection."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Producer closed")


def main():
    """Main entry point for the producer."""
    producer = TransactionProducer()

    try:
        producer.connect()
        producer.run_continuous()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Producer error: {e}")
        sys.exit(1)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
