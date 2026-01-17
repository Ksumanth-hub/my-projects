"""
Kafka consumer for e-commerce transaction data.
Consumes transaction events from Kafka for processing.
"""
import json
import logging
import signal
from typing import Callable, Optional, List, Dict, Any
from datetime import datetime

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from config.settings import settings
from kafka.schemas import validate_transaction

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TransactionConsumer:
    """Kafka consumer for e-commerce transactions."""

    def __init__(
        self,
        bootstrap_servers: str = None,
        group_id: str = None,
        topics: List[str] = None
    ):
        """
        Initialize the Kafka consumer.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            group_id: Consumer group ID
            topics: List of topics to subscribe to
        """
        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self.group_id = group_id or settings.KAFKA_CONSUMER_GROUP
        self.topics = topics or [settings.KAFKA_TOPIC_TRANSACTIONS]
        self.consumer: Optional[KafkaConsumer] = None
        self.running = False

        # Metrics
        self.messages_received = 0
        self.messages_processed = 0
        self.messages_failed = 0
        self.start_time: Optional[datetime] = None

    def connect(self) -> None:
        """Connect to Kafka broker."""
        logger.info(f"Connecting to Kafka at {self.bootstrap_servers}")

        self.consumer = KafkaConsumer(
            *self.topics,
            bootstrap_servers=self.bootstrap_servers.split(","),
            group_id=self.group_id,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            max_poll_records=500,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )

        logger.info(f"Connected to Kafka, subscribed to topics: {self.topics}")

    def consume(
        self,
        handler: Callable[[Dict[str, Any]], bool],
        max_messages: int = None
    ) -> None:
        """
        Consume messages and process with handler.

        Args:
            handler: Function to process each message, returns True on success
            max_messages: Maximum messages to consume (None for infinite)
        """
        if not self.consumer:
            raise RuntimeError("Consumer not connected. Call connect() first.")

        self.running = True
        self.start_time = datetime.utcnow()

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        logger.info("Starting consumption...")

        try:
            for message in self.consumer:
                if not self.running:
                    break

                self.messages_received += 1

                try:
                    # Validate message
                    validate_transaction(message.value)

                    # Process with handler
                    if handler(message.value):
                        self.messages_processed += 1
                    else:
                        self.messages_failed += 1

                except ValueError as e:
                    logger.warning(f"Invalid message: {e}")
                    self.messages_failed += 1
                except Exception as e:
                    logger.error(f"Handler error: {e}")
                    self.messages_failed += 1

                # Log progress periodically
                if self.messages_received % 1000 == 0:
                    self._log_metrics()

                # Check max messages limit
                if max_messages and self.messages_received >= max_messages:
                    logger.info(f"Reached max messages limit: {max_messages}")
                    break

        except KafkaError as e:
            logger.error(f"Kafka error: {e}")

        self._log_metrics()
        logger.info("Consumer stopped")

    def consume_batch(
        self,
        batch_size: int = 100,
        timeout_ms: int = 5000
    ) -> List[Dict[str, Any]]:
        """
        Consume a batch of messages.

        Args:
            batch_size: Maximum messages to fetch
            timeout_ms: Timeout for polling

        Returns:
            List of transaction dictionaries
        """
        if not self.consumer:
            raise RuntimeError("Consumer not connected. Call connect() first.")

        messages = []
        records = self.consumer.poll(timeout_ms=timeout_ms, max_records=batch_size)

        for topic_partition, partition_records in records.items():
            for record in partition_records:
                try:
                    validate_transaction(record.value)
                    messages.append(record.value)
                    self.messages_received += 1
                except ValueError as e:
                    logger.warning(f"Invalid message skipped: {e}")
                    self.messages_failed += 1

        return messages

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    def _log_metrics(self) -> None:
        """Log current consumer metrics."""
        if not self.start_time:
            return

        elapsed = (datetime.utcnow() - self.start_time).total_seconds()
        rate = self.messages_received / elapsed * 60 if elapsed > 0 else 0

        logger.info(
            f"Consumer metrics: "
            f"received={self.messages_received}, "
            f"processed={self.messages_processed}, "
            f"failed={self.messages_failed}, "
            f"rate={rate:.1f} msg/min"
        )

    def close(self) -> None:
        """Close the consumer connection."""
        if self.consumer:
            self.consumer.close()
            logger.info("Consumer closed")


def simple_handler(transaction: Dict[str, Any]) -> bool:
    """Simple handler that logs transactions."""
    logger.debug(
        f"Transaction: {transaction['transaction_id']} - "
        f"{transaction['product_category']} - "
        f"${transaction['total_price']}"
    )
    return True


def main():
    """Main entry point for the consumer."""
    consumer = TransactionConsumer()

    try:
        consumer.connect()
        consumer.consume(handler=simple_handler)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Consumer error: {e}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
