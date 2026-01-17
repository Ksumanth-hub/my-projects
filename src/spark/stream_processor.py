"""
Spark Structured Streaming processor for real-time e-commerce transactions.
Processes Kafka stream with windowed aggregations and fault tolerance.
"""
import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import sys
sys.path.insert(0, '/opt/spark-apps/src')

from config.settings import settings
from spark.transformations import TRANSACTION_SCHEMA

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StreamProcessor:
    """Spark Structured Streaming processor for real-time data."""

    def __init__(self, spark: SparkSession = None):
        """
        Initialize the stream processor.

        Args:
            spark: Optional SparkSession
        """
        self.spark = spark or self._create_spark_session()
        self.active_queries = []

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session optimized for streaming."""
        logger.info("Creating Spark session for streaming...")

        spark = (
            SparkSession.builder
            .appName(f"{settings.SPARK_APP_NAME}-Streaming")
            .master(settings.SPARK_MASTER_URL)
            .config("spark.sql.shuffle.partitions", "10")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.sql.streaming.checkpointLocation", str(settings.DATA_CHECKPOINT_PATH))
            .config("spark.driver.memory", settings.SPARK_DRIVER_MEMORY)
            .config("spark.executor.memory", settings.SPARK_EXECUTOR_MEMORY)
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
            )
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")
        logger.info(f"Streaming session created: {spark.sparkContext.applicationId}")

        return spark

    def read_stream_from_kafka(self, topic: str = None) -> DataFrame:
        """
        Create streaming DataFrame from Kafka topic.

        Args:
            topic: Kafka topic to subscribe to

        Returns:
            Streaming DataFrame
        """
        topic = topic or settings.KAFKA_TOPIC_TRANSACTIONS

        logger.info(f"Creating stream from Kafka topic: {topic}")

        raw_stream = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", 10000)  # Rate limiting
            .load()
        )

        # Parse JSON and add event time watermark
        parsed_stream = (
            raw_stream
            .select(
                F.from_json(
                    F.col("value").cast("string"),
                    TRANSACTION_SCHEMA
                ).alias("data"),
                F.col("timestamp").alias("kafka_timestamp")
            )
            .select("data.*", "kafka_timestamp")
            .withColumn(
                "event_time",
                F.to_timestamp(F.col("timestamp"))
            )
            .withWatermark("event_time", "1 minute")  # Late data tolerance
        )

        return parsed_stream

    def process_stream(self, stream_df: DataFrame) -> DataFrame:
        """
        Apply transformations to streaming data.

        Args:
            stream_df: Input streaming DataFrame

        Returns:
            Transformed streaming DataFrame
        """
        return (
            stream_df
            # Add time dimensions
            .withColumn("transaction_date", F.to_date(F.col("event_time")))
            .withColumn("transaction_hour", F.hour(F.col("event_time")))
            # Flatten address
            .withColumn("shipping_city", F.col("shipping_address.city"))
            .withColumn("shipping_state", F.col("shipping_address.state"))
            .drop("shipping_address")
            # Clean data
            .filter(F.col("transaction_id").isNotNull())
            .filter(F.col("total_price") > 0)
        )

    def create_windowed_aggregation(
        self,
        stream_df: DataFrame,
        window_duration: str = "5 minutes",
        slide_duration: str = "1 minute"
    ) -> DataFrame:
        """
        Create windowed aggregations for real-time metrics.

        Args:
            stream_df: Input streaming DataFrame
            window_duration: Window size
            slide_duration: Slide interval

        Returns:
            Aggregated streaming DataFrame
        """
        return (
            stream_df
            .groupBy(
                F.window(F.col("event_time"), window_duration, slide_duration),
                F.col("product_category")
            )
            .agg(
                F.count("transaction_id").alias("transaction_count"),
                F.sum("total_price").alias("total_revenue"),
                F.avg("total_price").alias("avg_transaction_value"),
                F.countDistinct("user_id").alias("unique_users"),
                F.sum(F.when(F.col("status") == "completed", 1).otherwise(0))
                    .alias("completed_count"),
                F.sum(F.when(F.col("status") == "failed", 1).otherwise(0))
                    .alias("failed_count")
            )
            .select(
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                F.col("product_category"),
                F.col("transaction_count"),
                F.col("total_revenue"),
                F.col("avg_transaction_value"),
                F.col("unique_users"),
                F.col("completed_count"),
                F.col("failed_count")
            )
        )

    def create_global_metrics(self, stream_df: DataFrame) -> DataFrame:
        """
        Create global real-time metrics stream.

        Args:
            stream_df: Input streaming DataFrame

        Returns:
            Global metrics streaming DataFrame
        """
        return (
            stream_df
            .groupBy(
                F.window(F.col("event_time"), "1 minute")
            )
            .agg(
                F.count("transaction_id").alias("transactions_per_minute"),
                F.sum("total_price").alias("revenue_per_minute"),
                F.countDistinct("user_id").alias("active_users"),
                F.countDistinct("product_category").alias("active_categories")
            )
            .select(
                F.col("window.start").alias("minute_start"),
                F.col("window.end").alias("minute_end"),
                F.col("transactions_per_minute"),
                F.col("revenue_per_minute"),
                F.col("active_users"),
                F.col("active_categories")
            )
        )

    def write_to_parquet(
        self,
        stream_df: DataFrame,
        output_path: str,
        checkpoint_path: str,
        query_name: str
    ):
        """
        Write streaming data to Parquet with checkpointing.

        Args:
            stream_df: Streaming DataFrame to write
            output_path: Output directory
            checkpoint_path: Checkpoint directory
            query_name: Name for the streaming query
        """
        query = (
            stream_df.writeStream
            .format("parquet")
            .outputMode("append")
            .option("path", output_path)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime="30 seconds")
            .queryName(query_name)
            .start()
        )

        self.active_queries.append(query)
        logger.info(f"Started streaming query: {query_name}")

        return query

    def write_to_console(
        self,
        stream_df: DataFrame,
        query_name: str,
        output_mode: str = "update"
    ):
        """
        Write streaming data to console for debugging.

        Args:
            stream_df: Streaming DataFrame
            query_name: Query name
            output_mode: Output mode (append, update, complete)
        """
        query = (
            stream_df.writeStream
            .format("console")
            .outputMode(output_mode)
            .option("truncate", "false")
            .trigger(processingTime="10 seconds")
            .queryName(query_name)
            .start()
        )

        self.active_queries.append(query)
        logger.info(f"Started console output: {query_name}")

        return query

    def write_to_kafka(
        self,
        stream_df: DataFrame,
        topic: str,
        checkpoint_path: str,
        query_name: str
    ):
        """
        Write streaming results back to Kafka.

        Args:
            stream_df: Streaming DataFrame
            topic: Output Kafka topic
            checkpoint_path: Checkpoint directory
            query_name: Query name
        """
        # Convert to JSON for Kafka
        kafka_df = stream_df.select(
            F.to_json(F.struct("*")).alias("value")
        )

        query = (
            kafka_df.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS)
            .option("topic", topic)
            .option("checkpointLocation", checkpoint_path)
            .outputMode("update")
            .trigger(processingTime="30 seconds")
            .queryName(query_name)
            .start()
        )

        self.active_queries.append(query)
        logger.info(f"Started Kafka output to {topic}: {query_name}")

        return query

    def run(self) -> None:
        """Run the complete streaming pipeline."""
        logger.info("Starting streaming pipeline...")

        try:
            # Read from Kafka
            raw_stream = self.read_stream_from_kafka()

            # Process stream
            processed_stream = self.process_stream(raw_stream)

            # Create aggregations
            windowed_metrics = self.create_windowed_aggregation(processed_stream)
            global_metrics = self.create_global_metrics(processed_stream)

            # Write processed transactions to Parquet
            self.write_to_parquet(
                processed_stream,
                output_path=str(settings.DATA_PROCESSED_PATH / "streaming" / "transactions"),
                checkpoint_path=str(settings.DATA_CHECKPOINT_PATH / "transactions"),
                query_name="transactions_to_parquet"
            )

            # Write windowed metrics to Parquet
            self.write_to_parquet(
                windowed_metrics,
                output_path=str(settings.DATA_OUTPUT_PATH / "streaming" / "windowed_metrics"),
                checkpoint_path=str(settings.DATA_CHECKPOINT_PATH / "windowed_metrics"),
                query_name="windowed_metrics_to_parquet"
            )

            # Write global metrics to console for monitoring
            self.write_to_console(
                global_metrics,
                query_name="global_metrics_console",
                output_mode="update"
            )

            # Optionally write aggregations to Kafka
            self.write_to_kafka(
                windowed_metrics,
                topic=settings.KAFKA_TOPIC_PROCESSED,
                checkpoint_path=str(settings.DATA_CHECKPOINT_PATH / "kafka_output"),
                query_name="metrics_to_kafka"
            )

            logger.info(f"Started {len(self.active_queries)} streaming queries")

            # Wait for termination
            self.spark.streams.awaitAnyTermination()

        except Exception as e:
            logger.error(f"Streaming error: {e}")
            self.stop_all()
            raise

    def stop_all(self) -> None:
        """Stop all streaming queries."""
        logger.info("Stopping all streaming queries...")

        for query in self.active_queries:
            try:
                query.stop()
                logger.info(f"Stopped query: {query.name}")
            except Exception as e:
                logger.warning(f"Error stopping query {query.name}: {e}")

        self.active_queries.clear()

    def close(self) -> None:
        """Stop queries and Spark session."""
        self.stop_all()
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main entry point."""
    processor = StreamProcessor()

    try:
        processor.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        processor.close()


if __name__ == "__main__":
    main()
