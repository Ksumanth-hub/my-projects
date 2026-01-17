"""
Spark batch processor for e-commerce transaction data.
Implements optimized ETL with partitioning, caching, and broadcast variables.
"""
import logging
import time
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

import sys
sys.path.insert(0, '/opt/spark-apps/src')

from config.settings import settings
from spark.transformations import (
    TRANSACTION_SCHEMA,
    prepare_for_analytics,
    aggregate_by_category,
    aggregate_by_time,
    filter_completed_transactions
)

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BatchProcessor:
    """Spark batch processor with optimizations."""

    def __init__(self, spark: SparkSession = None):
        """
        Initialize the batch processor.

        Args:
            spark: Optional SparkSession (creates one if not provided)
        """
        self.spark = spark or self._create_spark_session()
        self.metrics = {
            "start_time": None,
            "end_time": None,
            "records_processed": 0,
            "records_output": 0
        }

        # Create lookup table for broadcast
        self._category_lookup = self._create_category_lookup()

    def _create_spark_session(self) -> SparkSession:
        """Create optimized Spark session."""
        logger.info("Creating Spark session...")

        spark = (
            SparkSession.builder
            .appName(settings.SPARK_APP_NAME)
            .master(settings.SPARK_MASTER_URL)
            .config("spark.sql.shuffle.partitions", "10")
            .config("spark.default.parallelism", "10")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.driver.memory", settings.SPARK_DRIVER_MEMORY)
            .config("spark.executor.memory", settings.SPARK_EXECUTOR_MEMORY)
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")
        logger.info(f"Spark session created: {spark.sparkContext.applicationId}")

        return spark

    def _create_category_lookup(self) -> DataFrame:
        """Create category lookup table for broadcast join."""
        # Category metadata for enrichment
        category_data = [
            ("Electronics", "Tech", 0.15),
            ("Clothing", "Fashion", 0.20),
            ("Home & Garden", "Home", 0.10),
            ("Sports & Outdoors", "Active", 0.12),
            ("Books", "Media", 0.05),
            ("Toys & Games", "Entertainment", 0.08),
            ("Health & Beauty", "Personal", 0.18),
            ("Food & Grocery", "Consumables", 0.03),
            ("Automotive", "Transport", 0.10),
            ("Office Supplies", "Business", 0.07)
        ]

        schema = StructType([
            StructField("category_name", StringType(), False),
            StructField("category_group", StringType(), False),
            StructField("avg_margin", DoubleType(), False)
        ])

        return self.spark.createDataFrame(category_data, schema)

    def read_from_kafka(self, topic: str = None) -> DataFrame:
        """
        Read data from Kafka topic (batch mode).

        Args:
            topic: Kafka topic name

        Returns:
            DataFrame with transaction data
        """
        topic = topic or settings.KAFKA_TOPIC_TRANSACTIONS

        logger.info(f"Reading from Kafka topic: {topic}")

        df = (
            self.spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )

        # Parse JSON value
        from pyspark.sql.functions import from_json
        parsed_df = df.select(
            from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias("data")
        ).select("data.*")

        return parsed_df

    def read_from_json(self, path: str) -> DataFrame:
        """
        Read data from JSON files.

        Args:
            path: Path to JSON files

        Returns:
            DataFrame with transaction data
        """
        logger.info(f"Reading from JSON path: {path}")

        return (
            self.spark.read
            .schema(TRANSACTION_SCHEMA)
            .json(path)
        )

    def read_from_parquet(self, path: str) -> DataFrame:
        """
        Read data from Parquet files.

        Args:
            path: Path to Parquet files

        Returns:
            DataFrame
        """
        logger.info(f"Reading from Parquet path: {path}")
        return self.spark.read.parquet(path)

    def enrich_with_category_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich transactions with category metadata using broadcast join.

        Args:
            df: Input DataFrame

        Returns:
            Enriched DataFrame
        """
        # Broadcast the small lookup table
        broadcast_lookup = F.broadcast(self._category_lookup)

        return df.join(
            broadcast_lookup,
            df.product_category == broadcast_lookup.category_name,
            "left"
        ).drop("category_name")

    def process(self, df: DataFrame) -> DataFrame:
        """
        Main processing pipeline with optimizations.

        Args:
            df: Input DataFrame

        Returns:
            Processed DataFrame
        """
        self.metrics["start_time"] = datetime.utcnow()
        self.metrics["records_processed"] = df.count()

        logger.info(f"Processing {self.metrics['records_processed']} records...")

        # Apply transformations
        processed_df = prepare_for_analytics(df)

        # Cache for multiple operations
        processed_df.cache()

        # Enrich with category data (broadcast join)
        enriched_df = self.enrich_with_category_data(processed_df)

        # Repartition by date for optimal writes
        final_df = enriched_df.repartition(
            "transaction_year", "transaction_month", "transaction_day"
        )

        self.metrics["records_output"] = final_df.count()
        self.metrics["end_time"] = datetime.utcnow()

        self._log_metrics()

        return final_df

    def write_parquet(
        self,
        df: DataFrame,
        path: str = None,
        partition_cols: list = None
    ) -> None:
        """
        Write DataFrame to Parquet with partitioning.

        Args:
            df: DataFrame to write
            path: Output path
            partition_cols: Columns to partition by
        """
        output_path = path or str(settings.DATA_PROCESSED_PATH / "transactions")
        partition_cols = partition_cols or ["transaction_year", "transaction_month", "transaction_day"]

        logger.info(f"Writing to Parquet: {output_path}")
        logger.info(f"Partition columns: {partition_cols}")

        (
            df.write
            .mode("overwrite")
            .partitionBy(*partition_cols)
            .parquet(output_path)
        )

        logger.info("Write completed successfully")

    def generate_aggregations(self, df: DataFrame, output_path: str = None) -> None:
        """
        Generate and save aggregated analytics.

        Args:
            df: Processed DataFrame
            output_path: Base path for aggregations
        """
        output_base = Path(output_path or settings.DATA_OUTPUT_PATH)

        # Category aggregations
        logger.info("Generating category aggregations...")
        category_agg = aggregate_by_category(df)
        category_agg.write.mode("overwrite").parquet(
            str(output_base / "aggregations" / "by_category")
        )

        # Daily aggregations
        logger.info("Generating daily aggregations...")
        daily_agg = aggregate_by_time(df, "transaction_date")
        daily_agg.write.mode("overwrite").parquet(
            str(output_base / "aggregations" / "by_date")
        )

        # Hourly aggregations
        logger.info("Generating hourly aggregations...")
        hourly_agg = df.groupBy("transaction_date", "transaction_hour").agg(
            F.count("transaction_id").alias("transactions"),
            F.sum("total_price").alias("revenue")
        ).orderBy("transaction_date", "transaction_hour")
        hourly_agg.write.mode("overwrite").parquet(
            str(output_base / "aggregations" / "by_hour")
        )

        # Status breakdown
        logger.info("Generating status breakdown...")
        status_agg = df.groupBy("status").agg(
            F.count("transaction_id").alias("count"),
            F.sum("total_price").alias("total_value")
        )
        status_agg.write.mode("overwrite").parquet(
            str(output_base / "aggregations" / "by_status")
        )

        logger.info("All aggregations generated successfully")

    def _log_metrics(self) -> None:
        """Log processing metrics."""
        if self.metrics["start_time"] and self.metrics["end_time"]:
            duration = (
                self.metrics["end_time"] - self.metrics["start_time"]
            ).total_seconds()

            logger.info(
                f"Batch processing metrics: "
                f"input_records={self.metrics['records_processed']}, "
                f"output_records={self.metrics['records_output']}, "
                f"duration={duration:.2f}s, "
                f"throughput={self.metrics['records_processed']/duration:.0f} records/s"
            )

    def run(self, input_path: str = None, output_path: str = None) -> None:
        """
        Run the full batch processing pipeline.

        Args:
            input_path: Path to input data (JSON or Parquet)
            output_path: Path for output data
        """
        try:
            # Read data
            if input_path:
                if input_path.endswith(".json") or "json" in input_path:
                    df = self.read_from_json(input_path)
                else:
                    df = self.read_from_parquet(input_path)
            else:
                # Default: read from Kafka
                df = self.read_from_kafka()

            # Process
            processed_df = self.process(df)

            # Write results
            self.write_parquet(processed_df, output_path)

            # Generate aggregations
            self.generate_aggregations(processed_df)

            logger.info("Batch processing completed successfully!")

        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            raise
        finally:
            # Unpersist cached data
            self.spark.catalog.clearCache()

    def close(self) -> None:
        """Stop the Spark session."""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main entry point."""
    processor = BatchProcessor()

    try:
        # For demo: generate some sample data and process
        from utils.data_generator import generate_batch

        # Generate sample data
        logger.info("Generating sample data...")
        sample_data = generate_batch(10000)

        # Create DataFrame from sample data
        df = processor.spark.createDataFrame(sample_data, TRANSACTION_SCHEMA)

        # Save as JSON for processing
        raw_path = str(settings.DATA_RAW_PATH / "sample_transactions")
        df.write.mode("overwrite").json(raw_path)

        # Run processing pipeline
        processor.run(input_path=raw_path)

    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        processor.close()


if __name__ == "__main__":
    main()
