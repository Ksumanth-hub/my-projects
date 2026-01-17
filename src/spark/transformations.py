"""
Reusable Spark transformations for e-commerce data processing.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType
)


# Schema for transaction data
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("product_category", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("total_price", DoubleType(), False),
    StructField("currency", StringType(), True),
    StructField("payment_method", StringType(), False),
    StructField("status", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("shipping_address", StructType([
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("zip_code", StringType(), True)
    ]), True),
    StructField("device_type", StringType(), True),
    StructField("browser", StringType(), True)
])


def parse_timestamp(df: DataFrame, col_name: str = "timestamp") -> DataFrame:
    """
    Parse ISO timestamp string to timestamp type.

    Args:
        df: Input DataFrame
        col_name: Name of the timestamp column

    Returns:
        DataFrame with parsed timestamp
    """
    return df.withColumn(
        col_name,
        F.to_timestamp(F.col(col_name))
    )


def add_time_dimensions(df: DataFrame, timestamp_col: str = "timestamp") -> DataFrame:
    """
    Add time dimension columns for partitioning and analysis.

    Args:
        df: Input DataFrame
        timestamp_col: Name of the timestamp column

    Returns:
        DataFrame with additional time columns
    """
    return df.withColumn(
        "transaction_date", F.to_date(F.col(timestamp_col))
    ).withColumn(
        "transaction_year", F.year(F.col(timestamp_col))
    ).withColumn(
        "transaction_month", F.month(F.col(timestamp_col))
    ).withColumn(
        "transaction_day", F.dayofmonth(F.col(timestamp_col))
    ).withColumn(
        "transaction_hour", F.hour(F.col(timestamp_col))
    ).withColumn(
        "day_of_week", F.dayofweek(F.col(timestamp_col))
    )


def flatten_address(df: DataFrame) -> DataFrame:
    """
    Flatten nested shipping address into separate columns.

    Args:
        df: Input DataFrame with shipping_address struct

    Returns:
        DataFrame with flattened address columns
    """
    return df.withColumn(
        "shipping_city", F.col("shipping_address.city")
    ).withColumn(
        "shipping_state", F.col("shipping_address.state")
    ).withColumn(
        "shipping_country", F.col("shipping_address.country")
    ).withColumn(
        "shipping_zip", F.col("shipping_address.zip_code")
    ).drop("shipping_address")


def calculate_metrics(df: DataFrame) -> DataFrame:
    """
    Calculate additional metrics for transactions.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with calculated metrics
    """
    return df.withColumn(
        "discount_applied",
        F.when(F.col("total_price") < F.col("unit_price") * F.col("quantity"), True)
        .otherwise(False)
    ).withColumn(
        "price_per_unit_actual",
        F.round(F.col("total_price") / F.col("quantity"), 2)
    )


def aggregate_by_category(df: DataFrame) -> DataFrame:
    """
    Aggregate transaction metrics by product category.

    Args:
        df: Input DataFrame

    Returns:
        Aggregated DataFrame
    """
    return df.groupBy("product_category").agg(
        F.count("transaction_id").alias("total_transactions"),
        F.sum("quantity").alias("total_units_sold"),
        F.sum("total_price").alias("total_revenue"),
        F.avg("total_price").alias("avg_transaction_value"),
        F.min("total_price").alias("min_transaction_value"),
        F.max("total_price").alias("max_transaction_value"),
        F.countDistinct("user_id").alias("unique_customers")
    )


def aggregate_by_time(df: DataFrame, time_col: str = "transaction_date") -> DataFrame:
    """
    Aggregate transaction metrics by time period.

    Args:
        df: Input DataFrame
        time_col: Time column to group by

    Returns:
        Aggregated DataFrame
    """
    return df.groupBy(time_col).agg(
        F.count("transaction_id").alias("total_transactions"),
        F.sum("total_price").alias("total_revenue"),
        F.avg("total_price").alias("avg_transaction_value"),
        F.countDistinct("user_id").alias("unique_customers"),
        F.countDistinct("product_id").alias("unique_products")
    ).orderBy(time_col)


def aggregate_by_status(df: DataFrame) -> DataFrame:
    """
    Aggregate transactions by status.

    Args:
        df: Input DataFrame

    Returns:
        Aggregated DataFrame with status breakdown
    """
    return df.groupBy("status").agg(
        F.count("transaction_id").alias("count"),
        F.sum("total_price").alias("total_value")
    ).withColumn(
        "percentage",
        F.round(
            F.col("count") / F.sum("count").over(Window.partitionBy()) * 100,
            2
        )
    )


def filter_completed_transactions(df: DataFrame) -> DataFrame:
    """
    Filter to only completed transactions.

    Args:
        df: Input DataFrame

    Returns:
        Filtered DataFrame
    """
    return df.filter(F.col("status") == "completed")


def filter_by_date_range(
    df: DataFrame,
    start_date: str,
    end_date: str,
    date_col: str = "transaction_date"
) -> DataFrame:
    """
    Filter transactions by date range.

    Args:
        df: Input DataFrame
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        date_col: Date column name

    Returns:
        Filtered DataFrame
    """
    return df.filter(
        (F.col(date_col) >= start_date) &
        (F.col(date_col) <= end_date)
    )


def deduplicate_transactions(df: DataFrame) -> DataFrame:
    """
    Remove duplicate transactions based on transaction_id.

    Args:
        df: Input DataFrame

    Returns:
        Deduplicated DataFrame
    """
    return df.dropDuplicates(["transaction_id"])


def clean_data(df: DataFrame) -> DataFrame:
    """
    Apply standard data cleaning transformations.

    Args:
        df: Input DataFrame

    Returns:
        Cleaned DataFrame
    """
    return (
        df
        # Remove nulls in critical fields
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("total_price") > 0)
        .filter(F.col("quantity") > 0)
        # Trim string fields
        .withColumn("product_category", F.trim(F.col("product_category")))
        .withColumn("payment_method", F.trim(F.col("payment_method")))
        # Deduplicate
        .transform(deduplicate_transactions)
    )


def prepare_for_analytics(df: DataFrame) -> DataFrame:
    """
    Full transformation pipeline to prepare data for analytics.

    Args:
        df: Raw input DataFrame

    Returns:
        Fully transformed DataFrame ready for analytics
    """
    return (
        df
        .transform(parse_timestamp)
        .transform(add_time_dimensions)
        .transform(flatten_address)
        .transform(calculate_metrics)
        .transform(clean_data)
    )


# Import Window for aggregate_by_status
from pyspark.sql.window import Window
