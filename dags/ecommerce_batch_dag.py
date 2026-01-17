"""
Airflow DAG for batch processing of e-commerce transactions.
Orchestrates data ingestion, Spark processing, and output generation.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

# DAG definition
dag = DAG(
    'ecommerce_batch_processing',
    default_args=default_args,
    description='Daily batch processing of e-commerce transactions',
    schedule_interval='0 2 * * *',  # Run at 2 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ecommerce', 'batch', 'spark'],
    max_active_runs=1,
)


def generate_sample_data(**context):
    """Generate sample transaction data for processing."""
    import sys
    sys.path.insert(0, '/opt/airflow/src')

    from utils.data_generator import generate_batch
    from config.settings import settings
    import json
    from pathlib import Path

    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    output_path = Path(settings.DATA_RAW_PATH) / f"date={execution_date}"
    output_path.mkdir(parents=True, exist_ok=True)

    # Generate sample data
    transactions = generate_batch(batch_size=10000)

    # Save as JSON
    output_file = output_path / "transactions.json"
    with open(output_file, 'w') as f:
        for tx in transactions:
            f.write(json.dumps(tx) + '\n')

    logger.info(f"Generated {len(transactions)} transactions to {output_file}")

    return str(output_path)


def validate_raw_data(**context):
    """Validate raw data before processing."""
    import sys
    sys.path.insert(0, '/opt/airflow/src')

    from utils.validators import DataValidator
    from config.settings import settings
    from pathlib import Path

    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    data_path = Path(settings.DATA_RAW_PATH) / f"date={execution_date}"

    validator = DataValidator()
    validation_result = validator.validate_directory(str(data_path))

    if not validation_result['is_valid']:
        raise ValueError(f"Data validation failed: {validation_result['errors']}")

    logger.info(f"Validation passed: {validation_result}")
    return validation_result


def run_spark_batch_job(**context):
    """Submit Spark batch processing job."""
    import subprocess

    execution_date = context['execution_date'].strftime('%Y-%m-%d')

    # Build spark-submit command
    cmd = [
        '/opt/bitnami/spark/bin/spark-submit',
        '--master', 'spark://spark-master:7077',
        '--deploy-mode', 'client',
        '--driver-memory', '1g',
        '--executor-memory', '2g',
        '--executor-cores', '2',
        '--conf', 'spark.sql.shuffle.partitions=10',
        '--packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0',
        '/opt/airflow/src/spark/batch_processor.py',
        '--date', execution_date
    ]

    logger.info(f"Running Spark job: {' '.join(cmd)}")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        logger.error(f"Spark job failed: {result.stderr}")
        raise RuntimeError(f"Spark job failed with code {result.returncode}")

    logger.info(f"Spark job completed: {result.stdout}")
    return result.returncode


def validate_processed_data(**context):
    """Validate processed data quality."""
    import sys
    sys.path.insert(0, '/opt/airflow/src')

    from utils.validators import DataValidator
    from config.settings import settings

    validator = DataValidator()
    result = validator.validate_processed_data(
        str(settings.DATA_PROCESSED_PATH / "transactions")
    )

    context['ti'].xcom_push(key='processed_records', value=result.get('record_count', 0))

    if not result['is_valid']:
        raise ValueError(f"Processed data validation failed: {result['errors']}")

    return result


def generate_reports(**context):
    """Generate analytics reports from processed data."""
    import sys
    sys.path.insert(0, '/opt/airflow/src')

    from pyspark.sql import SparkSession
    from config.settings import settings

    spark = SparkSession.builder \
        .appName("ReportGeneration") \
        .master("local[*]") \
        .getOrCreate()

    try:
        # Read processed data
        df = spark.read.parquet(str(settings.DATA_PROCESSED_PATH / "transactions"))

        # Generate summary report
        summary = df.describe().toPandas()
        summary.to_csv(str(settings.DATA_OUTPUT_PATH / "daily_summary.csv"), index=False)

        # Category performance
        category_report = df.groupBy("product_category").agg({
            "total_price": "sum",
            "transaction_id": "count"
        }).toPandas()
        category_report.to_csv(str(settings.DATA_OUTPUT_PATH / "category_report.csv"), index=False)

        logger.info("Reports generated successfully")

    finally:
        spark.stop()


def cleanup_old_data(**context):
    """Clean up data older than retention period."""
    import sys
    sys.path.insert(0, '/opt/airflow/src')

    from config.settings import settings
    from pathlib import Path
    from datetime import datetime, timedelta
    import shutil

    retention_days = 30
    cutoff_date = datetime.now() - timedelta(days=retention_days)

    raw_path = Path(settings.DATA_RAW_PATH)
    deleted_count = 0

    for date_dir in raw_path.glob("date=*"):
        try:
            dir_date_str = date_dir.name.replace("date=", "")
            dir_date = datetime.strptime(dir_date_str, "%Y-%m-%d")

            if dir_date < cutoff_date:
                shutil.rmtree(date_dir)
                deleted_count += 1
                logger.info(f"Deleted old data: {date_dir}")

        except Exception as e:
            logger.warning(f"Error processing {date_dir}: {e}")

    logger.info(f"Cleanup complete: deleted {deleted_count} directories")
    return deleted_count


# Define tasks
with dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    # Data ingestion
    with TaskGroup('data_ingestion') as ingestion_group:
        generate_data = PythonOperator(
            task_id='generate_sample_data',
            python_callable=generate_sample_data,
            provide_context=True,
        )

        validate_raw = PythonOperator(
            task_id='validate_raw_data',
            python_callable=validate_raw_data,
            provide_context=True,
        )

        generate_data >> validate_raw

    # Spark processing
    with TaskGroup('spark_processing') as spark_group:
        run_spark = PythonOperator(
            task_id='run_spark_batch_job',
            python_callable=run_spark_batch_job,
            provide_context=True,
            execution_timeout=timedelta(hours=2),
        )

        validate_processed = PythonOperator(
            task_id='validate_processed_data',
            python_callable=validate_processed_data,
            provide_context=True,
        )

        run_spark >> validate_processed

    # Reporting
    with TaskGroup('reporting') as reporting_group:
        generate_reports_task = PythonOperator(
            task_id='generate_reports',
            python_callable=generate_reports,
            provide_context=True,
        )

    # Maintenance
    cleanup = PythonOperator(
        task_id='cleanup_old_data',
        python_callable=cleanup_old_data,
        provide_context=True,
    )

    # Task dependencies
    start >> ingestion_group >> spark_group >> reporting_group >> cleanup >> end
