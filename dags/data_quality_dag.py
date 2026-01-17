"""
Airflow DAG for data quality monitoring and validation.
Runs periodic checks on data completeness, freshness, and integrity.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
import logging
import json

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_quality_monitoring',
    default_args=default_args,
    description='Data quality checks and monitoring',
    schedule_interval='0 * * * *',  # Run every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ecommerce', 'quality', 'monitoring'],
)


def check_data_freshness(**context):
    """Check if data is being updated regularly."""
    import sys
    sys.path.insert(0, '/opt/airflow/src')

    from pathlib import Path
    from datetime import datetime, timedelta
    from config.settings import settings

    results = {
        'check': 'data_freshness',
        'timestamp': datetime.utcnow().isoformat(),
        'status': 'pass',
        'details': {}
    }

    # Check raw data freshness
    raw_path = Path(settings.DATA_RAW_PATH)
    if raw_path.exists():
        latest_file = max(raw_path.rglob('*'), key=lambda p: p.stat().st_mtime, default=None)
        if latest_file:
            last_modified = datetime.fromtimestamp(latest_file.stat().st_mtime)
            age_hours = (datetime.now() - last_modified).total_seconds() / 3600

            results['details']['raw_data'] = {
                'last_modified': last_modified.isoformat(),
                'age_hours': round(age_hours, 2)
            }

            if age_hours > 24:
                results['status'] = 'warning'
                results['details']['raw_data']['message'] = 'Data older than 24 hours'

    # Check processed data freshness
    processed_path = Path(settings.DATA_PROCESSED_PATH)
    if processed_path.exists():
        parquet_files = list(processed_path.rglob('*.parquet'))
        if parquet_files:
            latest_file = max(parquet_files, key=lambda p: p.stat().st_mtime)
            last_modified = datetime.fromtimestamp(latest_file.stat().st_mtime)
            age_hours = (datetime.now() - last_modified).total_seconds() / 3600

            results['details']['processed_data'] = {
                'last_modified': last_modified.isoformat(),
                'age_hours': round(age_hours, 2),
                'file_count': len(parquet_files)
            }

    logger.info(f"Freshness check: {json.dumps(results)}")
    context['ti'].xcom_push(key='freshness_results', value=results)

    return results


def check_data_completeness(**context):
    """Check data completeness metrics."""
    import sys
    sys.path.insert(0, '/opt/airflow/src')

    from pyspark.sql import SparkSession
    from config.settings import settings

    results = {
        'check': 'data_completeness',
        'timestamp': datetime.utcnow().isoformat(),
        'status': 'pass',
        'details': {}
    }

    spark = SparkSession.builder \
        .appName("DataQualityCheck") \
        .master("local[*]") \
        .getOrCreate()

    try:
        processed_path = str(settings.DATA_PROCESSED_PATH / "transactions")

        try:
            df = spark.read.parquet(processed_path)
            total_records = df.count()

            # Check null rates for critical fields
            null_checks = {}
            critical_fields = ['transaction_id', 'user_id', 'product_id', 'total_price', 'status']

            for field in critical_fields:
                if field in df.columns:
                    null_count = df.filter(df[field].isNull()).count()
                    null_rate = null_count / total_records if total_records > 0 else 0
                    null_checks[field] = {
                        'null_count': null_count,
                        'null_rate': round(null_rate * 100, 2)
                    }

                    if null_rate > 0.01:  # >1% null rate is warning
                        results['status'] = 'warning'

            results['details'] = {
                'total_records': total_records,
                'null_checks': null_checks
            }

        except Exception as e:
            results['status'] = 'error'
            results['details']['error'] = str(e)

    finally:
        spark.stop()

    logger.info(f"Completeness check: {json.dumps(results)}")
    context['ti'].xcom_push(key='completeness_results', value=results)

    return results


def check_data_integrity(**context):
    """Check data integrity and consistency."""
    import sys
    sys.path.insert(0, '/opt/airflow/src')

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from config.settings import settings

    results = {
        'check': 'data_integrity',
        'timestamp': datetime.utcnow().isoformat(),
        'status': 'pass',
        'details': {}
    }

    spark = SparkSession.builder \
        .appName("IntegrityCheck") \
        .master("local[*]") \
        .getOrCreate()

    try:
        processed_path = str(settings.DATA_PROCESSED_PATH / "transactions")

        try:
            df = spark.read.parquet(processed_path)

            # Check for duplicates
            total = df.count()
            distinct = df.select('transaction_id').distinct().count()
            duplicate_rate = (total - distinct) / total if total > 0 else 0

            results['details']['duplicates'] = {
                'total_records': total,
                'distinct_transactions': distinct,
                'duplicate_rate': round(duplicate_rate * 100, 2)
            }

            if duplicate_rate > 0.01:
                results['status'] = 'warning'

            # Check price consistency
            price_issues = df.filter(
                (F.col('total_price') < 0) |
                (F.col('unit_price') < 0) |
                (F.col('quantity') < 1)
            ).count()

            results['details']['price_consistency'] = {
                'invalid_prices': price_issues,
                'invalid_rate': round(price_issues / total * 100, 2) if total > 0 else 0
            }

            if price_issues > 0:
                results['status'] = 'warning'

            # Check status distribution
            status_dist = df.groupBy('status').count().collect()
            results['details']['status_distribution'] = {
                row['status']: row['count'] for row in status_dist
            }

        except Exception as e:
            results['status'] = 'error'
            results['details']['error'] = str(e)

    finally:
        spark.stop()

    logger.info(f"Integrity check: {json.dumps(results)}")
    context['ti'].xcom_push(key='integrity_results', value=results)

    return results


def check_schema_compliance(**context):
    """Verify data conforms to expected schema."""
    import sys
    sys.path.insert(0, '/opt/airflow/src')

    from pyspark.sql import SparkSession
    from config.settings import settings
    from spark.transformations import TRANSACTION_SCHEMA

    results = {
        'check': 'schema_compliance',
        'timestamp': datetime.utcnow().isoformat(),
        'status': 'pass',
        'details': {}
    }

    spark = SparkSession.builder \
        .appName("SchemaCheck") \
        .master("local[*]") \
        .getOrCreate()

    try:
        processed_path = str(settings.DATA_PROCESSED_PATH / "transactions")

        try:
            df = spark.read.parquet(processed_path)
            actual_fields = set(df.columns)
            expected_fields = set([f.name for f in TRANSACTION_SCHEMA.fields])

            missing_fields = expected_fields - actual_fields
            extra_fields = actual_fields - expected_fields

            results['details'] = {
                'expected_fields': list(expected_fields),
                'actual_fields': list(actual_fields),
                'missing_fields': list(missing_fields),
                'extra_fields': list(extra_fields)
            }

            if missing_fields:
                results['status'] = 'warning'
                results['details']['message'] = f"Missing fields: {missing_fields}"

        except Exception as e:
            results['status'] = 'error'
            results['details']['error'] = str(e)

    finally:
        spark.stop()

    logger.info(f"Schema check: {json.dumps(results)}")
    context['ti'].xcom_push(key='schema_results', value=results)

    return results


def aggregate_quality_report(**context):
    """Aggregate all quality check results into a report."""
    ti = context['ti']

    report = {
        'report_timestamp': datetime.utcnow().isoformat(),
        'overall_status': 'pass',
        'checks': {}
    }

    # Collect all check results
    check_keys = [
        'freshness_results',
        'completeness_results',
        'integrity_results',
        'schema_results'
    ]

    for key in check_keys:
        result = ti.xcom_pull(key=key)
        if result:
            report['checks'][result['check']] = result

            if result.get('status') == 'error':
                report['overall_status'] = 'error'
            elif result.get('status') == 'warning' and report['overall_status'] != 'error':
                report['overall_status'] = 'warning'

    # Save report
    import sys
    sys.path.insert(0, '/opt/airflow/src')

    from config.settings import settings
    from pathlib import Path

    report_path = Path(settings.DATA_OUTPUT_PATH) / "quality_reports"
    report_path.mkdir(parents=True, exist_ok=True)

    report_file = report_path / f"quality_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)

    logger.info(f"Quality report saved: {report_file}")
    logger.info(f"Overall status: {report['overall_status']}")

    if report['overall_status'] == 'error':
        raise ValueError(f"Data quality checks failed: {report}")

    return report


# Define tasks
with dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    with TaskGroup('quality_checks') as checks_group:
        freshness = PythonOperator(
            task_id='check_data_freshness',
            python_callable=check_data_freshness,
        )

        completeness = PythonOperator(
            task_id='check_data_completeness',
            python_callable=check_data_completeness,
        )

        integrity = PythonOperator(
            task_id='check_data_integrity',
            python_callable=check_data_integrity,
        )

        schema = PythonOperator(
            task_id='check_schema_compliance',
            python_callable=check_schema_compliance,
        )

    aggregate_report = PythonOperator(
        task_id='aggregate_quality_report',
        python_callable=aggregate_quality_report,
    )

    start >> checks_group >> aggregate_report >> end
