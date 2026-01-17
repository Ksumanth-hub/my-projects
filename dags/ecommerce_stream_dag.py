"""
Airflow DAG for managing streaming pipeline.
Controls start/stop of Kafka producer and Spark streaming jobs.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'ecommerce_streaming_pipeline',
    default_args=default_args,
    description='Manage streaming pipeline components',
    schedule_interval=None,  # Triggered manually or via API
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ecommerce', 'streaming', 'kafka', 'spark'],
)


def check_kafka_health(**context):
    """Check if Kafka is healthy and ready."""
    from kafka import KafkaAdminClient
    from kafka.errors import KafkaError
    import os

    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='airflow-health-check'
        )

        topics = admin.list_topics()
        admin.close()

        logger.info(f"Kafka healthy. Topics: {topics}")
        return True

    except KafkaError as e:
        logger.error(f"Kafka health check failed: {e}")
        return False


def check_spark_health(**context):
    """Check if Spark master is healthy."""
    import requests

    try:
        response = requests.get('http://spark-master:8080/json/', timeout=10)
        data = response.json()

        workers = data.get('workers', [])
        alive_workers = [w for w in workers if w.get('state') == 'ALIVE']

        logger.info(f"Spark healthy. Active workers: {len(alive_workers)}")
        return len(alive_workers) > 0

    except Exception as e:
        logger.error(f"Spark health check failed: {e}")
        return False


def start_kafka_producer(**context):
    """Start the Kafka producer for data generation."""
    import subprocess
    import os

    # Start producer in background
    cmd = [
        'docker', 'exec', '-d', 'kafka-producer',
        'python', '/opt/spark-apps/src/kafka/producer.py'
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

        if result.returncode == 0:
            logger.info("Kafka producer started successfully")
            return True
        else:
            logger.error(f"Failed to start producer: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        logger.warning("Producer start command timed out (may still be starting)")
        return True


def start_spark_streaming(**context):
    """Start Spark streaming job."""
    import subprocess

    cmd = [
        'docker', 'exec', '-d', 'spark-master',
        '/opt/bitnami/spark/bin/spark-submit',
        '--master', 'spark://spark-master:7077',
        '--deploy-mode', 'client',
        '--driver-memory', '1g',
        '--executor-memory', '2g',
        '--packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0',
        '--conf', 'spark.streaming.stopGracefullyOnShutdown=true',
        '/opt/spark-apps/src/spark/stream_processor.py'
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

        if result.returncode == 0:
            logger.info("Spark streaming job started")
            return True
        else:
            logger.error(f"Failed to start streaming: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        logger.warning("Streaming start timed out (may still be starting)")
        return True


def monitor_streaming(**context):
    """Monitor streaming job health."""
    import requests
    import time

    max_checks = 10
    check_interval = 30

    for i in range(max_checks):
        try:
            # Check Spark applications
            response = requests.get('http://spark-master:8080/json/')
            data = response.json()

            active_apps = data.get('activeapps', [])
            streaming_apps = [
                app for app in active_apps
                if 'streaming' in app.get('name', '').lower()
            ]

            if streaming_apps:
                logger.info(f"Streaming job running: {streaming_apps[0]['name']}")
                return True

        except Exception as e:
            logger.warning(f"Monitor check {i+1} failed: {e}")

        time.sleep(check_interval)

    logger.error("Streaming job not detected after monitoring period")
    return False


def stop_streaming(**context):
    """Gracefully stop streaming jobs."""
    import subprocess
    import requests

    try:
        # Get running apps
        response = requests.get('http://spark-master:8080/json/')
        data = response.json()

        for app in data.get('activeapps', []):
            app_id = app.get('id')
            if app_id:
                # Kill application
                kill_cmd = [
                    'docker', 'exec', 'spark-master',
                    '/opt/bitnami/spark/bin/spark-submit',
                    '--kill', app_id,
                    '--master', 'spark://spark-master:7077'
                ]
                subprocess.run(kill_cmd, timeout=30)
                logger.info(f"Stopped application: {app_id}")

        return True

    except Exception as e:
        logger.error(f"Error stopping streaming: {e}")
        return False


def collect_streaming_metrics(**context):
    """Collect and log streaming metrics."""
    import sys
    sys.path.insert(0, '/opt/airflow/src')

    from pathlib import Path
    from config.settings import settings
    import json

    metrics = {
        'timestamp': datetime.utcnow().isoformat(),
        'checkpoint_status': 'unknown',
        'output_files': 0
    }

    # Check checkpoint directory
    checkpoint_path = Path(settings.DATA_CHECKPOINT_PATH)
    if checkpoint_path.exists():
        metrics['checkpoint_status'] = 'exists'
        metrics['checkpoint_files'] = len(list(checkpoint_path.rglob('*')))

    # Check output directory
    output_path = Path(settings.DATA_OUTPUT_PATH) / "streaming"
    if output_path.exists():
        metrics['output_files'] = len(list(output_path.rglob('*.parquet')))

    logger.info(f"Streaming metrics: {json.dumps(metrics)}")

    context['ti'].xcom_push(key='streaming_metrics', value=metrics)
    return metrics


# Define tasks
with dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    # Health checks
    with TaskGroup('health_checks') as health_group:
        kafka_health = PythonSensor(
            task_id='check_kafka_health',
            python_callable=check_kafka_health,
            poke_interval=30,
            timeout=300,
            mode='poke',
        )

        spark_health = PythonSensor(
            task_id='check_spark_health',
            python_callable=check_spark_health,
            poke_interval=30,
            timeout=300,
            mode='poke',
        )

    # Start streaming components
    with TaskGroup('start_streaming') as start_group:
        start_producer = PythonOperator(
            task_id='start_kafka_producer',
            python_callable=start_kafka_producer,
        )

        start_spark = PythonOperator(
            task_id='start_spark_streaming',
            python_callable=start_spark_streaming,
        )

        start_producer >> start_spark

    # Monitoring
    monitor = PythonOperator(
        task_id='monitor_streaming',
        python_callable=monitor_streaming,
    )

    collect_metrics = PythonOperator(
        task_id='collect_streaming_metrics',
        python_callable=collect_streaming_metrics,
    )

    # Task flow
    start >> health_group >> start_group >> monitor >> collect_metrics >> end
