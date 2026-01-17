#!/bin/bash
# Submit Spark jobs to the cluster

SPARK_MASTER=${SPARK_MASTER:-"spark://spark-master:7077"}
JOB_TYPE=${1:-"batch"}

echo "Submitting Spark $JOB_TYPE job to $SPARK_MASTER..."

case $JOB_TYPE in
    "batch")
        docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
            --master "$SPARK_MASTER" \
            --deploy-mode client \
            --driver-memory 1g \
            --executor-memory 2g \
            --executor-cores 2 \
            --conf spark.sql.shuffle.partitions=10 \
            --conf spark.default.parallelism=10 \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
            /opt/spark-apps/src/spark/batch_processor.py
        ;;
    "stream")
        docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
            --master "$SPARK_MASTER" \
            --deploy-mode client \
            --driver-memory 1g \
            --executor-memory 2g \
            --executor-cores 2 \
            --conf spark.sql.shuffle.partitions=10 \
            --conf spark.streaming.stopGracefullyOnShutdown=true \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
            /opt/spark-apps/src/spark/stream_processor.py
        ;;
    *)
        echo "Usage: $0 [batch|stream]"
        exit 1
        ;;
esac

echo "Job submitted successfully!"
