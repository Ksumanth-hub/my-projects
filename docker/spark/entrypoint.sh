#!/bin/bash
set -e

# Spark mode selection
if [ "$SPARK_MODE" = "master" ]; then
    echo "Starting Spark Master..."
    exec /opt/bitnami/spark/sbin/start-master.sh --host $SPARK_MASTER_HOST --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT
elif [ "$SPARK_MODE" = "worker" ]; then
    echo "Starting Spark Worker..."
    exec /opt/bitnami/spark/sbin/start-worker.sh $SPARK_MASTER_URL
else
    # Default: run the provided command
    exec "$@"
fi

# Keep container running
tail -f /dev/null
