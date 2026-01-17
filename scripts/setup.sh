#!/bin/bash
# Initial setup script for the e-commerce data pipeline

set -e

echo "=========================================="
echo "E-Commerce Data Pipeline Setup"
echo "=========================================="

# Create data directories
echo "Creating data directories..."
mkdir -p data/raw data/processed data/output data/checkpoints logs

# Set permissions (for Linux/Mac)
if [[ "$OSTYPE" != "msys" && "$OSTYPE" != "win32" ]]; then
    chmod -R 777 data logs
fi

# Build Docker images
echo ""
echo "Building Docker images..."
docker-compose build

# Start services
echo ""
echo "Starting services..."
docker-compose up -d zookeeper
sleep 5

docker-compose up -d kafka
sleep 10

docker-compose up -d kafka-init
sleep 5

docker-compose up -d spark-master spark-worker-1 spark-worker-2
sleep 10

docker-compose up -d postgres
sleep 5

docker-compose up -d airflow-init
sleep 30

docker-compose up -d airflow-webserver airflow-scheduler

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Services running:"
echo "  - Kafka:          localhost:29092"
echo "  - Spark Master:   http://localhost:8080"
echo "  - Airflow:        http://localhost:8081 (admin/admin)"
echo ""
echo "To start the data producer:"
echo "  docker-compose --profile producer up kafka-producer"
echo ""
echo "To submit a Spark job:"
echo "  ./scripts/submit_spark_job.sh batch"
echo "  ./scripts/submit_spark_job.sh stream"
echo ""
