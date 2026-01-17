#!/bin/bash
# Initialize Kafka topics for the e-commerce data pipeline

KAFKA_BOOTSTRAP_SERVER=${KAFKA_BOOTSTRAP_SERVER:-"localhost:29092"}

echo "Creating Kafka topics on $KAFKA_BOOTSTRAP_SERVER..."

# Function to create a topic
create_topic() {
    local topic_name=$1
    local partitions=$2
    local replication=$3

    echo "Creating topic: $topic_name (partitions: $partitions, replication: $replication)"

    docker exec kafka kafka-topics \
        --bootstrap-server kafka:9092 \
        --create \
        --if-not-exists \
        --topic "$topic_name" \
        --partitions "$partitions" \
        --replication-factor "$replication"
}

# Create main topics
create_topic "ecommerce-transactions" 3 1
create_topic "ecommerce-transactions-dlq" 1 1
create_topic "processed-transactions" 3 1

echo ""
echo "Listing all topics:"
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list

echo ""
echo "Topic details:"
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic ecommerce-transactions

echo ""
echo "Kafka topics initialized successfully!"
