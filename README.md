# E-Commerce Data Pipeline

End-to-end data pipeline simulating real-time e-commerce transaction processing using Apache Spark (PySpark), Kafka, and Airflow.

## Quick Start (Local - No Docker)

### Prerequisites
```bash
# Install Python dependencies
pip install -r requirements.txt

# You need Java 11+ for Spark
java -version
```

### Run the Pipeline Locally
```bash
# Simple one-command run
python run_local.py
```

This will:
1. Generate 5,000 sample transactions
2. Validate data quality
3. Process with Spark transformations
4. Generate analytics reports
5. Save results to `data/output/`

### Generate More Sample Data
```bash
# Generate 10,000 records
python scripts/generate_sample_data.py --records 10000
```

---

## Full Setup (With Docker)

### Prerequisites
- Docker Desktop installed and running
- At least 8GB RAM available

### Start All Services
```bash
# Build and start everything
docker-compose up -d

# Check status
docker-compose ps
```

### Access UIs
| Service | URL | Credentials |
|---------|-----|-------------|
| Spark Master | http://localhost:8080 | - |
| Airflow | http://localhost:8081 | admin / admin |

### Run Data Producer
```bash
docker-compose --profile producer up kafka-producer
```

### Submit Spark Jobs
```bash
# Batch processing
./scripts/submit_spark_job.sh batch

# Stream processing
./scripts/submit_spark_job.sh stream
```

### Stop Everything
```bash
docker-compose down
```

---

## Project Structure

```
kalfa/
├── run_local.py            # <-- START HERE (local execution)
├── docker-compose.yml      # Docker multi-service setup
├── requirements.txt        # Python dependencies
│
├── data/
│   ├── raw/                # Sample input data
│   ├── processed/          # Transformed data (Parquet)
│   └── output/             # Analytics reports
│
├── src/
│   ├── kafka/              # Kafka producer/consumer
│   ├── spark/              # Spark batch & streaming
│   ├── utils/              # Data generation & validation
│   └── monitoring/         # Metrics collection
│
├── dags/                   # Airflow DAGs
├── scripts/                # Utility scripts
└── tests/                  # Unit tests
```

---

## Sample Data

Sample data is provided in `data/raw/sample_transactions.json`

Each transaction contains:
```json
{
  "transaction_id": "txn_001",
  "user_id": "USR_a1b2c3d4",
  "product_id": "PRD_electronics_001",
  "product_name": "Wireless Bluetooth Headphones",
  "product_category": "Electronics",
  "quantity": 1,
  "unit_price": 79.99,
  "total_price": 79.99,
  "payment_method": "credit_card",
  "status": "completed",
  "timestamp": "2024-01-15T10:30:00",
  "shipping_address": {...},
  "device_type": "mobile",
  "browser": "chrome"
}
```

---

## Key Features

| Feature | Description |
|---------|-------------|
| Data Generation | Realistic e-commerce data with Faker |
| Kafka Streaming | 5,000+ events/minute throughput |
| Spark Batch | Partitioning, caching, broadcast joins |
| Spark Streaming | Windowed aggregations, watermarking |
| Airflow DAGs | Automated ETL with retry logic |
| Data Quality | Schema validation, completeness checks |
| Monitoring | Throughput, latency, error tracking |

---

## Running Tests
```bash
pytest tests/ -v
```

---

## Technologies Used

- **Apache Spark 3.5** - Distributed data processing
- **Apache Kafka** - Event streaming
- **Apache Airflow 2.8** - Workflow orchestration
- **Python 3.11** - Primary language
- **Docker** - Containerization
- **Parquet** - Columnar storage format
