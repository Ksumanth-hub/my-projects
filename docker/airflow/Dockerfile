FROM apache/airflow:2.8.0-python3.11

USER root

# Install Java (required for Spark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.5.0 \
    apache-airflow-providers-apache-kafka==1.3.0 \
    kafka-python==2.0.2 \
    confluent-kafka==2.3.0 \
    pyspark==3.5.0 \
    pyarrow==14.0.1 \
    pandas==2.1.4 \
    faker==22.0.0 \
    python-dateutil==2.8.2 \
    pyyaml==6.0.1

# Set working directory
WORKDIR /opt/airflow
