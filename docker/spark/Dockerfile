FROM bitnami/spark:3.5.0

USER root

# Install additional Python packages
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    confluent-kafka==2.3.0 \
    faker==22.0.0 \
    pyarrow==14.0.1 \
    pandas==2.1.4 \
    numpy==1.26.2 \
    python-dateutil==2.8.2 \
    pyyaml==6.0.1

# Create directories for applications and data
RUN mkdir -p /opt/spark-apps/src /opt/spark-apps/data

# Set working directory
WORKDIR /opt/spark-apps

# Set environment variables
ENV PYTHONPATH=/opt/spark-apps/src
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Copy entrypoint script
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

ENTRYPOINT ["/opt/entrypoint.sh"]
