# Use a base image with Python and Java (required for Spark)
FROM apache/spark-py:latest

# Set the working directory
WORKDIR /app

# Ensure we're root to avoid permission errors
USER root

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Spark streaming script
COPY spark_streaming_raw_to_sql.py .

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Create checkpoint directory
RUN mkdir -p /tmp/checkpoint && \
    chmod -R 777 /tmp/checkpoint

# Command to run the Spark streaming application using spark-submit
CMD ["spark-submit", \
     "--master", "local[*]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1", \
     "--conf", "spark.sql.streaming.checkpointLocation=/tmp/checkpoint", \
     "--conf", "spark.sql.shuffle.partitions=4", \
     "--conf", "spark.streaming.stopGracefullyOnShutdown=true", \
     "--conf", "spark.hadoop.fs.defaultFS=file:///", \
     "--conf", "spark.driver.extraJavaOptions=-Dio.netty.tryReflectionSetAccessible=true", \
     "--conf", "spark.executor.extraJavaOptions=-Dio.netty.tryReflectionSetAccessible=true", \
     "spark_streaming_raw_to_sql.py"] 