# Spark Components for Streaming and Data Processing

This component contains Spark streaming scripts and configurations to process real-time data from Kafka and write it to a PostgreSQL database. The scripts are designed to handle raw data ingestion and processing for bus occupancy management.

## Contents

- `spark_streaming_raw_to_sql.py`: Script to read raw data from Kafka topics and write it to PostgreSQL.
- `Dockerfile`: Dockerfile to set up a Spark streaming environment in a Docker container.
- `requirements.txt`: List of Python dependencies required for the Spark scripts.

## Usage

- The `spark_streaming_raw_to_sql.py` script reads raw data from Kafka topics and writes it directly to PostgreSQL tables `raw_sensors` and `raw_tickets`.

## Environment Variables

Ensure the following environment variables are set for Kafka and PostgreSQL connections:

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka server addresses (e.g., `kafka:9092`)
- `POSTGRES_URL`: JDBC URL for the PostgreSQL database (e.g., `jdbc:postgresql://db:5432/raw_data`)
- `POSTGRES_USER`: PostgreSQL user (e.g., `postgres`)
- `POSTGRES_PASSWORD`: Password for the PostgreSQL user

## Notes

- Ensure that Kafka and PostgreSQL services are running and accessible from the Docker container.
- The Spark scripts are configured to handle streaming data and require appropriate Kafka topics and PostgreSQL tables to function correctly.
