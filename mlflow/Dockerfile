FROM ghcr.io/mlflow/mlflow:latest

# Install psycopg2-binary for PostgreSQL support
RUN pip install --no-cache-dir psycopg2-binary

# Set environment variables
ENV MLFLOW_TRACKING_URI=postgresql://postgres:example@db:5432/raw_data

# Expose the port
EXPOSE 5001

# Run MLflow server
CMD ["mlflow", "server", "--host", "0.0.0.0", "--port", "5001", "--backend-store-uri", "postgresql://postgres:example@db:5432/raw_data"] 