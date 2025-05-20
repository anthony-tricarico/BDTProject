# producer/Dockerfile
FROM python:3.10-slim

WORKDIR /app

COPY training_pipeline.py .
COPY utils/ utils/
COPY models/ models/

RUN pip install kafka-python requests scikit-learn pandas dask sqlalchemy pyarrow psycopg2-binary joblib boto3 minio

CMD ["python", "training_pipeline.py"]