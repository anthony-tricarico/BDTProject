# producer/Dockerfile
FROM python:3.10-slim

WORKDIR /app

COPY training_pipeline.py .
COPY utils/ utils/
COPY models/ models/

RUN pip install kafka-python requests numpy scikit-learn pandas dask sqlalchemy pyarrow psycopg2-binary joblib boto3 minio dask[distributed] bokeh>=3.1.0
ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "training_pipeline.py"]