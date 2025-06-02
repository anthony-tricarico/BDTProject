# producer/Dockerfile
FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

COPY training_pipeline.py .
COPY utils/ utils/
# COPY models/ models/

RUN pip install kafka-python requests numpy scikit-learn pandas dask sqlalchemy pyarrow psycopg2-binary joblib boto3 minio dask[distributed] bokeh>=3.1.0 xgboost>=1.7.0 mlflow
ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "training_pipeline.py"]