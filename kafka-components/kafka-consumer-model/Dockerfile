FROM python:3.10-slim

WORKDIR /app
COPY kafka-consumer-model/model_evaluator.py .
COPY utils/ utils/

RUN pip install boto3 joblib scikit-learn kafka-python minio

CMD ["python", "model_evaluator.py"]