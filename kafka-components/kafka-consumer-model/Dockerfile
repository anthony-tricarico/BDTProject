FROM python:3.10-slim

WORKDIR /app
COPY kafka-consumer-model/model_evaluator.py .
COPY utils/ utils/

RUN pip install boto3 joblib scikit-learn kafka-python minio

# Force Python to run unbuffered
ENV PYTHONUNBUFFERED=1

# Run Python with -u flag for unbuffered output
CMD ["python", "-u", "model_evaluator.py"]