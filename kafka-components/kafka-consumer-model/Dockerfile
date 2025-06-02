FROM python:3.10-slim

WORKDIR /app
COPY kafka-consumer-model/model_evaluator.py .
COPY utils/ utils/
COPY kafka-consumer-model/requirements.txt .

RUN pip install -r requirements.txt

# Force Python to run unbuffered
ENV PYTHONUNBUFFERED=1

# Run Python with -u flag for unbuffered output
CMD ["python", "-u", "model_evaluator.py"]