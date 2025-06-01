FROM python:3.10-slim

WORKDIR /app
COPY kafka-consumer-sensors/sensors_consumer.py .
COPY utils/ utils/
COPY kafka-consumer-sensors/requirements.txt .

RUN pip install -r requirements.txt

CMD ["uvicorn", "sensors_consumer:app", "--host", "0.0.0.0", "--port", "8002"]