# producer/Dockerfile
FROM python:3.10-slim

WORKDIR /app

COPY kafka-producer-sensors/sensors_producer.py .
COPY utils/ utils/
COPY kafka-producer-sensors/requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "sensors_producer.py"]