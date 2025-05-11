# producer/Dockerfile
FROM python:3.10-slim

WORKDIR /app

COPY kafka-producer-sensors/sensors_producer.py .
COPY utils/ utils/

RUN pip install kafka-python requests

CMD ["python", "sensors_producer.py"]