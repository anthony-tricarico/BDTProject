# producer/Dockerfile
FROM python:3.10-slim

WORKDIR /app

COPY kafka-producer-tickets/ticketing_producer.py .
COPY utils/ utils/
COPY kafka-producer-tickets/requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "ticketing_producer.py"]