# producer/Dockerfile
FROM python:3.10-slim

WORKDIR /app

COPY ticketing_producer.py .

RUN pip install kafka-python requests

CMD ["python", "ticketing_producer.py"]