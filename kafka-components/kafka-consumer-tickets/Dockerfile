FROM python:3.10-slim

WORKDIR /app
COPY kafka-consumer-tickets/ticketing_consumer.py .
COPY utils/ utils/
COPY kafka-consumer-tickets/requirements.txt .

RUN pip install -r requirements.txt

CMD ["uvicorn", "ticketing_consumer:app", "--host", "0.0.0.0", "--port", "8001"]