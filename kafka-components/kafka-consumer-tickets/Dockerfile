FROM python:3.10-slim

WORKDIR /app
COPY kafka-consumer-tickets/ticketing_consumer.py .
COPY utils/ utils/

RUN pip install fastapi uvicorn kafka-python

CMD ["uvicorn", "ticketing_consumer:app", "--host", "0.0.0.0", "--port", "8001"]