FROM python:3.10-slim

WORKDIR /app
COPY kafka-consumer-passengers/consumer_api.py .
COPY utils/ utils/

RUN pip install fastapi uvicorn kafka-python

CMD ["uvicorn", "consumer_api:app", "--host", "0.0.0.0", "--port", "8000"]