FROM python:3.10-slim

WORKDIR /app
COPY consumer_api.py .

RUN pip install fastapi uvicorn kafka-python

CMD ["uvicorn", "consumer_api:app", "--host", "0.0.0.0", "--port", "8000"]