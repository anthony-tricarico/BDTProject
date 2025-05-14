FROM python:3.10-slim

WORKDIR /app
COPY kafka-consumer-weather/weather_consumer.py .
COPY utils/ utils/

RUN pip install kafka-python sqlalchemy psycopg2-binary

CMD ["python", "weather_consumer.py"]