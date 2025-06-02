FROM python:3.10-slim

WORKDIR /app
COPY kafka-consumer-weather/weather_consumer.py .
COPY utils/ utils/
COPY kafka-consumer-weather/requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "weather_consumer.py"]