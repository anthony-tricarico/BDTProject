FROM python:3.10

WORKDIR /app
COPY kafka-consumer-weather/weather_consumer.py .
COPY utils/ utils/
COPY kafka-consumer-weather/requirements.txt .

# RUN apt-get update && apt-get install -y gcc g++ build-essential

RUN pip install -r requirements.txt

CMD ["python", "weather_consumer.py"]