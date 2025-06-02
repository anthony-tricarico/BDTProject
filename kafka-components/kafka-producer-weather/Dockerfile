FROM python:3.10-slim

WORKDIR /app

COPY kafka-producer-weather/weather.py .
COPY utils/ utils/
COPY kafka-producer-weather/requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "weather.py"]