FROM python:3.10-slim

WORKDIR /app

COPY kafka-producer-weather/weather.py .
COPY utils/ utils/

RUN pip install kafka-python requests numpy

CMD ["python", "weather.py"]