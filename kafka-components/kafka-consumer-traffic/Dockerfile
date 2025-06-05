FROM python:3.9

WORKDIR /app

COPY kafka-consumer-traffic/traffic_consumer.py .
COPY kafka-consumer-traffic/requirements.txt .

# RUN apt-get update && apt-get install -y gcc g++ build-essential
RUN pip install -r requirements.txt

CMD ["python", "traffic_consumer.py"] 