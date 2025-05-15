FROM python:3.9

WORKDIR /app

COPY kafka-consumer-gps/gps_consumer.py .
COPY kafka-consumer-gps/requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "gps_consumer.py"] 