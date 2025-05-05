import requests
import random
import json
import time
from kafka import KafkaProducer

def create_kafka_producer():
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Kafka producer connected.")
        except Exception as e:
            print(f"Kafka not ready, retrying in 3 seconds... ({e})")
            time.sleep(3)
    return producer

producer = create_kafka_producer()

def poll_stream_and_generate_sensors():
    while True:
        try:
            # Call your Kafka-exposed API
            response = requests.get("http://kafka-consumer-passengers:8000/stream")
            if response.status_code == 200:
                messages = response.json()

                for msg in messages:
                    predicted_in = msg.get('predicted_passengers_out', 0)

                    for i in range(predicted_in):
                        ticket = generate_sensors(msg)
                        print("Sending ticket:", ticket)
                        producer.send("sensors.topic", value=ticket)

        except Exception as e:
            print("Error:", e)

        time.sleep(0.5)

def generate_sensors(msg):
    measurement_id = f"{msg['stop_id']}-{msg['route']}-{msg['timestamp']}-{random.randint(1000, 9999)}"

    return {
        "measurement_id": measurement_id,
        "timestamp": msg['timestamp'],
        "stop_id": msg['stop_id'],
        "route": msg['route'],
        # hard-coded since the assumption is that 
        "status": "active"
    }

if __name__ == "__main__":
    poll_stream_and_generate_sensors()