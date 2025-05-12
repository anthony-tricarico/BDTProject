import requests
import random
import json
import time
from kafka import KafkaProducer
import os
from utils.kafka_producer import create_kafka_producer

SLEEP = os.getenv("SLEEP")

producer = create_kafka_producer()

def generate_sensors(msg):
    if not hasattr(generate_sensors, "idx"):
        generate_sensors.idx = 0
    measurement_id = f"{msg['stop_id']}-{msg['route']}-{msg['timestamp']}-{generate_sensors.idx}"

    generate_sensors.idx += 1

    return {
        "measurement_id": measurement_id,
        "timestamp": msg['timestamp'],
        "stop_id": msg['stop_id'],
        "route": msg['route'],
        # hard-coded since the assumption is that 
        "status": 1,
        "activation_type": 2,
        "bus_id": msg['bus_id'],
        "trip_id": msg['trip_id']
    }

def poll_stream_and_generate_sensors():
    while True:
        try:
            # Call Kafka-exposed API
            response = requests.get("http://kafka-consumer-passengers:8000/stream")
            if response.status_code == 200:
                messages = response.json()

                for msg in messages:
                    predicted_out = msg.get('predicted_passengers_out', 0)

                    for i in range(predicted_out):
                        sensor = generate_sensors(msg)
                        print("Sending sensor:", sensor)
                        producer.send("sensors.topic", value=sensor)

        except Exception as e:
            print("Error:", e)

        time.sleep(float(SLEEP))

if __name__ == "__main__":
    poll_stream_and_generate_sensors()