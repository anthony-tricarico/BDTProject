import requests
import random
import json
import time
import os
from utils.kafka_producer import create_kafka_producer

SLEEP = os.getenv("SLEEP")

producer = create_kafka_producer()

def poll_stream_and_generate_tickets():
    while True:
        try:
            # Call your Kafka-exposed API
            response = requests.get("http://kafka-consumer-passengers:8000/stream")
            if response.status_code == 200:
                messages = response.json()

                for msg in messages:
                    predicted_in = msg.get('predicted_passengers_in', 0)

                    for i in range(predicted_in):
                        ticket = generate_ticket(msg)
                        print("Sending ticket:", ticket)
                        producer.send('ticketing.topic', value=ticket)

        except Exception as e:
            print("Error:", e)

        time.sleep(float(SLEEP))

def generate_ticket(msg):
    ticket_id = f"{msg['stop_id']}-{msg['route']}-{msg['timestamp']}-{random.randint(1000, 9999)}"
    passenger_type = random.choice(["adult", "student", "senior"])
    fare = {
        "adult": 2.50,
        "student": 1.25,
        "senior": 1.00
    }[passenger_type]

    return {
        "ticket_id": ticket_id,
        "timestamp": msg['timestamp'],
        "stop_id": msg['stop_id'],
        "route": msg['route'],
        "passenger_type": passenger_type,
        "fare": fare,
        "bus_id": msg['bus_id'],
        "trip_id": msg['trip_id']
    }

if __name__ == "__main__":
    poll_stream_and_generate_tickets()