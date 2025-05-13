import requests
import random
import json
import time
import os
from utils.kafka_producer import create_kafka_producer

SLEEP = os.getenv("SLEEP")

producer = create_kafka_producer()

def generate_ticket(msg):

    if not hasattr(generate_ticket, "idx"):
        generate_ticket.idx = 0

    ticket_id = f"{msg['stop_id']}-{msg['route']}-{msg['timestamp']}-{generate_ticket.idx}"

    generate_ticket.idx += 1
    
    passenger_type = random.choice(["adult", "student", "senior"])
    fare = {
        "adult": 2.50,
        "student": 1.25,
        "senior": 1.00
    }[passenger_type]

    return {
        # this must depend on an internal generator, otherwise it would be overwritten by the upsert operation in MongoDB
        "ticket_id": ticket_id,
        "timestamp": msg['timestamp'],
        "stop_id": msg['stop_id'],
        "route": msg['route'],
        "passenger_type": passenger_type,
        "fare": fare,
        "bus_id": msg['bus_id'],
        "trip_id": msg['trip_id']
    }

    # return {
    #     # this must depend on an internal generator, otherwise it would be overwritten by the upsert operation in MongoDB
    #     "ticket_id": ticket_id,
    #     "timestamp": msg['value']['timestamp'],
    #     "stop_id": msg['value']['stop_id'],
    #     "route": msg['value']['route'],
    #     "passenger_type": passenger_type,
    #     "fare": fare,
    #     "bus_id": msg['value']['bus_id'],
    #     "trip_id": msg['value']['trip_id']
    # }


def poll_stream_and_generate_tickets():
    already_predicted = []
    while True:
        try:
            # Call your Kafka-exposed API
            # check the limit to be used to change how many messages are retrieved each time
            response = requests.get("http://kafka-consumer-passengers:8000/stream")
            if response.status_code == 200:
                messages = response.json()

                for msg in messages:
                    if msg['prediction_id'] not in already_predicted:
                        predicted_in = msg.get('predicted_passengers_in', 0)
                        already_predicted.append(msg['prediction_id'])

                        for i in range(predicted_in):
                            ticket = generate_ticket(msg)
                            print("Sending ticket:", ticket)
                            producer.send('ticketing.topic', value=ticket)

        except Exception as e:
            print("Error:", e)

        time.sleep(float(SLEEP))


if __name__ == "__main__":
    poll_stream_and_generate_tickets()