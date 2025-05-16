import random
import json
import time
from kafka import KafkaConsumer
import os
from utils.kafka_producer import create_kafka_producer

SLEEP = float(os.getenv("SLEEP", "0.5"))

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
        "trip_id": msg['trip_id'],
        "peak_hour": msg['peak_hour'],
        # "event": msg['event'],
        "hospital": msg['hospital'],
        "school": msg['school']
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

def process_passenger_predictions():
    already_predicted = []
    
    # Create Kafka consumer for passenger predictions
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                'bus.passenger.predictions',
                bootstrap_servers='kafka:9092',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='tickets-producer-group'
            )
            print("Connected to Kafka topic: bus.passenger.predictions")
        except Exception as e:
            print(f"Kafka not ready, retrying in 3 seconds... ({e})")
            time.sleep(3)
    
    # Process messages from Kafka
    for message in consumer:
        try:
            msg = message.value
            
            if 'prediction_id' not in msg:
                print("Missing prediction_id in message:", msg)
                continue
                
            if msg['prediction_id'] not in already_predicted:
                predicted_in = msg.get('predicted_passengers_in', 0)
                already_predicted.append(msg['prediction_id'])

                for i in range(predicted_in):
                    ticket = generate_ticket(msg)
                    print("Sending ticket:", ticket)
                    producer.send('ticketing.topic', value=ticket)
                    
        except Exception as e:
            print("Error processing message:", e)

        time.sleep(float(SLEEP))

if __name__ == "__main__":
    process_passenger_predictions()