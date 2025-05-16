import random
import json
import time
from kafka import KafkaConsumer
import os
from utils.kafka_producer import create_kafka_producer

SLEEP = float(os.getenv("SLEEP", "1"))

producer = create_kafka_producer()

def generate_sensors(msg):
    if not hasattr(generate_sensors, "idx"):
        generate_sensors.idx = 0
    measurement_id = f"{msg['stop_id']}-{msg['route']}-{msg['timestamp']}-{generate_sensors.idx}"

    generate_sensors.idx += 1

    return {
        # this must depend on an internal generator, otherwise it would be overwritten by the upsert operation in MongoDB
        "measurement_id": measurement_id,
        "timestamp": msg['timestamp'],
        "stop_id": msg['stop_id'],
        "route": msg['route'],
        # hard-coded since the assumption is that 
        "status": 1,
        "activation_type": 2,
        "bus_id": msg['bus_id'],
        "trip_id": msg['trip_id'],
        "peak_hour": msg['peak_hour'],
        # "event": msg['event'],
        "hospital": msg['hospital'],
        "school": msg['school']
    }

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
                group_id='sensors-producer-group'
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
                predicted_out = msg.get('predicted_passengers_out', 0)
                already_predicted.append(msg['prediction_id'])

                for i in range(predicted_out):
                    sensor = generate_sensors(msg)
                    print("Sending sensor:", sensor)
                    producer.send("sensors.topic", value=sensor)
                    
        except Exception as e:
            print("Error processing message:", e)

        time.sleep(float(SLEEP))

if __name__ == "__main__":
    process_passenger_predictions()