import random
import json
import time
from kafka import KafkaConsumer, KafkaProducer
import os
from sqlalchemy import create_engine, text
import numpy as np
from datetime import datetime, timedelta
import math
from utils.kafka_producer import create_kafka_producer
from utils.time_utils import parse_time
from utils.db_connect import create_db_connection
from utils.distance_utils import compute_distance

SLEEP = float(os.getenv("SLEEP", "1"))

shapes_dct = dict()

def generate_gps(msg, shape_id, index, timestamp):
    """
    variable timestamp to get as input a changing timestamp
    """
    global shapes_dct

    measurement_id = f"{msg['stop_id']}-{msg['route']}-{msg['timestamp']}-{random.randint(1000, 9999)}"

    all_distances = compute_distance(shapes_dct)
    distance = all_distances.get(shape_id, [])

    if index >= len(distance):
        print(f"Index {index} out of bounds for shape_id {shape_id} (distance len = {len(distance)})", flush=True)
        return None

    curr_distance = distance[index]
    time.sleep(float(curr_distance))

    return {
        "measurement_id": str(measurement_id),
        "timestamp": timestamp,
        "stop_id": msg['stop_id'],
        "route": msg['route'],
        "bus_id": msg['bus_id'],
        "trip_id": msg['trip_id'],
        "shape_id": msg['shape_id'],
        "sequence": msg['stop_sequence'],
        "latitude": shapes_dct[shape_id][index][0],
        "longitude": shapes_dct[shape_id][index][1]
    }

VELOCITY = 30 # km/h
def process_passenger_predictions():
    global shapes_dct

    # Create Kafka producer for GPS data
    producer = create_kafka_producer()
    
    # Create Kafka consumer for passenger predictions
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                'bus.passenger.predictions',
                bootstrap_servers='kafka:9092',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='gps-producer-group'
            )
            print("Connected to Kafka topic: bus.passenger.predictions")
        except Exception as e:
            print(f"Kafka not ready, retrying in 3 seconds... ({e})")
            time.sleep(3)
    
    tried_shapes = []
    
    # Process messages from Kafka
    for message in consumer:
        try:
            msg = message.value
            shape_id = msg.get("shape_id")
            timestamp = msg.get('timestamp')

            if not shape_id or not timestamp:
                print("Missing shape_id or timestamp in message:", msg, flush=True)
                continue

            try:
                timestamp_converted = parse_time(timestamp)
            except ValueError as ve:
                print("Timestamp conversion error:", ve, "timestamp:", timestamp, flush=True)
                continue

            if shape_id not in shapes_dct and shape_id != 'nan':
                # Get shape geometry from the database
                connection = create_db_connection()
                shapes_dct[shape_id] = dict()
                with connection:
                    result = connection.execute(
                        text("SELECT shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence FROM shapes WHERE shape_id = :shape_id"),
                        {"shape_id": shape_id}
                    )
                    rows = list(result)
                
                if not rows:
                    print(f"No coordinates found for shape_id: {shape_id}", flush=True)
                    continue

                for row in rows:
                    shapes_dct[shape_id][row[3]] = (row[1], row[2])
            
                all_distances = compute_distance(shapes_dct)
                distances = all_distances.get(shape_id, [])
                
                if len(distances) < 2:
                    print(f"Not enough coordinates for shape_id: {shape_id}", flush=True)
                    continue
                
                timestamp_obj = timestamp_converted
                
                for i in range(1, len(distances)):
                    estimated_travel_time = distances[i] / VELOCITY #in hours
                    estimated_travel_time_seconds = estimated_travel_time * 3600 # in seconds
                    timestamp_obj = timestamp_obj + timedelta(seconds=estimated_travel_time_seconds)
                    sensor = generate_gps(msg, shape_id, i, str(timestamp_obj))
                    if sensor is not None:
                        print("Sending GPS data:", sensor)
                        producer.send("gps.topic", value=sensor)

        except Exception as e:
            print("Error processing message:", e, flush=True)
            
        time.sleep(float(SLEEP))


if __name__ == "__main__":
    process_passenger_predictions()