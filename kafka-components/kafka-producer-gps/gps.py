import requests
import random
import json
import time
from kafka import KafkaProducer
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
        "latitude": shapes_dct[shape_id][index][0],
        "longitude": shapes_dct[shape_id][index][1]
    }

VELOCITY = 30 # km/h
def poll_stream_and_generate_gps():
    global shapes_dct

    producer = create_kafka_producer()
    
    while True:
        try:
            response = requests.get("http://kafka-consumer-passengers:8000/stream")
            if response.status_code == 200:
                messages = response.json()

                for msg in messages:
                    shape_id = msg['shape_id']

                    if shape_id not in shapes_dct and shape_id != 'nan':
                        timestamp = parse_time(msg['timestamp'])

                        connection = create_db_connection()
                        shapes_dct[shape_id] = dict()
                        with connection:
                            result = connection.execute(
                                text("SELECT shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence FROM shapes WHERE shape_id = :shape_id"),
                                {"shape_id": shape_id}
                            )
                        for row in result:
                            shapes_dct[shape_id][row[3]] = (row[1], row[2])
                
                        all_distances = compute_distance(shapes_dct)
                        distances = all_distances[shape_id]
                        
                        for i in range(1, len(distances)):
                            estimated_travel_time = distances[i] / VELOCITY #in hours
                            estimated_travel_time_seconds = estimated_travel_time * 3600 # in seconds
                            timestamp = timestamp + timedelta(seconds=estimated_travel_time_seconds)
                            sensor = generate_gps(msg, shape_id, i, str(timestamp))
                            if sensor is not None:
                                print("Sending sensor:", sensor)
                                producer.send("gps.topic", value=sensor)

        except Exception as e:
            print("Error:", e)

        time.sleep(float(SLEEP))


if __name__ == "__main__":
    poll_stream_and_generate_gps()