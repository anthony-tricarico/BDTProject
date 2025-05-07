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

SLEEP = float(os.getenv("SLEEP", "1"))
print("SLEEP is set to:", SLEEP, flush=True)

def parse_time(ts):
    # Adjust based on your data format
    if isinstance(ts, str):
        return datetime.fromisoformat(ts)
    return datetime.fromtimestamp(ts)

# Connection settings
db_user = 'postgres'
db_pass = 'example'
db_host = 'db'
db_port = '5432'
db_name = 'raw_data'

# SQLAlchemy connection string
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

def create_db_connection():
    global engine
    connection = None
    while connection is None:
        try:
             connection = engine.connect()
        except Exception as e:
            print(f"DB not ready, retrying in 3 seconds... ({e})")
            time.sleep(3)
    return connection

# get the max number of buses

print('after db connection')
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

shapes_dct = dict()

# def compute_distance(shapes_dct):
#     """
#     Computes Manhattan distances between points for each shape.
#     Returns a dict: shape_id -> list of distances.
#     """
#     all_distances = {}
#     for shape in shapes_dct:
#         distances = []
#         for coordinate in range(1, len(shapes_dct[shape]) - 1):
#             point_1 = np.array(shapes_dct[shape][coordinate])
#             point_2 = np.array(shapes_dct[shape][coordinate + 1])
#             distance = np.sum(np.abs(point_1 - point_2))
#             distances.append(float(distance))
#         all_distances[shape] = distances
#     return all_distances



def compute_distance(shapes_dct):
    """
    Computes Manhattan distances between points for each shape in kilometers.
    Returns a dict: shape_id -> list of distances in km.
    """
    all_distances = {}
    for shape in shapes_dct:
        distances = []
        for coordinate in range(1, len(shapes_dct[shape]) - 1):
            lat1, lon1 = shapes_dct[shape][coordinate]
            lat2, lon2 = shapes_dct[shape][coordinate + 1]

            # Compute absolute lat/lon differences
            d_lat = abs(lat2 - lat1)
            d_lon = abs(lon2 - lon1)
            avg_lat = (lat1 + lat2) / 2

            # Convert degree diffs to km
            lat_km = d_lat * 111.32
            lon_km = d_lon * 111.32 * math.cos(math.radians(avg_lat))

            distance_km = lat_km + lon_km  # Manhattan distance in km
            distances.append(distance_km)
        
        all_distances[shape] = distances
    return all_distances


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

producer = create_kafka_producer()

VELOCITY = 5 # km/h
def poll_stream_and_generate_gps():
    global shapes_dct
    global engine
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