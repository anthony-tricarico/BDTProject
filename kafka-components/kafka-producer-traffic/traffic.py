import random
import requests
import json
import time
from kafka import KafkaConsumer, KafkaProducer
import os
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
from utils.db_connect import create_db_connection
from utils.kafka_producer import create_kafka_producer

load_dotenv()

SLEEP = float(os.getenv("SLEEP", "1"))
print("SLEEP is set to:", SLEEP, flush=True)
# read API KEY from .env file
GOOGLE_API_KEY = str(os.getenv("GOOGLE_API_KEY", None))

def build_get_traffic(destinations: tuple, origins: tuple, key: str, departure_time: str ='now'):
    gmaps_endpoint = "https://maps.googleapis.com/maps/api/distancematrix/json?"
    return f"{gmaps_endpoint}destinations={destinations[0]}%2C{destinations[1]}&origins={origins[0]}%2C{origins[1]}&key={key}&departure_time={departure_time}"

def generate_traffic(msg, traffic_level, normal_time, traffic_time):
    measurement_id = f"{msg['stop_id']}-{msg['route']}-{msg['timestamp']}-{random.randint(1000, 9999)}"
    
    return {
        "measurement_id": str(measurement_id),
        "timestamp": msg['timestamp'],
        "stop_id": msg['stop_id'],
        "route": msg['route'],
        "bus_id": msg['bus_id'],
        "trip_id": msg['trip_id'],
        "traffic_level": traffic_level,
        "normal": normal_time,
        "traffic": traffic_time

    }

def unique_shapes():
    shapes = []
    connection = create_db_connection()
    with connection:
        result = connection.execute(
            text("SELECT DISTINCT(shape_id) FROM shapes")
        )
    for row in result:
        shapes.append(row[0])

    return shapes

shapes_unique = unique_shapes()
producer = create_kafka_producer()
all_coordinates = []
tried_shapes = []

def process_passenger_predictions(key: str = GOOGLE_API_KEY):
    global all_coordinates
    global tried_shapes
    
    # Create Kafka consumer for passenger predictions
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                'bus.passenger.predictions',
                bootstrap_servers='kafka:9092',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='traffic-producer-group'
            )
            print("Connected to Kafka topic: bus.passenger.predictions")
        except Exception as e:
            print(f"Kafka not ready, retrying in 3 seconds... ({e})")
            time.sleep(3)
    
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
                timestamp_converted = int(datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S").timestamp())
            except ValueError as ve:
                print("Timestamp conversion error:", ve, "timestamp:", timestamp, flush=True)
                continue

            if len(tried_shapes) == len(shapes_unique):
                tried_shapes = []

            if shape_id in tried_shapes:
                continue
            tried_shapes.append(shape_id)

            connection = create_db_connection()
            with connection:
                result = connection.execute(
                    text("SELECT shape_pt_lat, shape_pt_lon FROM shapes WHERE shape_id = :shape_id"),
                    {"shape_id": shape_id}
                )
                rows = list(result)

            if not rows:
                print(f"No coordinates found for shape_id: {shape_id}", flush=True)
                continue

            all_coordinates = [(row[0], row[1]) for row in rows]

            if len(all_coordinates) < 2:
                print(f"Not enough coordinates for shape_id: {shape_id}", flush=True)
                continue

            origins = all_coordinates[0]
            destinations = all_coordinates[-1]

            req = build_get_traffic(destinations, origins, key, timestamp_converted)
            api_response = requests.get(req).json()

            try:
                element = api_response['rows'][0]['elements'][0]
                normal = element['duration']['value']
                traffic = element['duration_in_traffic']['value']
            except (IndexError, KeyError) as e:
                print("Error extracting traffic data:", e, flush=True)
                print("API response was:", json.dumps(api_response, indent=2), flush=True)
                continue

            time_traffic = (traffic - normal) / normal * 100

            if time_traffic <= 10:
                traffic_level = 'no traffic/low'
            elif time_traffic <= 30:
                traffic_level = 'medium'
            elif time_traffic < 100:
                traffic_level = 'heavy'
            else:
                traffic_level = 'severe/standstill'

            traffic_msg = generate_traffic(msg, traffic_level, normal, traffic)
            if traffic_msg:
                print(f"origins: {origins}, destinations: {destinations}")
                print("Sending traffic data:", traffic_msg, flush=True)
                producer.send("traffic.topic", value=traffic_msg)

            all_coordinates = []

        except Exception as e:
            print("Error processing message:", e, flush=True)

        time.sleep(SLEEP)

if __name__ == "__main__":
    process_passenger_predictions()