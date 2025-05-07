# Inside producer/app.py
from kafka import KafkaProducer
import json
import time
from datetime import datetime, timedelta
import pandas as pd
from pickle import load
from sqlalchemy import create_engine, text
import random
import os

SLEEP = os.getenv("SLEEP")

# Load your model and encoder (ensure files are included in Docker image)
with open("treemodel.pkl", "rb") as f:
    model = load(f)

with open("labelencoder.pkl", "rb") as f:
    le = load(f)

with open("treemodel_out.pkl", "rb") as f:
    model_out = load(f)

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
connection = create_db_connection()

with connection:
    result = connection.execute(text("SELECT MAX(bus_id) FROM bus"))
    for row in result:
        max_buses = row[0]


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

app_start = datetime(2025, 1, 1, 0, 0, 0)

pred_id = 0

df = pd.read_csv('stop_times_passengers_shapes.csv')
unique_trip_ids = list(df['trip_id'].unique())

bus_deactive_list = [i for i in range(1, max_buses+1)]
 # ensure we do not pop always the same element
random.shuffle(bus_deactive_list)
bus_active_list = []

def get_passengers():
    global pred_id
    global bus_deactive_list
    global max_buses
    global bus_active_list
    global app_start

    # real_start = time.time()
    while True:
        # for each trip_id get stop and route, and assign one unique bus to the trip 
        for trip_id in unique_trip_ids:
            try:
                bus_active_list.append(bus_deactive_list.pop())
            except:
                bus_deactive_list = [i for i in range(1, max_buses+1)]
                random.shuffle(bus_deactive_list)
                bus_active_list = []
                bus_active_list.append(bus_deactive_list.pop())
            # get smaller dataset of unique trip
            # for testing does not include null shape ids
            for _, row in df[(df['trip_id'] == trip_id) & ~(df['shape_id'].isna())].iterrows():
                # extract relevant information for each row wich corresponds to a stop
                shape_id = row.loc['shape_id']
                trip_idx = trip_id
                timestamp = row.loc['arrival_time']
                route = row.loc['route_short_name']
                stop = row.loc['stop_id'] 
                sequence = row.loc['stop_sequence']

                sim_time = app_start + pd.to_timedelta(timestamp)
                seconds = (sim_time - sim_time.replace(hour=0, minute=0, second=0)).total_seconds()
                data_x = pd.DataFrame({
                    'arrival_time': [seconds],
                    'stop_id': [stop],
                    'encoded_routes': [le.transform([str(route)])[0]]
                })

                passenger_in = int(model.predict(data_x)[0])
                passenger_out = int(model_out.predict(data_x)[0])

                payload = {
                    'prediction_id': pred_id,
                    'timestamp': sim_time.isoformat(),
                    # convert to str to avoid serialization issues when saved in JSON
                    'stop_id': str(stop),
                    'route': str(route),
                    'predicted_passengers_in': passenger_in,
                    'predicted_passengers_out': passenger_out,
                    'shape_id': str(shape_id),
                    'trip_id': str(trip_idx),
                    'stop_sequence': str(sequence),
                    'bus_id': int(bus_active_list[-1])
                }
                print("Sending:", payload)
                producer.send('bus.passenger.predictions', value=payload)
                pred_id += 1
                time.sleep(float(SLEEP))

        app_start = app_start + timedelta(days=1) 


if __name__ == "__main__":
    get_passengers()