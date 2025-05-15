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
from utils.db_connect import create_db_connection
from utils.kafka_producer import create_kafka_producer

SLEEP = os.getenv("SLEEP")

def get_passengers():
    # global pred_id
    # global bus_deactive_list
    # global max_buses
    # global bus_active_list
    # global app_start

    with open("treemodel.pkl", "rb") as f:
        model = load(f)

    with open("labelencoder.pkl", "rb") as f:
        le = load(f)

    with open("treemodel_out.pkl", "rb") as f:
        model_out = load(f)

    # get the max number of buses
    connection = create_db_connection()

    with connection:
        result = connection.execute(text("SELECT MAX(bus_id) FROM bus"))
        for row in result:
            max_buses = row[0]
    
    producer = create_kafka_producer()

    app_start = (datetime.today() + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)# app_start = datetime(2025, 1, 1, 0, 0, 0)

    pred_id = 0

    df = pd.read_csv('stop_times_passengers_shapes.csv')
    unique_trip_ids = list(df['trip_id'].unique())

    bus_deactive_list = [i for i in range(1, max_buses+1)]
    # ensure we do not pop always the same element
    random.shuffle(bus_deactive_list)
    bus_active_list = []

    # real_start = time.time()
    while True:
        # for each trip_id get stop and route, and assign one unique bus to the trip 
        
        for trip_id in unique_trip_ids:
            # max_sequence = str(df[df['trip_id'] == trip_id]['stop_sequence'].max())
            # cur_on = 0
            # cur_off = 0
            # on_bus = 0
            try:
                bus_active_list.append(bus_deactive_list.pop())
            except:
                bus_deactive_list = [i for i in range(1, max_buses+1)]
                random.shuffle(bus_deactive_list)
                bus_active_list = []
                bus_active_list.append(bus_deactive_list.pop())
            # get smaller dataset of unique trip
            # for testing does not include null shape ids
            for _, row in df[(df['trip_id'] == trip_id) & ~(df['shape_id'].isna()) & (df['route_short_name'].isin(["5", "5/", "8"]))].iterrows():
                # extract relevant information for each row wich corresponds to a stop
                shape_id = row.loc['shape_id']
                trip_idx = trip_id
                timestamp = row.loc['arrival_time']
                route = row.loc['route_short_name']
                stop = row.loc['stop_id'] 
                sequence = str(row.loc['stop_sequence'])

                sim_time = app_start + pd.to_timedelta(timestamp)
                seconds = (sim_time - sim_time.replace(hour=0, minute=0, second=0)).total_seconds()
                data_x = pd.DataFrame({
                    'arrival_time': [seconds],
                    'stop_id': [stop],
                    'encoded_routes': [le.transform([str(route)])[0]]
                })
                
                # Ensure the prediction is not negative
                # if int(sequence) != int(max_sequence):
                passenger_in = max(0, int(model.predict(data_x)[0]) - 10)
                passenger_out = max(0, int(model_out.predict(data_x)[0]))
                # cur_on += passenger_in
                # cur_off += passenger_out
                # on_bus += (cur_on - cur_off)
                # cur_on = 0
                # cur_off = 0
                # else:
                #     # passenger_in = max(0, int(model.predict(data_x)[0]) - 10)
                #     passenger_in = 0
                #     passenger_out = on_bus
                #     on_bus -= passenger_out


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
                    'bus_id': int(bus_active_list[-1]),
                    # 'on_bus': on_bus
                }
                print("Sending:", payload)
                producer.send('bus.passenger.predictions', value=payload)
                pred_id += 1
                time.sleep(float(SLEEP))

        app_start = app_start + timedelta(days=1) 


if __name__ == "__main__":
    get_passengers()