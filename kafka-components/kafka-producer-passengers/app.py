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
from dotenv import load_dotenv

# parse .env file and add all variables contained in it as environment variables 
load_dotenv()
SLEEP = os.getenv("SLEEP")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", None)

def get_passengers():

    # with open("treemodel.pkl", "rb") as f:
    #     model = load(f)

    # with open("labelencoder.pkl", "rb") as f:
    #     le = load(f)

    # with open("treemodel_out.pkl", "rb") as f:
    #     model_out = load(f)

    # get the max number of buses
    connection = create_db_connection()

    with connection:
        result = connection.execute(text("SELECT MAX(bus_id) FROM bus"))
        for row in result:
            max_buses = row[0]
    
    producer = create_kafka_producer()

    app_start = (datetime.today() + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0) # always start on day after the current one

    pred_id = 0

    df = pd.read_csv('passengers_with_shapes.csv')
    unique_trip_ids = list(df['trip_id'].unique())

    bus_deactive_list = [i for i in range(1, max_buses+1)]
    # ensure we do not pop always the same element
    random.shuffle(bus_deactive_list)
    bus_active_list = []

    # max_passengers_per_trip = 100  # Cap for each trip

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
            running_in = 0
            running_out = 0
            for _, row in df[(df['trip_id'] == trip_id) & ~(df['shape_id'].isna()) & (df['route_short_name'].isin(["5", "8", "5/", "2"]))].iterrows():
                # extract relevant information for each row which corresponds to a stop
                shape_id = row.loc['shape_id']
                trip_idx = trip_id
                timestamp = row.loc['departure_time_obj']
                route = row.loc['route_short_name']
                stop = row.loc['stop_id'] 
                sequence = str(row.loc['stop_sequence'])
                peak_hour = row.loc['peak_hour']
                # event = row.loc['event']
                hospital = row.loc['hospital']
                school = row.loc['school']
                passenger_in = row.loc['passengers']
                passenger_out = row.loc['passengers_out']

                sim_time = app_start + pd.to_timedelta(timestamp)
                weekend = 1 if sim_time.weekday() >= 5 else 0
                seconds = (sim_time - sim_time.replace(hour=0, minute=0, second=0)).total_seconds()
                
                # create dataframe to feed data into the model to output predictions
                # data_x = pd.DataFrame({
                #     'arrival_time': [seconds],
                #     # 'stop_id': [stop],
                #     'encoded_routes': [le.transform([str(route)])[0]],
                #     'weekend': [weekend],
                #     'peak_hour': [peak_hour]
                # })
                
                # Ensure the prediction is not negative
                # passenger_in = min(max(0, int(model.predict(data_x)[0])), 8)
                # passenger_out = max(max(0, int(model_out.predict(data_x)[0])), 2)

                # # --- Cap so we don't exceed the max for the trip ---
                # if running_in + passenger_in > max_passengers_per_trip:
                #     passenger_in = max_passengers_per_trip - running_in
                # if running_out + passenger_out > max_passengers_per_trip:
                #     passenger_out = max_passengers_per_trip - running_out

                # # Don't allow negative predictions after capping
                # passenger_in = max(0, passenger_in)
                # passenger_out = max(0, passenger_out)

                # running_in += passenger_in
                # running_out += passenger_out
                print(GOOGLE_API_KEY)

                if GOOGLE_API_KEY is not None:
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
                        # always get the last appended bus to the list as the currently active one for this specific trip
                        'bus_id': int(bus_active_list[-1]),
                        "weekend": weekend,
                        "peak_hour": peak_hour,
                        # "event": event,
                        "hospital": hospital,
                        "school": school
                    }
                
                else:
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
                        # always get the last appended bus to the list as the currently active one for this specific trip
                        'bus_id': int(bus_active_list[-1]),
                        "weekend": weekend,
                        "peak_hour": peak_hour,
                        # "event": event,
                        "hospital": hospital,
                        "school": school,
                        "traffic": row.loc['traffic_condition']
                    }

                print("Sending:", payload)
                producer.send('bus.passenger.predictions', value=payload)
                pred_id += 1
                time.sleep(float(SLEEP))

        app_start = app_start + timedelta(days=1) 

if __name__ == "__main__":
    get_passengers()