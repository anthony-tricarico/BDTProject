# Inside producer/app.py
from kafka import KafkaProducer
import json
import time
from datetime import datetime, timedelta
import pandas as pd
from pickle import load

# Load your model and encoder (ensure files are included in Docker image)
with open("treemodel.pkl", "rb") as f:
    model = load(f)

with open("labelencoder.pkl", "rb") as f:
    le = load(f)

with open("treemodel_out.pkl", "rb") as f:
    model_out = load(f)

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

# Use it in your main code
producer = create_kafka_producer()

TIME_MULTIPLIER = 10e3 # about every 15 min considering a delay in the function call of 0.1 seconds

app_start = datetime(2025, 1, 1, 6, 0, 0)

def get_simulated_time(real_start = time.time()):
    elapsed_real = time.time() - real_start
    elapsed_simulated = timedelta(seconds=elapsed_real * TIME_MULTIPLIER)
    return app_start + elapsed_simulated

pred_id = 0
def get_passengers(stop, route):
    global pred_id
    real_start = time.time()
    while True:
        sim_time = get_simulated_time(real_start=real_start)
        seconds = (sim_time - sim_time.replace(hour=0, minute=0, second=0)).total_seconds()
        data_x = pd.DataFrame({
            'arrival_time': [seconds],
            'stop_id': [stop],
            'encoded_routes': [le.transform([route])[0]]
        })

        hour_stop = 2 # the time when bus stops service
        hour_start = 6 # the time when bus starts service

        if seconds > 3600 * hour_stop and seconds < 3600 * hour_start:
            break # passenger_num = 0
        else:
            passenger_in = int(model.predict(data_x)[0])
            passenger_out = int(model_out.predict(data_x)[0])

        payload = {
            'prediction_id': pred_id,
            'timestamp': sim_time.isoformat(),
            'stop_id': stop,
            'route': route,
            'predicted_passengers_in': passenger_in,
            'predicted_passengers_out': passenger_out,
        }
        print("Sending:", payload)
        producer.send('bus.passenger.predictions', value=payload)
        pred_id += 1
        time.sleep(0.1)

with open("route_to_stops.json", "r") as f:
    route_stop_map = json.load(f)

def main():
    for route in route_stop_map:
        for stop in route_stop_map[route]['stops']:
            get_passengers(stop, route)

if __name__ == "__main__":
    main()