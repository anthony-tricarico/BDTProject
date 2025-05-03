# Inside producer/app.py
from kafka import KafkaProducer
import json
import time
from datetime import datetime, timedelta
import pandas as pd
from pickle import load

# ensures kafka services start up correctly before sending messages
time.sleep(5)

# Load your model and encoder (ensure files are included in Docker image)
with open("treemodel.pkl", "rb") as f:
    model = load(f)

with open("labelencoder.pkl", "rb") as f:
    le = load(f)

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TIME_MULTIPLIER = 300
real_start = time.time()
app_start = datetime(2025, 1, 1, 6, 0, 0)

def get_simulated_time():
    elapsed_real = time.time() - real_start
    elapsed_simulated = timedelta(seconds=elapsed_real * TIME_MULTIPLIER)
    return app_start + elapsed_simulated

def get_passengers(stop, route):
    while True:
        sim_time = get_simulated_time()
        seconds = (sim_time - sim_time.replace(hour=0, minute=0, second=0)).total_seconds()
        data_x = pd.DataFrame({
            'arrival_time': [seconds],
            'stop_id': [stop],
            'encoded_routes': [le.transform([route])[0]]
        })

        passenger_num = int(model.predict(data_x)[0])
        payload = {
            'timestamp': sim_time.isoformat(),
            'stop_id': stop,
            'route': route,
            'predicted_passengers': passenger_num
        }
        print("Sending:", payload)
        producer.send('bus.passenger.predictions', value=payload)
        time.sleep(2)

get_passengers(2278, '5')
