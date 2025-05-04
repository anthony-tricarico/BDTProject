from pickle import load
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta

# import trained tree model
with open("generate-passengers/treemodel.pkl", "rb") as f:
    model = load(f)

# import label encoder
with open("generate-passengers/labelencoder.pkl", "rb") as f:
    le = load(f)

# Constants
TIME_MULTIPLIER = 300  # 5 minutes (300s) of app time = 1s real time

# Starting time of the simulation
real_start = time.time()
# starting time of the app
app_start = datetime(2025, 1, 1, 6, 0, 0)  # Let's say buses start at 6:00 AM

def get_simulated_time():
    # measure real elapsed time
    elapsed_real = time.time() - real_start
    # specifies how many seconds should be added to the simulation time
    elapsed_simulated = timedelta(seconds=elapsed_real * TIME_MULTIPLIER)
    # returns the new time augmented by the multiplier
    return app_start + elapsed_simulated

# Simulation loop
def get_passengers(stop, route):
    while True:
        sim_time = get_simulated_time()

        seconds = (sim_time - sim_time.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()

        data_x = pd.DataFrame({'arrival_time' : [seconds],
                            'stop_id' : [stop],
                            'encoded_routes' : [le.transform(pd.Series(route))]})

        # round and convert prediction from model to int to have passenger number
        passenger_num = int(model.predict(data_x)[0])
        print(sim_time)
        print(passenger_num)

        time.sleep(1)  # Every 2 seconds == 5 minutes in the app

print(get_passengers(2278, '5'))