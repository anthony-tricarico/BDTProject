import requests
import json
import time
from kafka import KafkaProducer
import os
from datetime import datetime, timedelta

SLEEP = float(os.getenv("SLEEP", "1"))
print("SLEEP is set to:", SLEEP, flush=True)

def parse_time(ts):
    # Adjust based on your data format
    if isinstance(ts, str):
        return datetime.fromisoformat(ts)
    return datetime.fromtimestamp(ts)

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

weather_id = 1
def generate_weather(weather_data: dict, timestamp) -> dict:
    global weather_id
    """
    variable timestamp to get as input a changing timestamp
    """

    weather_id += 1

    return {
        "measurement_id": str(weather_id),
        "timestamp": timestamp,
        "latitude": weather_data['latitude'],
        "longitude": weather_data['longitude'],
        "weather_code": weather_data['hourly']['weather_code'],
        "precipitation_probability": weather_data['hourly']['precipitation_probability'],
        "temperature": weather_data['hourly']['temperature_2m'],
        "hour": weather_data['hourly']['time']
    }

producer = create_kafka_producer()
WEATHER_REQUEST = 'https://api.open-meteo.com/v1/forecast?latitude=46.0679&longitude=11.1211&hourly=temperature_2m,precipitation_probability,rain,showers,snowfall,snow_depth,weather_code&forecast_days=1'
MAX_TIMESTAMP = 60*60*1 # every 6 hours
def poll_stream_and_generate_weather():
    global engine
    global MAX_TIMESTAMP
    global current_timestamp
    global WEATHER_REQUEST
    current_timestamp = datetime(2025,1,1,0,0,0)
    while True:
        try:
            response = requests.get("http://kafka-consumer-passengers:8000/stream")
            if response.status_code == 200:
                messages = response.json()

                for msg in messages:
                    # get time from message
                    timestamp = parse_time(msg['timestamp'])                    
                    print(timestamp) 
                    # max update_time
                    update_time = current_timestamp + timedelta(seconds=MAX_TIMESTAMP)

                    # timestamp from message in UNIX seconds
                    unix_seconds = timestamp.timestamp()
                    if unix_seconds > update_time.timestamp():
                        # save time as current_timestamp
                        current_timestamp = timestamp
                        
                        weather_response = requests.get(WEATHER_REQUEST)
                        if weather_response.status_code == 200:
                            weather_data = weather_response.json()
                        weather_dict = generate_weather(weather_data=weather_data, timestamp=str(timestamp))
                        if weather_dict is not None:
                            print("Sending sensor:", weather_dict)
                            producer.send("weather.topic", value=weather_dict)

        except Exception as e:
            print("Error:", e)

        time.sleep(float(SLEEP))


if __name__ == "__main__":
    poll_stream_and_generate_weather()