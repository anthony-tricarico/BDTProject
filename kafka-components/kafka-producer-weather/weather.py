import requests
import json
import time
import os
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from utils.time_utils import parse_time
from utils.kafka_producer import create_kafka_producer

SLEEP = float(os.getenv("SLEEP", "1"))
print("SLEEP is set to:", SLEEP, flush=True)


weather_id = 0
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

WEATHER_REQUEST = 'https://api.open-meteo.com/v1/forecast?latitude=46.0679&longitude=11.1211&hourly=temperature_2m,precipitation_probability,rain,showers,snowfall,snow_depth,weather_code&forecast_days=7'

MAX_TIMESTAMP = 60*60*6 # every 6 hours make new request to open-meteo API
def process_passenger_predictions():
    global MAX_TIMESTAMP
    global current_timestamp
    global WEATHER_REQUEST
    
    current_timestamp = datetime(2025,1,1,0,0,0)
    
    # Create Kafka consumer for passenger predictions
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                'bus.passenger.predictions',
                bootstrap_servers='kafka:9092',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='weather-producer-group'
            )
            print("Connected to Kafka topic: bus.passenger.predictions")
        except Exception as e:
            print(f"Kafka not ready, retrying in 3 seconds... ({e})")
            time.sleep(3)
    
    # Process messages from Kafka
    for message in consumer:
        try:
            msg = message.value
            
            # get time from message
            timestamp = parse_time(msg['timestamp'])                    
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
                        print("Sending weather data:", weather_dict)
                        producer.send("weather.topic", value=weather_dict)
                else:
                    print(f"Error fetching weather data: {weather_response.status_code}")

        except Exception as e:
            print("Error processing message:", e)

        time.sleep(float(SLEEP))


if __name__ == "__main__":
    process_passenger_predictions()