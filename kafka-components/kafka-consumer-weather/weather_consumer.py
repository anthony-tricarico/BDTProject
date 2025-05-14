from kafka import KafkaConsumer
import psycopg2
import json
from datetime import datetime
import time
# Kafka consumer setup
consumer = None
while consumer is None:
    try:
        consumer = KafkaConsumer(
            'weather.topic',
            bootstrap_servers='kafka:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
    except Exception as e:
        print(f"Kafka not ready, retrying in 3 seconds... ({e})")
        time.sleep(3)

# PostgreSQL connection setup
conn = psycopg2.connect(
    dbname='raw_data',
    user='postgres',
    password='example',
    host='db'
)
cur = conn.cursor()

# Function to update weather table
def update_weather_table(weather_data):
    timestamp = weather_data['timestamp']
    latitude = weather_data['latitude']
    longitude = weather_data['longitude']
    hours = weather_data['hour']
    temperatures = weather_data['temperature']
    precipitation_probabilities = weather_data['precipitation_probability']
    weather_codes = weather_data['weather_code']

    # Clear the table before inserting new data
    cur.execute("TRUNCATE TABLE weather")

    # Iterate over the arrays and insert each element as a separate row
    for hour, temperature, precipitation_probability, weather_code in zip(hours, temperatures, precipitation_probabilities, weather_codes):
        cur.execute("""
            INSERT INTO weather (timestamp, latitude, longitude, weather_code, precipitation_probability, temperature, hour)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            timestamp,
            latitude,
            longitude,
            weather_code,
            precipitation_probability,
            temperature,
            hour
        ))
    conn.commit()

# Consume messages and update the table
for message in consumer:
    weather_data = message.value
    update_weather_table(weather_data)

# Close connections
cur.close()
conn.close()