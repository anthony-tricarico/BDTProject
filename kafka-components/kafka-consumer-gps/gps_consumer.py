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
            'gps.topic',
            bootstrap_servers='kafka:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        print("Kafka consumer connected.")
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

# Create gps table if it doesn't exist
try:
    cur.execute("""
    CREATE TABLE IF NOT EXISTS gps (
        measurement_id VARCHAR PRIMARY KEY,
        timestamp TIMESTAMP,
        stop_id VARCHAR,
        route VARCHAR,
        bus_id INTEGER,
        trip_id VARCHAR,
        shape_id VARCHAR,
        sequence INTEGER,
        latitude FLOAT,
        longitude FLOAT
    )
    """)
    conn.commit()
    print("GPS table created or already exists.")
except Exception as e:
    print(f"Error creating GPS table: {e}")

# Function to update GPS table
def update_gps_table(gps_data):
    try:
        # Insert GPS data
        cur.execute("""
            INSERT INTO gps 
            (measurement_id, timestamp, stop_id, route, bus_id, trip_id, shape_id, sequence, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (measurement_id) DO NOTHING
        """, (
            gps_data['measurement_id'],
            gps_data['timestamp'],
            gps_data['stop_id'],
            gps_data['route'],
            gps_data['bus_id'],
            gps_data['trip_id'],
            gps_data['shape_id'],
            gps_data['sequence'],
            gps_data['latitude'],
            gps_data['longitude']
        ))
        conn.commit()
        print(f"GPS data inserted: {gps_data['measurement_id']}")
    except Exception as e:
        print(f"Error updating GPS table: {e}")
        conn.rollback()

# Consume messages and update the table
try:
    print("Starting to consume GPS messages...")
    for message in consumer:
        gps_data = message.value
        update_gps_table(gps_data)
except KeyboardInterrupt:
    print("Consumer stopped by user")
except Exception as e:
    print(f"Unexpected error: {e}")
finally:
    # Close connections
    cur.close()
    conn.close()
    print("Connections closed") 