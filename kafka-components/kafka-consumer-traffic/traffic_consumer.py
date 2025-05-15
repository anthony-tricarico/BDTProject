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
            'traffic.topic',
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

# Create traffic table if it doesn't exist
try:
    cur.execute("""
    CREATE TABLE IF NOT EXISTS traffic (
        measurement_id VARCHAR PRIMARY KEY,
        timestamp TIMESTAMP,
        stop_id VARCHAR,
        route VARCHAR,
        bus_id INTEGER,
        trip_id VARCHAR,
        traffic_level VARCHAR,
        normal INTEGER,
        traffic INTEGER
    )
    """)
    conn.commit()
    print("Traffic table created or already exists.")
except Exception as e:
    print(f"Error creating traffic table: {e}")

# Function to update traffic table
def update_traffic_table(traffic_data):
    try:
        # Insert traffic data
        cur.execute("""
            INSERT INTO traffic 
            (measurement_id, timestamp, stop_id, route, bus_id, trip_id, traffic_level, normal, traffic)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (measurement_id) DO NOTHING
        """, (
            traffic_data['measurement_id'],
            traffic_data['timestamp'],
            traffic_data['stop_id'],
            traffic_data['route'],
            traffic_data['bus_id'],
            traffic_data['trip_id'],
            traffic_data['traffic_level'],
            traffic_data['normal'],
            traffic_data['traffic']
        ))
        conn.commit()
        print(f"Traffic data inserted: {traffic_data['measurement_id']}")
    except Exception as e:
        print(f"Error updating traffic table: {e}")
        conn.rollback()

# Consume messages and update the table
try:
    print("Starting to consume traffic messages...")
    for message in consumer:
        traffic_data = message.value
        update_traffic_table(traffic_data)
except KeyboardInterrupt:
    print("Consumer stopped by user")
except Exception as e:
    print(f"Unexpected error: {e}")
finally:
    # Close connections
    cur.close()
    conn.close()
    print("Connections closed") 