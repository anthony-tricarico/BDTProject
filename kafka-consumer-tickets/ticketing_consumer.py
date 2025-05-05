from fastapi import FastAPI
from kafka import KafkaConsumer
import threading
import json
from collections import deque, defaultdict
import time
from datetime import datetime, timedelta

app = FastAPI()

MAX_MESSAGES = 100
message_store = deque(maxlen=MAX_MESSAGES)

def consume():
    print("Starting Kafka consumer thread...")

    # Retry loop for Kafka connection
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                "ticketing.topic",
                bootstrap_servers="kafka:9092",
                # value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset="earliest",
                group_id="ticketing-consumer-group"
            )
            print("Kafka consumer connected.")
        except Exception as e:
            print(f"Kafka not ready, retrying in 3 seconds... ({e})")
            time.sleep(3)

    # Process messages once connected
    for msg in consumer:
        try:
            decoded = json.loads(msg.value.decode("utf-8"))
            print("Received:", decoded)
            message_store.append(decoded)
        except Exception as e:
            print(f"Error decoding message: {e}")

# Run Kafka consumer in a background thread
threading.Thread(target=consume, daemon=True).start()

# Expose the latest message
@app.get("/latest_tickets")
def latest_message():
    return message_store[-1] if message_store else {"message": "No data yet"}

# Expose all recent messages
@app.get("/stream_tickets")
def all_messages():
    return list(message_store)