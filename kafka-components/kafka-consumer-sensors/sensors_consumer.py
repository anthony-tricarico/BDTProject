from fastapi import FastAPI
from kafka import KafkaConsumer
import threading
import json
from collections import deque, defaultdict
import time
from datetime import datetime, timedelta
from utils.kafka_consumer import create_kafka_consumer

app = FastAPI()

MAX_MESSAGES = 100
message_store = deque(maxlen=MAX_MESSAGES)

def consume():
    print("Starting Kafka consumer thread...")

    consumer = create_kafka_consumer(topic="sensors.topic", group_id="sensors-consumer-group")

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
@app.get("/latest_sensors")
def latest_message():
    return message_store[-1] if message_store else {"message": "No data yet"}

# Expose all recent messages
@app.get("/stream_sensors")
def all_messages():
    return list(message_store)