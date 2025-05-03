from fastapi import FastAPI
from kafka import KafkaConsumer
import threading
import json
from collections import deque

app = FastAPI()

# FIFO memory buffer to store the last N messages
MAX_MESSAGES = 100
message_store = deque(maxlen=MAX_MESSAGES)

# Kafka consumer setup
def consume():
    print("Starting Kafka consumer thread...")
    try:
        consumer = KafkaConsumer(
            "bus.passenger.predictions",
            bootstrap_servers="kafka:9092",
            auto_offset_reset="earliest",
            group_id="passenger-consumer-group"
        )
        print("Kafka consumer connected.")
    except Exception as e:
        print(f"Kafka connection failed: {e}")
        return

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
@app.get("/latest")
def latest_message():
    return message_store[-1] if message_store else {"message": "No data yet"}

# Expose all recent messages
@app.get("/stream")
def all_messages():
    return list(message_store)
