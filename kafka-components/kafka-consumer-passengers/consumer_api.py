from fastapi import FastAPI, Query
from kafka import KafkaConsumer
import threading
import json
from collections import deque, defaultdict
import time
from datetime import datetime, timedelta
from utils.time_utils import parse_time
from utils.kafka_consumer import create_kafka_consumer

app = FastAPI()

# FIFO memory buffer to store the last N messages
MAX_MESSAGES = 100
message_store = deque(maxlen=MAX_MESSAGES)

def consume():
    print("Starting Kafka consumer thread...")

    consumer = create_kafka_consumer(topic="bus.passenger.predictions", group_id="passenger-consumer-group")

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
@app.get("/latest")
def latest_message():
    return message_store[-1] if message_store else {"message": "No data yet"}

# Expose all recent messages
@app.get("/stream")
def all_messages():
    return list(message_store)

@app.get("/filter-by-route")
def filter_by_route(route: str = Query(..., description="Route to filter messages by")):
    filtered = [msg for msg in message_store if msg.get("route") == route]
    return filtered if filtered else {"message": f"No data found for route '{route}'"}

@app.get("/filter-by-time-range")
def filter_by_time_range(
    start_time: str = Query(..., description="Start time in ISO 8601 format (e.g. '2025-05-04T10:00:00Z')"),
    end_time: str = Query(..., description="End time in ISO 8601 format (e.g. '2025-05-04T12:00:00Z')")
):
    try:
        # Parse the start and end time
        start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
    except ValueError:
        return {"error": "Invalid date format. Use ISO 8601 format (e.g., '2025-05-04T10:00:00Z')"}
    
    # Filter messages within the time range
    filtered_msgs = [
        msg for msg in message_store
        if "timestamp" in msg and parse_time(msg["timestamp"]) >= start_dt and parse_time(msg["timestamp"]) <= end_dt
    ]
    
    return filtered_msgs if filtered_msgs else {"message": f"No data in the range {start_time} to {end_time}"}