import random
import requests
import json
import time
from kafka import KafkaConsumer, KafkaProducer
import os
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
from utils.db_connect import create_db_connection
from utils.kafka_producer import create_kafka_producer
from collections import deque
import redis
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_result

class RateLimitException(Exception):
    pass

def is_rate_limit_error(result):
    # If API response is returned but shows over-query
    if isinstance(result, dict) and result.get("status") == "OVER_QUERY_LIMIT":
        return True
    return False

def is_stale_or_limited_result(data):
    if not isinstance(data, dict):
        return False
    if data.get("status") == "OVER_QUERY_LIMIT":
        return True
    try:
        routes = data.get("routes", [])
        if routes:
            duration = routes[0]["legs"][0]["duration"]["value"]
            # Add a heuristic: if it's always ~same low value, consider it stale
            if duration < 120:  # less than 2 minutes
                print(f"[RETRY] Duration too low: {duration}")
                return True
    except Exception:
        return False
    return False

# Connect to Redis (adjust hostname as needed)
redis_client = redis.Redis(host='redis', port=6379, db=0)

def enforce_redis_rate_limit(max_requests_per_second=9, window_seconds=1):
    key = "api_request_timestamps"
    now = int(time.time() * 1000)  # current time in ms
    window_start = now - (window_seconds * 1000)

    # Remove old entries
    redis_client.zremrangebyscore(key, 0, window_start)

    # Count current entries in the window
    current_count = redis_client.zcard(key)
    if current_count >= max_requests_per_second:
        oldest = redis_client.zrange(key, 0, 0, withscores=True)[0][1]
        sleep_ms = (oldest + window_seconds * 1000) - now
        sleep_s = sleep_ms / 1000
        print(f"[RedisRateLimit] Sleeping for {sleep_s:.2f}s", flush=True)
        time.sleep(sleep_s)

    # Add current timestamp
    redis_client.zadd(key, {str(now): now})

load_dotenv()
recent_requests = deque()

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    retry=retry_if_result(is_stale_or_limited_result)
)
def fetch_with_retry_decorator(url):
    try:
        print(f"[API CALL] Requesting URL: {url}")
        response = requests.get(url, timeout=10)

        print(f"[API RESPONSE] Status code: {response.status_code}")

        if response.status_code == 429:
            print("[API RESPONSE] Rate limited with 429 status code")
            raise RateLimitException("HTTP 429: Rate limit hit")

        if response.status_code != 200:
            print(f"[API ERROR] Unexpected HTTP status: {response.status_code}")
            raise Exception(f"Unexpected HTTP code {response.status_code}")

        # Print a snippet of the response text for debugging
        print(f"[API RESPONSE] Body snippet: {response.text[:200]}")

        data = response.json()

        if data.get("status") == "OVER_QUERY_LIMIT":
            print("[API] OVER_QUERY_LIMIT from API response, triggering retry")
            return data  # triggers retry via is_stale_or_limited_result

        return data

    except RateLimitException:
        # To trigger tenacity retry on 429 errors
        raise
    except Exception as e:
        print(f"[Retry Exception] {e}")
        raise

SLEEP = float(os.getenv("SLEEP", "1"))
print("SLEEP is set to:", SLEEP, flush=True)
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", None)

def build_get_traffic(destinations: tuple, origins: tuple, key: str, departure_time: str ='now'):
    gmaps_endpoint = "https://maps.googleapis.com/maps/api/distancematrix/json?"
    return f"{gmaps_endpoint}destinations={destinations[0]}%2C{destinations[1]}&origins={origins[0]}%2C{origins[1]}&key={key}&departure_time={departure_time}"

def generate_traffic(msg, traffic_level, normal_time, traffic_time):
    measurement_id = f"{msg['stop_id']}-{msg['route']}-{msg['timestamp']}-{random.randint(1000, 9999)}"
    return {
        "measurement_id": str(measurement_id),
        "timestamp": msg['timestamp'],
        "stop_id": msg['stop_id'],
        "route": msg['route'],
        "bus_id": msg['bus_id'],
        "trip_id": msg['trip_id'],
        "traffic_level": traffic_level,
        "normal": normal_time,
        "traffic": traffic_time,
        "shape_id": msg['shape_id']
    }

def generate_traffic_no_api(msg):
    measurement_id = f"{msg['stop_id']}-{msg['route']}-{msg['timestamp']}-{random.randint(1000, 9999)}"
    return {
        "measurement_id": str(measurement_id),
        "timestamp": msg['timestamp'],
        "stop_id": msg['stop_id'],
        "route": msg['route'],
        "bus_id": msg['bus_id'],
        "trip_id": msg['trip_id'],
        "traffic_level": msg['traffic'],
        "normal": None,
        "traffic": None,
        "shape_id": msg['shape_id']
    }


def unique_shapes():
    shapes = []
    connection = create_db_connection()
    with connection:
        result = connection.execute(
            text("SELECT DISTINCT(shape_id) FROM shapes")
        )
    for row in result:
        shapes.append(row[0])
    return shapes

shapes_unique = unique_shapes()
producer = create_kafka_producer()
all_coordinates = []
tried_shapes = []

def process_passenger_predictions(key: str = GOOGLE_API_KEY):
    global all_coordinates
    global tried_shapes
    
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                'bus.passenger.predictions',
                bootstrap_servers='kafka:9092',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='traffic-producer-group'
            )
            print("Connected to Kafka topic: bus.passenger.predictions")
        except Exception as e:
            print(f"Kafka not ready, retrying in 3 seconds... ({e})")
            time.sleep(3)
    
    for message in consumer:
        try:
            msg = message.value
            shape_id = msg.get("shape_id")
            timestamp = msg.get('timestamp')
            if GOOGLE_API_KEY is not None:
                if not shape_id or not timestamp:
                    print("Missing shape_id or timestamp in message:", msg, flush=True)
                    continue

                try:
                    timestamp_converted = int(datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S").timestamp())
                except ValueError as ve:
                    print("Timestamp conversion error:", ve, "timestamp:", timestamp, flush=True)
                    continue

                if len(tried_shapes) == len(shapes_unique):
                    tried_shapes = []

                if shape_id in tried_shapes:
                    continue
                tried_shapes.append(shape_id)

                connection = create_db_connection()
                with connection:
                    result = connection.execute(
                        text("SELECT shape_pt_lat, shape_pt_lon FROM shapes WHERE shape_id = :shape_id"),
                        {"shape_id": shape_id}
                    )
                    rows = list(result)

                if not rows:
                    print(f"No coordinates found for shape_id: {shape_id}", flush=True)
                    continue

                all_coordinates = [(row[0], row[1]) for row in rows]

                if len(all_coordinates) < 2:
                    print(f"Not enough coordinates for shape_id: {shape_id}", flush=True)
                    continue

                origins = all_coordinates[0]
                destinations = all_coordinates[-1]

                req = build_get_traffic(destinations, origins, key, timestamp_converted)
                enforce_redis_rate_limit(9)
                api_response = fetch_with_retry_decorator(req)
                
                if api_response is None:
                    print("[API] No response data, skipping")
                    continue

                try:
                    element = api_response['rows'][0]['elements'][0]
                    normal = element['duration']['value']
                    traffic = element['duration_in_traffic']['value']
                except (IndexError, KeyError) as e:
                    print("Error extracting traffic data:", e, flush=True)
                    print("API response was:", json.dumps(api_response, indent=2), flush=True)
                    continue

                time_traffic = (traffic - normal) / normal * 100

                if time_traffic <= 10:
                    traffic_level = 'no traffic/low'
                elif time_traffic <= 30:
                    traffic_level = 'medium'
                elif time_traffic < 100:
                    traffic_level = 'heavy'
                else:
                    traffic_level = 'severe/standstill'

                traffic_msg = generate_traffic(msg, traffic_level, normal, traffic)
                if traffic_msg:
                    print(f"origins: {origins}, destinations: {destinations}")
                    print("Sending traffic data:", traffic_msg, flush=True)
                    producer.send("traffic.topic", value=traffic_msg)

                all_coordinates = []

            else:
                shape_id = msg.get("shape_id")
                if shape_id in tried_shapes:
                    continue
                tried_shapes.append(shape_id)
                traffic_msg = generate_traffic_no_api(msg)
                if traffic_msg:
                    # print(f"origins: {origins}, destinations: {destinations}")
                    print("Sending traffic data:", traffic_msg, flush=True)
                    producer.send("traffic.topic", value=traffic_msg)

                # all_coordinates = []


        except Exception as e:
            print("Error processing message:", e, flush=True)

            time.sleep(SLEEP)

if __name__ == "__main__":
    process_passenger_predictions()