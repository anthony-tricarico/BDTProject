from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import time

# MongoDB setup
client = MongoClient("mongodb://mongodb:27017")
db = client["raw"]

# Topics to listen to
TOPIC_COLLECTION_MAP = {
    "sensors.topic": "sensors",
    "ticketing.topic": "tickets",
    "bus.passenger.predictions": "passengers"
}
def add_to_mongodb():
    print("Starting Kafka consumer thread...")

    # Retry loop for Kafka connection
    consumer = None
    while consumer is None:
        try:
            # Create Kafka consumer
            consumer = KafkaConsumer(
                *TOPIC_COLLECTION_MAP.keys(),  # Subscribes to all topics in the map
                bootstrap_servers='kafka:9092',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='multi-consumer-group'
            )
            print("Kafka consumer connected.")
        except Exception as e:
            print(f"Kafka not ready, retrying in 3 seconds... ({e})")
            time.sleep(3)

    while consumer:
    # Consume and store
        for message in consumer:
            topic = message.topic
            data = message.value

            if topic in TOPIC_COLLECTION_MAP:
                collection_name = TOPIC_COLLECTION_MAP[topic]
                collection = db[collection_name]

                # Choose unique key field per topic
                if topic == "sensors.topic":
                    unique_field = "measurement_id"
                elif topic == "ticketing.topic":
                    unique_field = "ticket_id"
                elif topic == "bus.passenger.predictions":
                    unique_field = "prediction_id"
                else:
                    continue

                # Upsert by unique ID
                if unique_field in data:
                    collection.update_one(
                        {unique_field: data[unique_field]},
                        {"$set": data},
                        upsert=True
                    )

if __name__ == "__main__":
    add_to_mongodb()