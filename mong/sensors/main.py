from kafka import KafkaConsumer
from pymongo import MongoClient, UpdateOne
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
        messages = consumer.poll(timeout_ms=1000, max_records=100)

        for topic_partition, msgs in messages.items():
            topic = topic_partition.topic
            collection_name = TOPIC_COLLECTION_MAP.get(topic)
            if not collection_name:
                continue
            collection = db[collection_name]

            # Choose unique key per topic
            if topic == "sensors.topic":
                unique_field = "measurement_id"
            elif topic == "ticketing.topic":
                unique_field = "ticket_id"
            elif topic == "bus.passenger.predictions":
                unique_field = "prediction_id"
            else:
                continue

            operations = []
            for message in msgs:
                data = message.value
                if unique_field in data:
                    operations.append(
                        UpdateOne(
                            {unique_field: data[unique_field]},
                            {"$set": data},
                            upsert=True
                        )
                    )

            if operations:
                collection.bulk_write(operations, ordered=False)

if __name__ == "__main__":
    add_to_mongodb()