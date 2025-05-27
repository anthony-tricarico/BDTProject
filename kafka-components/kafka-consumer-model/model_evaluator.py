import io
import time
import json
from kafka import KafkaConsumer
from minio import Minio
from minio.commonconfig import CopySource

print("Starting model evaluator...")

# --- Initialize MinIO client with retry ---
minio_client = None
while minio_client is None:
    try:
        minio_client = Minio(
            "minio:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )
        # Test connection
        minio_client.list_buckets()
        print("Successfully connected to MinIO")
    except Exception as e:
        print(f"MinIO connection error: {e} -- retrying in 3 seconds")
        time.sleep(3)

# Ensure models bucket exists
if not minio_client.bucket_exists("models"):
    minio_client.make_bucket("models")
    print("Created 'models' bucket")

# --- Function to get current champion's accuracy ---
def get_champion_accuracy():
    try:
        # Metadata file saved alongside the champion model
        response = minio_client.get_object("models", "champion/latest_rf_accuracy.json")
        metadata = json.load(io.BytesIO(response.read()))
        return metadata.get("accuracy", float("-inf"))
    except Exception as e:
        print(f"Could not load champion accuracy: {e}")
        # Initialize champion if it doesn't exist
        if "NoSuchKey" in str(e):
            print("Initializing first champion model...")
            accuracy_bytes = io.BytesIO(json.dumps({"accuracy": float("-inf")}).encode("utf-8"))
            minio_client.put_object(
                "models",
                "champion/latest_rf_accuracy.json",
                accuracy_bytes,
                length=accuracy_bytes.getbuffer().nbytes,
                content_type="application/json"
            )
        return float("inf")

# --- Promote challenger to champion ---
def promote_model(challenger_key, accuracy):
    try:
        source = CopySource("models", challenger_key)

        # Copy the model
        minio_client.copy_object("models", "champion/latest_rf.pkl", source)

        # Store the accuracy as metadata
        accuracy_bytes = io.BytesIO(json.dumps({"accuracy": accuracy}).encode("utf-8"))
        minio_client.put_object(
            "models",
            "champion/latest_rf_accuracy.json",
            accuracy_bytes,
            length=accuracy_bytes.getbuffer().nbytes,
            content_type="application/json"
        )

        print(f"Promoted model {challenger_key} to champion!")
    except Exception as e:
        print(f"Error promoting model: {e}")

# --- Connect to Kafka with Retry ---
print("Attempting to connect to Kafka...")
consumer = None
while consumer is None:
    try:
        consumer = KafkaConsumer(
            "model.train.topic",
            bootstrap_servers="kafka:9092",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            group_id="model-evaluator"
        )
        print("Successfully connected to Kafka")
    except Exception as e:
        print(f"Kafka connection error: {e} -- retrying in 3 seconds")
        time.sleep(3)

# --- Listen for New Models ---
print("Waiting for models...")

try:
    while True:  # Keep running indefinitely
        for msg in consumer:
            print(f"Received message: {msg.value}")  # Debug log
            try:
                challenger_key = msg.value["model_key"]
                challenger_acc = msg.value["accuracy"]

                champion_acc = get_champion_accuracy()
                print(f"Champion accuracy: {champion_acc:.4f}, Challenger: {challenger_acc:.4f}")

                if challenger_acc < champion_acc:
                    print("New champion found!")
                    promote_model(challenger_key, challenger_acc)
                else:
                    print("Challenger rejected.")
            except Exception as e:
                print(f"Error processing message: {e}")
except KeyboardInterrupt:
    print("Shutting down gracefully...")
except Exception as e:
    print(f"Error in consumer loop: {e}")
finally:
    print("Closing consumer...")
    consumer.close()
