import boto3
import joblib
import os
import io
from kafka import KafkaConsumer
import json
from sklearn.metrics import r2_score
from utils.kafka_consumer import create_kafka_consumer
import time
from minio import Minio
from minio.commonconfig import CopySource

# --- Initialize MinIO client ---
minio_client = Minio(
    "minio:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# --- Function Definitions ---
def download_model(key):
    response = minio_client.get_object("models", key)
    return joblib.load(io.BytesIO(response.read()))

def get_champion_accuracy():
    try:
        model = download_model("champion/latest_rf.pkl")
        x_obj = minio_client.get_object("models", "evaluation/X_test.pkl")
        y_obj = minio_client.get_object("models", "evaluation/y_test.pkl")
        X_test = joblib.load(io.BytesIO(x_obj.read()))
        y_test = joblib.load(io.BytesIO(y_obj.read()))
        return model.score(X_test, y_test)
    except Exception as e:
        print(f"Could not load champion model or evaluation set: {e}")
        return float("-inf")

def promote_model(challenger_key):
    source = CopySource("models", challenger_key)
    minio_client.copy_object("models", "champion/latest_rf.pkl", source)
    print(f"Promoted model {challenger_key} to champion!")

# --- Connect to Kafka with Retry ---
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
    except Exception as e:
        print(f"Kafka connection error: {e} -- retrying in 3 seconds")
        time.sleep(3)

# --- Listen for New Models ---
print("Waiting for models...")

for msg in consumer:
    challenger_key = msg.value["model_key"]
    challenger_acc = msg.value["accuracy"]

    champion_acc = get_champion_accuracy()
    print(f"Champion accuracy: {champion_acc:.4f}, Challenger: {challenger_acc:.4f}")

    if challenger_acc > champion_acc:
        print("New champion found!")
        promote_model(challenger_key)
    else:
        print("Challenger rejected.")
