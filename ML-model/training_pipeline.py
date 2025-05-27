import dask.dataframe as dd
import pandas as pd
from sqlalchemy import create_engine, text
from typing import List
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import root_mean_squared_error
from utils.kafka_producer import create_kafka_producer
from utils.db_connect import create_db_connection
import time
import joblib
import json
import boto3
import io
from minio import Minio
import numpy as np
from dask.distributed import Client

# Initialize Dask client
client = Client("dask-scheduler:8786")

total_seats = 400
# --- Configuration ---
POSTGRES_URL = "postgresql+psycopg2://postgres:example@db:5432/raw_data"

# --- Load Data from PostgreSQL ---
# engine = create_engine(POSTGRES_URL)
engine = create_db_connection()

def perform_aggregations(
    selected_features: List[str] = [
        'trip_id_x', 'peak_hour', 'timestamp_x', 'seconds_from_midnight',
        'temperature', 'precipitation_probability', 'weather_code',
        'normal', 'traffic', 'traffic_level', 'event_dummy',
        'passengers_in', 'passengers_out', 'total_capacity', 'congestion_rate'
    ]
):

    # Read tables into Dask DataFrames
    tickets = dd.read_sql_table('raw_tickets', POSTGRES_URL, index_col='timestamp', npartitions=10)
    tickets = tickets.reset_index()
    sensors = dd.read_sql_table('raw_sensors', POSTGRES_URL, index_col='timestamp', npartitions=10)
    sensors = sensors.reset_index()
    trips = dd.read_sql_table('trips', POSTGRES_URL, index_col='route_id', npartitions=10)
    traffic = dd.read_sql_table('traffic', POSTGRES_URL, index_col='timestamp', npartitions=10)
    traffic = traffic.reset_index()
    weather = dd.read_sql_table('weather', POSTGRES_URL, index_col='measurement_id', npartitions=10)
    events = dd.read_sql_table('events', POSTGRES_URL, index_col='event_id', npartitions=10)

    # --- Aggregations (similar to MLmodels.ipynb) ---

    # Tickets: count passengers in per trip per day
    # Extract peak_hour per trip_id (assuming it's consistent per trip)
    peak_hours = tickets[['trip_id', 'peak_hour']].dropna().drop_duplicates(subset='trip_id')
    tickets['timestamp'] = dd.to_datetime(tickets['timestamp'])
    tickets['day'] = tickets['timestamp'].dt.date

    agg_tickets = tickets.groupby(['trip_id', 'day']).agg({
        'fare': 'count',
        'school': 'max',
        'hospital': 'max',
        'timestamp': 'max'
    }).rename(columns={'fare': 'passengers_in'}).reset_index()
    agg_tickets = dd.merge(agg_tickets, peak_hours, on='trip_id', how='left')

    # Sensors: count passengers out per trip per day
    sensors['timestamp'] = dd.to_datetime(sensors['timestamp'])
    sensors['day'] = sensors['timestamp'].dt.date
    agg_sensors = sensors.groupby(['trip_id', 'day']).agg({
        'status': 'count',
        'timestamp': 'max'
    }).rename(columns={'status': 'passengers_out'}).reset_index()

    # Merge tickets and sensors
    merged = dd.merge(agg_tickets, agg_sensors, on=['trip_id', 'day', 'timestamp'], how='inner')

    # Merge with trips
    merged = dd.merge(merged, trips.reset_index(), on='trip_id', how='left')

    # Merge with traffic
    traffic['timestamp'] = dd.to_datetime(traffic['timestamp'])
    merged = dd.merge(merged, traffic.reset_index(), on=['shape_id'], how='left')

    # Merge with weather (asof merge, so convert to pandas for this step)
    merged_pd = merged.compute()
    weather_pd = weather.compute()
    weather_pd['hour'] = pd.to_datetime(weather_pd['hour'])
    merged_pd['timestamp_x'] = pd.to_datetime(merged_pd['timestamp_x'])
    merged_pd = pd.merge_asof(
        merged_pd.sort_values('timestamp_x'),
        weather_pd[['hour', 'temperature', 'precipitation_probability', 'weather_code', 'latitude', 'longitude']].sort_values('hour'),
        left_on='timestamp_x', right_on='hour', direction='backward'
    )

    # Merge with events (by date)
    events_pd = events.compute()
    events_pd['day_event'] = pd.to_datetime(events_pd['day_event']).dt.date
    merged_pd['day'] = pd.to_datetime(merged_pd['day']).dt.date
    final = pd.merge(events_pd, merged_pd, left_on='day_event', right_on='day', how='right')
    final['event_dummy'] = final['day_event'].notna().astype(int)
    final['total_capacity'] = total_seats
    final['congestion_rate'] = (final['passengers_in'] - final['passengers_out']) / final['total_capacity']
    final['seconds_from_midnight'] = (
        final['timestamp_x'].dt.hour * 3600 +
        final['timestamp_x'].dt.minute * 60 +
        final['timestamp_x'].dt.second
    )
    final['weekend'] = (final['timestamp_x'].dt.dayofweek >= 5).astype(int)
    final['sine_time'] = np.sin(2 * np.pi * final['seconds_from_midnight'] / 86400)
    # print(final.columns)
    final = final[selected_features]
    # --- Save to PostgreSQL ---
    final_clean = final.dropna()
    final_clean = final_clean.drop_duplicates(['trip_id_x', 'timestamp_x'])

    return final_clean

def write_to_sql(final_clean):
    final_clean = final_clean.drop_duplicates(subset=['trip_id_x', 'timestamp_x'])

    engine = create_engine(POSTGRES_URL)
    existing_keys = pd.read_sql(
        "SELECT trip_id_x, timestamp_x FROM feature_table", engine
    )

    # Filter only new records
    new_data = final_clean.merge(
        existing_keys,
        on=["trip_id_x", "timestamp_x"],
        how="left",
        indicator=True
    )
    new_data = new_data[new_data["_merge"] == "left_only"].drop(columns=["_merge"])

    if not new_data.empty:
        final_dd = dd.from_pandas(new_data, npartitions=10)
        final_dd.to_sql('feature_table', POSTGRES_URL, if_exists='append', index=False)
        print(f"{len(new_data)} new rows inserted.")
    else:
        print("No new rows to insert.")

def enforce_unique():
    # Connect using SQLAlchemy engine
    with engine.connect() as conn:
        # Try to add the unique constraint on (trip_id_x, timestamp_x)
        try:
            conn.execute(text("""
                ALTER TABLE feature_table
                ADD CONSTRAINT unique_trip_timestamp
                UNIQUE (trip_id_x, timestamp_x)
            """))
            print("Unique constraint added.")
        except Exception as e:
            print("Could not add constraint:", e)

def split_data(final_clean, test_size: float=0.3, shuffle: bool=True, random_state = 42):
    X = final_clean.drop(['congestion_rate', 'trip_id_x', 'timestamp_x'], axis=1)
    mapping = {'no traffic/low': 0, 'medium': 1, 'heavy': 2, 'severe/standstill': 3}
    X['traffic_level'] = X['traffic_level'].map(mapping)
    y = final_clean['congestion_rate']

    # Drop any columns with all NaNs 
    X = X.dropna(axis=1)

    # Reset index to avoid index name issues
    X = X.reset_index(drop=True)

    # Convert all column names to strings explicitly (important)
    X.columns = [str(col).strip().strip('"').strip("'") for col in X.columns]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, shuffle=shuffle, random_state=random_state
    )
    print(X_train.head())

    return X_train, X_test, y_train, y_test


def train_rf_model(final_clean, best_params = None):

    if best_params is None:
        best_params = {
            'criterion': 'friedman_mse',
            'max_depth': 10,
            'max_features': 10,
            'min_samples_split': 10,
            'n_estimators': 100
            }

    rf = RandomForestRegressor(**best_params)

    X_train, X_test, y_train, y_test = split_data(final_clean, test_size=0.3, shuffle=True)

    rf.fit(X_train, y_train)

    return rf

def get_accuracy_model(final_clean, rf):

    X_train, X_test, y_train, y_test = split_data(final_clean, test_size=0.3, shuffle=True)
    # print(rf.score(X_test, y_test))
    preds = rf.predict(X_test)
    rmse = root_mean_squared_error(y_true=y_test, y_pred=preds)
    print(f"RMSE is: {rmse}")
    return rmse

if __name__ == "__main__":
    while True:  # Run indefinitely
        try:
            # Wait for services to be ready
            max_retries = 10
            retry_count = 0
            
            while retry_count < max_retries:
                try:
                    # Test database connection
                    engine = create_db_connection()
                    with engine as conn:
                        conn.execute(text("SELECT 1"))
                    
                    # Test MinIO connection
                    minio_client = Minio(
                        "minio:9000",
                        access_key="minioadmin",
                        secret_key="minioadmin",
                        secure=False
                    )
                    minio_client.list_buckets()
                    
                    # Test Kafka connection
                    producer = create_kafka_producer()
                    # producer.send("model.train.topic", value={"test": "connection"})
                    # producer.flush()
                    
                    print("All services are ready!")
                    break
                except Exception as e:
                    retry_count += 1
                    print(f"Service not ready (attempt {retry_count}/{max_retries}): {e}")
                    if retry_count == max_retries:
                        raise Exception("Failed to connect to required services")
                    time.sleep(5)

            # Run the training pipeline
            print("\nStarting new training cycle...")
            final_clean = perform_aggregations(selected_features=[
                'trip_id_x' ,'timestamp_x', 'peak_hour', 'seconds_from_midnight',
                'temperature', 'precipitation_probability', 'weather_code',
                'traffic_level', 'event_dummy', 'congestion_rate', 'school', 'hospital', 'weekend', 'sine_time'
            ])

            final_clean = final_clean.drop_duplicates(subset=['trip_id_x', 'timestamp_x'])
            write_to_sql(final_clean)
            rf = train_rf_model(final_clean)
            acc = get_accuracy_model(final_clean, rf)

            # Save model to MinIO
            minio_client = Minio(
                "minio:9000",
                access_key="minioadmin",
                secret_key="minioadmin",
                secure=False
            )

            bucket_name = "models"
            if not minio_client.bucket_exists(bucket_name):
                minio_client.make_bucket(bucket_name)
                print(f"Created bucket: {bucket_name}")
            else:
                print(f"Bucket '{bucket_name}' already exists.")

            timestamp = int(time.time())
            model_key = f"challengers/challenger_{timestamp}.pkl"
            model_buffer = io.BytesIO()
            joblib.dump(rf, model_buffer)
            model_buffer.seek(0)

            minio_client.put_object(
                "models", model_key, model_buffer, length=-1, part_size=10*1024*1024
            )

            # Send metadata to Kafka
            try:
                message = {
                    "model_key": model_key,
                    "accuracy": float(acc),
                    "timestamp": timestamp
                }
                print(f"Attempting to send message to Kafka: {message}")
                future = producer.send("model.train.topic", value=message)
                record_metadata = future.get(timeout=10)
                print(f"Message sent successfully to partition {record_metadata.partition} at offset {record_metadata.offset}")
                producer.flush()
                print(f"Producer flushed successfully")
            except Exception as e:
                print(f"Error sending to Kafka: {e}")
                raise
            finally:
                print("Closing producer...")
                producer.close()

            print(f"Model sent to Kafka: {model_key} (accuracy: {acc:.4f})")
            
            # Wait for 60 seconds before next cycle
            print("Waiting 60 seconds before next training cycle...")
            time.sleep(60)
            
        except Exception as e:
            print(f"Error in training cycle: {e}")
            print("Retrying in 60 seconds...")
            time.sleep(60)