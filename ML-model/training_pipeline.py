import dask.dataframe as dd
import pandas as pd
from sqlalchemy import create_engine, text
from typing import List
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.tree import DecisionTreeRegressor
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
from dask.distributed import Client, LocalCluster
import xgboost as xgb
import mlflow
import mlflow.xgboost
from mlflow.models import infer_signature
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import multiprocessing
from functools import partial

# Database configuration
POSTGRES_URL = "postgresql://postgres:example@db:5432/raw_data"
POSTGRES_HOST = "db"
POSTGRES_PORT = "5432"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "example"
POSTGRES_DB = "raw_data"

# MLflow database configuration
MLFLOW_TRACKING_URI = "http://mlflow:5001"
MLFLOW_DB_URL = "postgresql://postgres:example@mlflow-db:5432/mlflow"

# Configure MLflow to disable Git tracking and silence warnings
os.environ['MLFLOW_DISABLE_GIT'] = 'true'
os.environ['GIT_PYTHON_REFRESH'] = 'quiet'

# Number of seats on each bus
total_seats = 400

def initialize_dask_client():
    """Initialize Dask client with proper error handling"""
    try:
        # Try to connect to existing scheduler
        client = Client("dask-scheduler:8786", timeout=30)
        print("Connected to existing Dask scheduler")
        return client
    except Exception as e:
        print(f"Could not connect to Dask scheduler: {e}")
        print("Creating local Dask cluster...")
        # Create a local cluster with explicit multiprocessing configuration
        cluster = LocalCluster(
            n_workers=2,
            threads_per_worker=2,
            processes=True,
            memory_limit='4GB'
        )
        client = Client(cluster)
        print("Local Dask cluster initialized")
        return client

def main():
    # Set multiprocessing start method
    if os.name != 'nt':  # Not Windows
        multiprocessing.set_start_method('fork', force=True)
    else:
        multiprocessing.set_start_method('spawn', force=True)

    # Initialize Dask client
    client = initialize_dask_client()
    
    # Run the training pipeline
    result = run_training_pipeline()
    return result

def ensure_mlflow_database_exists():
    pass

def setup_mlflow():
    """Consolidated MLflow setup function"""
    try:
        # Set tracking URI to the MLflow server
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        print(f"[MLflow] Tracking URI set to: {mlflow.get_tracking_uri()}")
        
        # Create experiment if it doesn't exist
        experiment_name = "bus_congestion_prediction"
        experiment = mlflow.get_experiment_by_name(experiment_name)
        
        if experiment is None:
            experiment_id = mlflow.create_experiment(
                experiment_name,
                artifact_location="/mlflow/artifacts"  
            )
            print(f"[MLflow] Created new experiment '{experiment_name}' with ID {experiment_id}")
        else:
            experiment_id = experiment.experiment_id
            print(f"[MLflow] Using existing experiment '{experiment_name}' with ID {experiment_id}")
        
        mlflow.set_experiment(experiment_name)
        
        # Verify setup
        current_experiment = mlflow.get_experiment_by_name(experiment_name)
        if current_experiment is None:
            raise Exception(f"Failed to set up experiment {experiment_name}")
        
        print(f"[MLflow] Setup complete. Using experiment: {current_experiment.name}")
        print(f"[MLflow] Artifact location: {current_experiment.artifact_location}")
        
        return experiment_name
    except Exception as e:
        print(f"[MLflow] Error in setup: {e}")
        raise

# Initialize MLflow tracking
mlflow.set_tracking_uri(MLFLOW_DB_URL)
print(f"[MLflow] Tracking URI: {mlflow.get_tracking_uri()}")

# Call the setup function to initialize MLflow
EXPERIMENT_NAME = setup_mlflow()

# --- Configuration ---
POSTGRES_URL = "postgresql+psycopg2://postgres:example@db:5432/raw_data"

# --- Load Data from PostgreSQL ---
engine = create_db_connection()

def perform_aggregations(
    selected_features: List[str] = [
        'trip_id_x', 'peak_hour', 'timestamp_x', 'seconds_from_midnight',
        'temperature', 'precipitation_probability', 'weather_code',
        'normal', 'traffic', 'traffic_level', 'event_dummy',
        'passengers_in', 'passengers_out', 'total_capacity', 'congestion_rate'
    ]
):
    try:
        # Read tables into Dask DataFrames
        print("Reading tickets table...")
        tickets = dd.read_sql_table('raw_tickets', POSTGRES_URL, index_col='timestamp', npartitions=10)
        tickets = tickets.reset_index()
        
        print("Reading sensors table...")
        sensors = dd.read_sql_table('raw_sensors', POSTGRES_URL, index_col='timestamp', npartitions=10)
        sensors = sensors.reset_index()
        
        print("Reading trips table...")
        trips = dd.read_sql_table('trips', POSTGRES_URL, index_col='route_id', npartitions=10)
        
        print("Reading traffic table...")
        traffic = dd.read_sql_table('traffic', POSTGRES_URL, index_col='timestamp', npartitions=10)
        traffic = traffic.reset_index()
        
        print("Reading weather table...")
        weather = dd.read_sql_table('weather', POSTGRES_URL, index_col='measurement_id', npartitions=10)
        
        print("Reading events table...")
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

        # Ensure both timestamp columns are datetime type
        merged_pd['timestamp_x'] = pd.to_datetime(merged_pd['timestamp_x'])
        weather_pd['hour'] = pd.to_datetime(weather_pd['hour'])

        # Sort both dataframes by their timestamp columns
        merged_pd = merged_pd.sort_values('timestamp_x')
        weather_pd = weather_pd.sort_values('hour')

        # Perform the merge_asof
        merged_pd = pd.merge_asof(
            merged_pd,
            weather_pd[['hour', 'temperature', 'precipitation_probability', 'weather_code', 'latitude', 'longitude']],
            left_on='timestamp_x',
            right_on='hour',
            direction='backward'
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

    except Exception as e:
        print(f"Error in perform_aggregations: {str(e)}")
        print("Available tables in database:")
        try:
            with create_engine(POSTGRES_URL).connect() as conn:
                result = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"))
                tables = [row[0] for row in result]
                print("Tables:", tables)
        except Exception as db_error:
            print(f"Error listing tables: {str(db_error)}")
        raise

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
    
    # grid for regression tree
    if best_params is None:
        best_params = {
            'criterion': 'friedman_mse',
            'max_depth': 10,
            'max_features': 'auto',  
            'min_samples_split': 2,  # Minimum samples required to split a node
            'min_samples_leaf': 1,   # Minimum samples required in a leaf node
            'random_state': 42       # For reproducibility
            }

    #rf = RandomForestRegressor(**best_params)
    rf = DecisionTreeRegressor()

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

def train_xgboost_model(final_clean, best_params = None):
    """Train an XGBoost model with the current data and MLflow tracking"""
    if best_params is None:
        best_params = {
            'objective': 'reg:squarederror',
            'max_depth': 8,
            'learning_rate': 0.05,
            'n_estimators': 200,
            'subsample': 0.8,
            'colsample_bytree': 1.0,
            'min_child_weight': 1,
            'gamma': 0,
            'random_state': 42,
            'tree_method': 'hist',
            'max_bin': 256,
            'scale_pos_weight': 1,
            'reg_alpha': 0,
            'reg_lambda': 1,
            'max_leaves': 64
        }

    X_train, X_test, y_train, y_test = split_data(final_clean, test_size=0.3, shuffle=True)
    boolean_features = ['peak_hour', 'event_dummy', 'school', 'hospital', 'weekend']
    for feature in boolean_features:
        if feature in X_train.columns:
            X_train[feature] = X_train[feature].astype(int)
            X_test[feature] = X_test[feature].astype(int)
    dtrain = xgb.DMatrix(X_train, label=y_train, feature_names=X_train.columns.tolist())
    dtest = xgb.DMatrix(X_test, label=y_test, feature_names=X_test.columns.tolist())

    # No mlflow.start_run here; use the active run from the main loop
    print(f"[MLflow] Logging params and metrics to run: {mlflow.active_run().info.run_id if mlflow.active_run() else 'No active run!'}")
    mlflow.log_params(best_params)
    model = xgb.train(
        best_params,
        dtrain,
        num_boost_round=best_params['n_estimators'],
        evals=[(dtrain, 'train'), (dtest, 'test')],
        early_stopping_rounds=20,
        verbose_eval=True
    )
    train_preds = model.predict(dtrain)
    test_preds = model.predict(dtest)
    train_rmse = root_mean_squared_error(y_train, train_preds)
    test_rmse = root_mean_squared_error(y_test, test_preds)
    mlflow.log_metric("train_rmse", train_rmse)
    mlflow.log_metric("test_rmse", test_rmse)
    importance_types = ['weight', 'gain', 'total_gain', 'cover', 'total_cover']
    for importance_type in importance_types:
        importance_dict = model.get_score(importance_type=importance_type)
        importance_df = pd.DataFrame({
            'feature': list(importance_dict.keys()),
            f'importance_{importance_type}': list(importance_dict.values())
        }).sort_values(f'importance_{importance_type}', ascending=False)
        importance_df.to_csv(f"feature_importance_{importance_type}.csv", index=False)
        mlflow.log_artifact(f"feature_importance_{importance_type}.csv")
    mlflow.xgboost.log_model(model, "model", registered_model_name="bus_congestion_model")
    signature = infer_signature(X_test, test_preds)
    mlflow.xgboost.log_model(model, "model", signature=signature)
    return model

def get_accuracy_model(final_clean, model, model_type='decision_tree'):
    """Get model accuracy with support for different model types"""
    X_train, X_test, y_train, y_test = split_data(final_clean, test_size=0.3, shuffle=True)
    
    if model_type == 'xgboost':
        dtest = xgb.DMatrix(X_test)
        preds = model.predict(dtest)
    else:
        preds = model.predict(X_test)
    
    rmse = root_mean_squared_error(y_true=y_test, y_pred=preds)
    print(f"RMSE is: {rmse}")
    return rmse

def get_current_champion_accuracy():
    """Get the accuracy of the current champion model"""
    try:
        # Connect to MLflow
        client = mlflow.tracking.MlflowClient()
        
        # Get the current champion model
        champion_model = client.get_registered_model("bus_congestion_model")
        if not champion_model:
            return None
            
        # Get the latest version
        latest_versions = client.get_latest_versions("bus_congestion_model", stages=["Production"])
        if not latest_versions:
            return None
            
        # Get the run ID from the latest version
        run_id = latest_versions[0].run_id
        run = client.get_run(run_id)
        
        # Get the accuracy metric
        return run.data.metrics.get("test_rmse")
    except Exception as e:
        print(f"[Training] Error getting champion accuracy: {e}")
        return None

def run_training_pipeline():
    """Run the training pipeline once and return the training status"""
    try:
        print("[Training] Starting training pipeline...")
        # Wait for services to be ready
        max_retries = 10
        retry_count = 0
        while retry_count < max_retries:
            try:
                print("[Training] Checking service connections...")
                # Test database connection
                engine = create_db_connection()
                with engine as conn:
                    conn.execute(text("SELECT 1"))
                
                # Test MinIO connection
                print("[Training] Testing MinIO connection...")
                minio_client = Minio(
                    "minio:9000",
                    access_key="minioadmin",
                    secret_key="minioadmin",
                    secure=False
                )
                minio_client.list_buckets()
                
                # Test Kafka connection
                print("[Training] Testing Kafka connection...")
                producer = create_kafka_producer()
                
                # Initialize MLflow
                print("[Training] Setting up MLflow...")
                setup_mlflow()
                print("[Training] All services are ready!")
                break
            except Exception as e:
                retry_count += 1
                print(f"[Training] Service not ready (attempt {retry_count}/{max_retries}): {e}")
                if retry_count == max_retries:
                    raise Exception("Failed to connect to required services")
                time.sleep(5)

        # Run the training pipeline inside an MLflow run
        print("[Training] Starting MLflow run...")
        with mlflow.start_run(run_name="training_cycle"):
            print(f"[Training] Started MLflow run: {mlflow.active_run().info.run_id}")
            
            print("[Training] Performing data aggregations...")
            final_clean = perform_aggregations(selected_features=[
                'trip_id_x', 'timestamp_x', 'peak_hour', 'sine_time',
                'temperature', 'precipitation_probability', 'weather_code',
                'traffic_level', 'event_dummy', 'congestion_rate', 'school', 'hospital', 'weekend'
            ])
            final_clean = final_clean.drop_duplicates(subset=['trip_id_x', 'timestamp_x'])
            
            print("[Training] Writing to SQL...")
            write_to_sql(final_clean)
            
            print("[Training] Training XGBoost model...")
            xgb_model = train_xgboost_model(final_clean)
            acc = get_accuracy_model(final_clean, xgb_model, model_type='xgboost')
            
            print("[Training] Saving model to MinIO...")
            minio_client = Minio(
                "minio:9000",
                access_key="minioadmin",
                secret_key="minioadmin",
                secure=False
            )
            
            bucket_name = "models"
            if not minio_client.bucket_exists(bucket_name):
                minio_client.make_bucket(bucket_name)
                print(f"[Training] Created bucket: {bucket_name}")
            else:
                print(f"[Training] Using existing bucket: {bucket_name}")
            
            timestamp = int(time.time())
            model_key = f"challengers/challenger_{timestamp}.pkl"
            model_buffer = io.BytesIO()
            joblib.dump(xgb_model, model_buffer)
            model_buffer.seek(0)
            
            print("[Training] Uploading model to MinIO...")
            minio_client.put_object(
                bucket_name,
                model_key,
                data=model_buffer,
                length=model_buffer.getbuffer().nbytes,
                part_size=10*1024*1024
            )
            
            print("[Training] Sending metadata to Kafka...")
            try:
                message = {
                    "model_key": model_key,
                    "accuracy": float(acc),
                    "timestamp": timestamp
                }
                print(f"[Training] Sending message to Kafka: {message}")
                future = producer.send("model.train.topic", value=message)
                record_metadata = future.get(timeout=10)
                print(f"[Training] Message sent successfully to partition {record_metadata.partition} at offset {record_metadata.offset}")
                producer.flush()
                print("[Training] Producer flushed successfully")
            except Exception as e:
                print(f"[Training] Error sending to Kafka: {e}")
                raise
            finally:
                print("[Training] Closing Kafka producer...")
                producer.close()
            
            # Get current champion's accuracy
            current_champion_rmse = get_current_champion_accuracy()
            is_champion = False
            
            if current_champion_rmse is None or acc < current_champion_rmse:
                is_champion = True
                # If this is a better model, promote it to production
                client = mlflow.tracking.MlflowClient()
                model_details = mlflow.register_model(f"runs:/{mlflow.active_run().info.run_id}/model", "bus_congestion_model")
                client.transition_model_version_stage(
                    name="bus_congestion_model",
                    version=model_details.version,
                    stage="Production"
                )
                print(f"[Training] New champion model! Old RMSE: {current_champion_rmse}, New RMSE: {acc}")
            else:
                print(f"[Training] Not a champion model. Champion RMSE: {current_champion_rmse}, This RMSE: {acc}")
            
            print(f"[Training] Training completed! Model: {model_key} (RMSE: {acc:.4f})")
            return {
                "success": True,
                "model_key": model_key,
                "accuracy": acc,
                "is_champion": is_champion
            }
            
    except Exception as e:
        print(f"[Training] Error in training cycle: {e}")
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    time.sleep(60)
    main()