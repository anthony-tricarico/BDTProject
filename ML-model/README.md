# ML Model Training Pipeline

This component is responsible for training machine learning models to predict bus congestion rates. It includes both automated periodic training and manual training capabilities through a web interface.

## Overview

The training pipeline:

1. Aggregates data from multiple sources (tickets, sensors, traffic, weather, events)
2. Trains an XGBoost model to predict congestion rates
3. Evaluates model performance and determines if it's a new champion
4. Saves the model to MinIO storage
5. Sends model metadata to Kafka for deployment
6. Tracks experiments and models using MLflow

## Features

- **Data Aggregation**: Combines data from:
  - Passenger tickets
  - Sensor readings
  - Traffic conditions
  - Weather data
  - Special events
- **Model Training**: Uses XGBoost with MLflow tracking
- **Champion/Challenger Model**: Automatically evaluates and promotes better models
- **Manual Training**: Allows retraining via web interface
- **Model Storage**: Saves models to MinIO for persistence
- **Kafka Integration**: Sends model metadata for deployment
- **MLflow Integration**: Tracks experiments and model versions

## Technical Details

### Dependencies

- Python 3.9+
- Required packages:
  - xgboost
  - scikit-learn
  - pandas
  - dask
  - sqlalchemy
  - kafka-python
  - minio
  - joblib
  - mlflow
  - fastapi
  - uvicorn

### Configuration

The service connects to:

- PostgreSQL database (`db:5432`)
- Kafka broker (`kafka:9092`)
- MinIO storage (`minio:9000`)
- MLflow server (`mlflow:5001`)
- Dask scheduler (`dask-scheduler:8786`)

### Model Features

The model uses the following features:

- `trip_id_x`: Unique trip identifier.
- `timestamp_x`: Time of the trip
- `peak_hour`: Whether the trip is during peak hours
- `sine_time`: Cyclical encoding of time
- `temperature`: Current temperature
- `precipitation_probability`: Chance of rain
- `weather_code`: Weather condition code
- `traffic_level`: Traffic condition (0-3)
- `event_dummy`: Whether there's a special event
- `congestion_rate`: Target variable (to predict)
- `school`: Whether near a school
- `hospital`: Whether near a hospital
- `weekend`: Whether it's a weekend

## Usage

### Automated Training

The service runs automatically in the Docker environment with an initial 60-second delay. It:

1. Connects to required services
2. Trains a new model
3. Evaluates against current champion
4. Saves models to MinIO
5. Sends model metadata to Kafka

### Manual Training

You can trigger manual training through the web interface:

1. Navigate to either:
   - Bus Congestion Prediction page
   - Congestion Forecast page
2. Find the "Train New Model" button in the sidebar
3. Click to start training
4. Monitor training progress in the sidebar
5. View results including:
   - Model RMSE
   - Champion/Challenger status
   - Training success/failure

The training process will:

- Train a new model
- Compare with current champion
- Promote to champion if better (lower RMSE)
- Display appropriate status message:
  - 🏆 New champion model (if better)
  - Regular challenger model (if not better)

### Monitoring

Monitor the service logs:

```bash
docker-compose logs -f training-service
```

## Error Handling

The service includes robust error handling:

- Retries service connections
- Handles database connection issues
- Manages Kafka message delivery
- Handles MinIO storage errors
- Training timeout after 10 minutes
- Proper error messages in UI

## Integration

This component works in conjunction with:

- `prediction-service`: Uses the champion model
- `mlflow`: Tracks experiments and models
- `minio`: Stores model files
- `kafka`: Message broker for model metadata
- `postgresql`: Source of training data
- `dask`: Distributed computing
- `streamlit`: Web interface for manual training
