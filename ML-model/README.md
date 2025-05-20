# ML Model Training Pipeline

This component is responsible for training machine learning models to predict bus congestion rates. It runs periodically, trains new models, and sends them to a model evaluation service via Kafka.

## Overview

The training pipeline:

1. Aggregates data from multiple sources (tickets, sensors, traffic, weather, events)
2. Trains a Random Forest model to predict congestion rates
3. Saves the model to MinIO storage
4. Sends model metadata to Kafka for evaluation

## Features

- **Data Aggregation**: Combines data from:
  - Passenger tickets
  - Sensor readings
  - Traffic conditions
  - Weather data
  - Special events
- **Model Training**: Uses Random Forest Regressor with optimized parameters
- **Continuous Learning**: Runs every minute to train new models
- **Model Storage**: Saves models to MinIO for persistence
- **Kafka Integration**: Sends model metadata for evaluation

## Technical Details

### Dependencies

- Python 3.10
- Required packages:
  - scikit-learn
  - pandas
  - dask
  - sqlalchemy
  - kafka-python
  - minio
  - joblib

### Configuration

The service connects to:

- PostgreSQL database (`db:5432`)
- Kafka broker (`kafka:9092`)
- MinIO storage (`minio:9000`)

### Model Features

The model uses the following features:

- `trip_id_x`: Unique trip identifier
- `timestamp_x`: Time of the trip
- `peak_hour`: Whether the trip is during peak hours
- `seconds_from_midnight`: Time of day in seconds
- `temperature`: Current temperature
- `precipitation_probability`: Chance of rain
- `weather_code`: Weather condition code
- `traffic_level`: Traffic condition (0-3)
- `event_dummy`: Whether there's a special event
- `congestion_rate`: Target variable
- `school`: Whether near a school
- `hospital`: Whether near a hospital

## Usage

The service runs automatically in the Docker environment. It:

1. Connects to required services
2. Trains a new model every minute
3. Saves models to MinIO
4. Sends model metadata to Kafka

### Manual Testing

To test the service manually:

```bash
docker-compose up ml-model
```

### Logs

Monitor the service logs:

```bash
docker-compose logs -f ml-model
```

## Error Handling

The service includes robust error handling:

- Retries service connections
- Handles database connection issues
- Manages Kafka message delivery
- Handles MinIO storage errors

## Integration

This component works in conjunction with:

- `kafka-consumer-model`: Evaluates and promotes models
- `minio`: Stores model files
- `kafka`: Message broker for model metadata
- `postgresql`: Source of training data
