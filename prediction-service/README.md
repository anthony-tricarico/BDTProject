# Bus Congestion Prediction Service

This component provides a REST API service for making real-time predictions about bus congestion rates using the champion machine learning model stored in MinIO.

## Overview

The prediction service:

1. Loads the champion model from MinIO storage
2. Provides a REST API for making predictions
3. Automatically reloads the model periodically to ensure it's using the latest champion
4. Includes health checks and comprehensive error handling

## Features

- **Real-time Predictions**: Make predictions using the latest champion model
- **Automatic Model Updates**: Reloads the model every 5 minutes (by default) to ensure it's using the latest champion. The time interval used to reload the model can be changed by modifying the `MODEL_RELOAD_INTERVAL` environment variable under the `prediction_service` service in the `docker-compose.yml` file. This value should be the number of seconds before a new model is retrieved from the model storage (only integer values admitted).
- **REST API**: Easy-to-use HTTP endpoints for predictions
- **Health Monitoring**: Built-in health check endpoint
- **Error Handling**: Comprehensive error handling for model loading and prediction failures
- **Input Validation**: Uses Pydantic for robust input validation

## Technical Details

### Dependencies

- Python 3.10
- Required packages:
  - FastAPI
  - Uvicorn
  - scikit-learn
  - joblib
  - minio
  - numpy
  - pydantic

### Configuration

The service connects to:

- MinIO storage (`minio:9000`)
- Uses default credentials (can be configured via environment variables)

### API Endpoints

#### Health Check

```http
GET /health
```

Response:

```json
{
  "status": "healthy",
  "model_loaded": true
}
```

#### Make Prediction

```http
POST /predict
```

Request body:

```json
{
  "trip_id": 123,
  "timestamp": "2024-03-20T10:00:00",
  "peak_hour": true,
  "sine_time": 0.5,
  "temperature": 20.5,
  "precipitation_probability": 0.2,
  "weather_code": 1,
  "traffic_level": 2,
  "event_dummy": false,
  "school": true,
  "hospital": false,
  "weekend": false
}
```

Response:

```json
{
  "prediction": 0.75,
  "model_timestamp": 1710936000
}
```

### Model Features

The prediction model uses the following features:

- `peak_hour`: Whether the trip is during peak hours
- `sine_time`: Cyclical time feature
- `temperature`: Current temperature
- `precipitation_probability`: Chance of rain
- `weather_code`: Weather condition code
- `traffic_level`: Traffic condition (0-3)
- `event_dummy`: Whether there's a special event
- `school`: Whether near a school
- `hospital`: Whether near a hospital
- `weekend`: Whether the trip is on a weekend

## Usage

### Running the Service

The service runs automatically in the Docker environment. To start it:

```bash
docker-compose up prediction-service
```

The service will be available at `http://localhost:8006`.

### Manual Testing

You can test the API using curl:

```bash
# Health check
curl http://localhost:8006/health

# Make a prediction
curl -X POST http://localhost:8006/predict \
  -H "Content-Type: application/json" \
  -d '{
    "trip_id": 123,
    "timestamp": "2024-03-20T10:00:00",
    "peak_hour": true,
    "sine_time": 0.5,
    "temperature": 20.5,
    "precipitation_probability": 0.2,
    "weather_code": 1,
    "traffic_level": 2,
    "event_dummy": false,
    "school": true,
    "hospital": false,
    "weekend": false
  }'
```

### Monitoring

Monitor the service logs:

```bash
docker-compose logs -f prediction-service
```

## Error Handling

The service includes robust error handling:

- **Model Loading Errors**: Retries loading the model on startup and periodically
- **Prediction Errors**: Returns appropriate HTTP status codes and error messages
- **Input Validation**: Validates all input data before processing
- **Service Health**: Provides health check endpoint for monitoring

## Integration

This component works in conjunction with:

- `minio`: Stores and serves the champion model
- `kafka-consumer-model`: Manages the champion/challenger model pattern
- `ml-model`: Trains new models

### Adding New Features

1. The service is built with FastAPI, making it easy to add new endpoints
2. Model reloading logic can be modified in the `load_model()` function
3. Input validation can be extended by modifying the `PredictionInput` class

## Troubleshooting

Common issues and solutions:

1. **Model Loading Failures**:

   - Check MinIO connection
   - Verify model file exists in MinIO
   - Check model file format

2. **Prediction Errors**:

   - Verify input data format
   - Check feature values are within expected ranges
   - Monitor model performance

3. **Service Unavailability**:
   - Check service logs
   - Verify port availability
   - Check MinIO health status
