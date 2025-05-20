# Passenger Prediction Producer

This component generates and publishes passenger predictions for bus routes using machine learning models. It simulates passenger flow for bus routes and publishes the data to Kafka.

## Overview

The producer:

1. Loads pre-trained models for predicting passenger inflow and outflow
2. Simulates bus routes and stops
3. Generates data for each stop
4. Publishes data to Kafka for downstream processing

## Features

- **Dual Model Generation**: Uses separate models for passenger inflow and outflow
- **Route Simulation**: Simulates bus routes with realistic stop sequences
- **Time-based Simulation**: Generates data for a full day of operations
- **Bus Assignment**: Dynamically assigns buses to routes
- **Realistic Constraints**: Tries to apply realistic constraints to passenger numbers (as much as possible)

## Technical Details

### Dependencies

- Python 3.10
- Required packages:
  - kafka-python
  - pandas
  - scikit-learn
  - sqlalchemy
  - psycopg2-binary

### Configuration

The service connects to:

- Kafka broker (`kafka:9092`)
- PostgreSQL database (`db:5432`)
- Uses environment variable `SLEEP` to control message production rate

### Produced Message Format

Messages are published to the `bus.passenger.predictions` topic with the following structure:

```json
{
  "prediction_id": 123, // Unique identifier for each prediction
  "timestamp": "2024-03-20T10:00:00", // ISO format timestamp
  "stop_id": "123", // Stop identifier
  "route": "5", // Route number (currently supports "5" and "8" but can be extended to any route)
  "predicted_passengers_in": 5, // Predicted number of passengers boarding
  "predicted_passengers_out": 3, // Predicted number of passengers getting off
  "shape_id": "123", // Geographic shape identifier for the route
  "trip_id": "456", // Trip identifier
  "stop_sequence": "1", // Stop sequence in the trip
  "bus_id": 42, // Assigned bus identifier
  "weekend": 0, // 1 if weekend, 0 if weekday
  "peak_hour": true, // Whether the trip is during peak hours
  "hospital": true, // Whether the stop is near a hospital
  "school": false // Whether the stop is near a school
}
```

### Model Features

The prediction models use the following features:

- `arrival_time`: Time of arrival in seconds from midnight
- `encoded_routes`: Encoded route identifier
- `weekend`: Whether it's a weekend (1) or weekday (0)
- `peak_hour`: Whether the trip is during peak hours

### Prediction Constraints

- Only stops with valid shape IDs are considered

## Usage

### Running the Service

The service runs automatically in the Docker environment. To start it:

```bash
docker-compose up kafka-producer-passengers
```

### Configuration Options

Set the `SLEEP` environment variable to control the rate of message production:

```bash
SLEEP=0.1  # Produces messages with 0.1 second delay
```

### Monitoring

Monitor the service logs:

```bash
docker-compose logs -f kafka-producer-passengers
```

## Integration

This component works in conjunction with:

- `kafka-consumer-passengers`: Consumes the predictions
- `db`: Provides bus and route information
- `kafka`: Message broker for predictions

## Development

### Local Development

1. Create a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Run the service:

```bash
python app.py
```

### Required Files

The service requires the following files in its directory:

- `treemodel.pkl`: Model for predicting passenger inflow
- `treemodel_out.pkl`: Model for predicting passenger outflow
- `labelencoder.pkl`: Label encoder for route encoding
- `passengers_with_shapes.csv`: Dataset containing route and stop information

## Troubleshooting

Common issues and solutions:

1. **Model Loading Failures**:

   - Verify model files exist in the correct location
   - Check model file format and compatibility
   - Ensure proper file permissions

2. **Database Connection Issues**:

   - Check database credentials
   - Verify database is running
   - Check network connectivity

3. **Kafka Connection Issues**:

   - Verify Kafka broker is running
   - Check network connectivity
   - Verify topic exists and is accessible

4. **Prediction Issues**:
   - Check input data format
   - Verify model features match training data
   - Monitor prediction ranges
