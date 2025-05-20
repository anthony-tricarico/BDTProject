# Sensor Data Producer

This component generates sensor data based on passenger predictions. It consumes passenger prediction messages from Kafka and generates corresponding sensor activations for each predicted passenger alighting.

## Overview

The producer:

1. Consumes passenger prediction messages from the `bus.passenger.predictions` topic
2. Generates sensor activations for each predicted passenger alighting
3. Publishes sensor data to the `sensors.topic` for downstream processing

## Features

- **Event-Driven Generation**: Generates sensor data based on passenger predictions
- **Deduplication**: Ensures each prediction generates sensor data only once
- **Real-time Processing**: Processes passenger predictions as they arrive
- **Configurable Rate**: Adjustable message production rate via environment variable

## Technical Details

### Dependencies

- Python 3.10
- Required packages:
  - kafka-python

### Configuration

The service connects to:

- Kafka broker (`kafka:9092`)
- Uses environment variable `SLEEP` to control message production rate (default: 1 second)

### Message Flow

1. **Input**: Consumes messages from `bus.passenger.predictions` topic
2. **Processing**: Generates sensor activations for each predicted passenger alighting
3. **Output**: Publishes to `sensors.topic`

### Produced Message Format

Messages are published to the `sensors.topic` with the following structure:

```json
{
  "measurement_id": "stop_id-route-timestamp-index", // Unique identifier for each sensor activation
  "timestamp": "2024-03-20T10:00:00", // ISO format timestamp from passenger prediction
  "stop_id": "123", // Stop identifier
  "route": "5", // Route number
  "status": 1, // Sensor status (hardcoded)
  "activation_type": 2, // Activation type (hardcoded)
  "bus_id": 42, // Bus identifier
  "trip_id": "456", // Trip identifier
  "peak_hour": true, // Whether during peak hours
  "hospital": true, // Whether stop is near hospital
  "school": false // Whether stop is near school
}
```

### Processing Logic

1. For each passenger prediction message:
   - Check if the prediction has been processed before
   - For each predicted passenger alighting:
     - Generate a unique measurement ID
     - Create a sensor activation message
     - Publish to sensors topic

## Usage

### Running the Service

The service runs automatically in the Docker environment. To start it:

```bash
docker-compose up kafka-producer-sensors
```

### Configuration Options

Set the `SLEEP` environment variable to control the rate of message production:

```bash
SLEEP=0.1  # Produces messages with 0.1 second delay
```

### Monitoring

Monitor the service logs:

```bash
docker-compose logs -f kafka-producer-sensors
```

## Integration

This component works in conjunction with:

- `kafka-producer-passengers`: Source of passenger predictions
- `kafka`: Message broker for both input and output
- Downstream consumers of the `sensors.topic`

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
python sensors_producer.py
```

## Troubleshooting

Common issues and solutions:

1. **Kafka Connection Issues**:

   - Verify Kafka broker is running
   - Check network connectivity
   - Verify topics exist and are accessible

2. **Message Processing Issues**:

   - Check input message format
   - Verify message deserialization
   - Monitor for missing required fields

3. **Performance Issues**:

   - Adjust SLEEP value if messages are being processed too quickly/slowly
   - Monitor message queue sizes
   - Check for message processing delays

4. **Deduplication Issues**:
   - Monitor the size of the already_predicted list
   - Check for memory usage if running for extended periods
   - Verify prediction_id uniqueness
