# Ticket Data Producer

This component generates ticket data based on passenger predictions. It consumes passenger prediction messages from Kafka and generates corresponding tickets for each predicted passenger boarding.

## Overview

The producer:

1. Consumes passenger prediction messages from the `bus.passenger.predictions` topic
2. Generates tickets for each predicted passenger boarding
3. Publishes ticket data to the `ticketing.topic` for downstream processing

## Features

- **Event-Driven Generation**: Generates ticket data based on passenger predictions
- **Deduplication**: Ensures each prediction generates ticket data only once
- **Real-time Processing**: Processes passenger predictions as they arrive
- **Configurable Rate**: Adjustable message production rate via environment variable
- **Dynamic Pricing**: Generates different ticket types with corresponding fares

## Technical Details

### Dependencies

- Python 3.10
- Required packages:
  - kafka-python

### Configuration

The service connects to:

- Kafka broker (`kafka:9092`)
- Uses environment variable `SLEEP` to control message production rate (default: 0.5 seconds)

### Message Flow

1. **Input**: Consumes messages from `bus.passenger.predictions` topic
2. **Processing**: Generates tickets for each predicted passenger boarding
3. **Output**: Publishes to `ticketing.topic`

### Produced Message Format

Messages are published to the `ticketing.topic` with the following structure:

```json
{
  "ticket_id": "stop_id-route-timestamp-index", // Unique identifier for each ticket
  "timestamp": "2024-03-20T10:00:00", // ISO format timestamp from passenger prediction
  "stop_id": "123", // Stop identifier
  "route": "5", // Route number
  "passenger_type": "adult", // Randomly selected from ["adult", "student", "senior"]
  "fare": 2.5, // Fare based on passenger type (adult: 2.50, student: 1.25, senior: 1.00)
  "bus_id": 42, // Bus identifier
  "trip_id": "456", // Trip identifier
  "peak_hour": true, // Whether during peak hours
  "hospital": true, // Whether a specific route can be used to reach a hospital
  "school": false // Whether a specific route can be used to reach a school
}
```

### Processing Logic

1. For each passenger prediction message:
   - Check if the prediction has been processed before
   - For each predicted passenger boarding:
     - Generate a unique ticket ID
     - Randomly select passenger type and corresponding fare
     - Create a ticket message
     - Publish to ticketing topic

### Fare Structure

- Adult: €2.50
- Student: €1.25
- Senior: €1.00

## Usage

### Running the Service

The service runs automatically in the Docker environment. To start it:

```bash
docker-compose up kafka-producer-tickets
```

### Configuration Options

Set the `SLEEP` environment variable to control the rate of message production:

```bash
SLEEP=0.1  # Produces messages with 0.1 second delay
```

### Monitoring

Monitor the service logs:

```bash
docker-compose logs -f kafka-producer-tickets
```

## Integration

This component works in conjunction with:

- `kafka-producer-passengers`: Source of passenger predictions
- `kafka`: Message broker for both input and output
- Downstream consumers of the `ticketing.topic`

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
python ticketing_producer.py
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

5. **Ticket Generation Issues**:
   - Verify passenger type distribution
   - Check fare calculations
   - Monitor ticket ID uniqueness
