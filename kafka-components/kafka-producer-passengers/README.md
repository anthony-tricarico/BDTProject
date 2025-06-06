# Passenger Prediction Producer

This component generates and publishes passenger predictions for bus routes. It simulates passenger flow for bus routes and publishes the data to Kafka.

## Overview

The producer:

1. Loads passenger data from a CSV file
2. Simulates bus routes and stops based on existing data
3. Assigns buses dynamically to routes
4. Publishes predictions to Kafka for downstream processing

## Features

- **Data-Driven Simulation**: Uses real passenger data from CSV file
- **Dynamic Bus Assignment**: Maintains active and inactive bus pools
- **Time-based Simulation**: Generates data for consecutive days of operations
- **Location-based Features**: Includes hospital and school proximity data

## Technical Details

### Dependencies

- Python 3.10
- Required packages:
  - kafka-python
  - pandas
  - sqlalchemy
  - psycopg2-binary
  - python-dotenv

### Configuration

The service connects to:

- Kafka broker (`kafka:9092`)
- PostgreSQL database (`db:5432`)
- Uses environment variables:
  - `SLEEP`: Controls message production rate
  - `BUSES`: One of `selected` or `None`; default: `selected`. This variable manages which bus routes will be used. When this is set to `selected` only some routes will be shown, whereas when this is set to `None` all bus routes from Trento urban transportation system will be used.
  - `GOOGLE_API_KEY`: Optional, enables additional traffic condition data retrieval from Google Maps Distance Matrix API. Make sure to set this running the appropriate command as explained in the project's `README` file.

**IMPORTANT**: selecting a relatively high sleep rate along with setting `BUSES` to `None` will slow down the data generation process as the day indicated in the fictitious timestamp will only change when all trips within a day for all routes selected will be generated. Use this option with caution as a smaller `SLEEP` value will consume more computing resources.

### Produced Message Format

Messages are published to the `bus.passenger.predictions` topic with the following structure:

```json
{
  "prediction_id": 123,
  "timestamp": "2024-03-20T10:00:00",
  "stop_id": "123",
  "route": "1",
  "predicted_passengers_in": 5,
  "predicted_passengers_out": 3,
  "shape_id": "123",
  "trip_id": "456",
  "stop_sequence": "1",
  "bus_id": 42,
  "weekend": 0,
  "peak_hour": true,
  "hospital": true,
  "school": false,
  "traffic": "medium" // Only included if GOOGLE_API_KEY is set
}
```

### Data Source

The service requires:

- `passengers_with_shapes.csv`: Dataset containing route, stop, and passenger information

### Simulation Logic

1. **Bus Assignment**:

   - Maintains a pool of available buses
   - Randomly shuffles bus assignments
   - Reassigns buses when pool is exhausted

2. **Route Processing**:

   - Skips stops with null shape IDs
   - Processes each stop in sequence

3. **Time Simulation**:

   - Starts from next day at midnight
   - Advances by one day after completing all routes
   - Uses actual departure times from data

## Usage

### Configuration Options

Environment variables:

```bash
SLEEP=0.1  # Delay between messages in seconds
GOOGLE_API_KEY=your_key  # Optional: Enables traffic condition data
```

Note: the GOOGLE_API_KEY is set when running the script `src/guided_setup.py`

## Integration

This component integrates with:

- `kafka`: Message broker for predictions
- `db`: Provides maximum bus ID information
- Other consumers of the `bus.passenger.predictions` topic

## Troubleshooting

Common issues and solutions:

1. **Data Loading Issues**:

   - Verify `passengers_with_shapes.csv` exists
   - Check CSV file format and required columns
   - Ensure proper file permissions

2. **Database Connection Issues**:

   - Check database credentials
   - Verify database is running
   - Ensure bus table exists with bus_id column

3. **Kafka Connection Issues**:

   - Verify Kafka broker is running
   - Check network connectivity
   - Verify topic exists and is accessible

4. **Message Rate Issues**:

   - Adjust SLEEP environment variable
   - Monitor system resources
