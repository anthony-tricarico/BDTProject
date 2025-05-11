# Get GPS Data

This is a data generator component of the application. Specifically, it produces GPS information for each bus, creates a Kafka producer, and posts the stream of data on the topic `gps.topic` so that Kafka consumers can subscribe and obtain the messages with the data as soon as the cluster makes it available to them.

## Produced fields

The generator publishes to the above-specified topic and produces the following data fields:

- `measurement_id`: id for the measurement
- `timestamp`: date-time object referring to when the measurement occurred
- `stop_id`: unique identifier of the stop
- `route`: unique identifier of the route the bus is traveling on
- `bus_id`: unique identifier of the bus for which traffic data was requested
- `trip_id`: unique identifier of the trip
- `latitude`: indicates the latitude at the specific timestamp
- `longitude`: indicates the longitude at the specific timestamp
