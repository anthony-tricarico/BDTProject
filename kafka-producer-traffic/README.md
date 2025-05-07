# Get Traffic Data

This is a data generator component of the application. Specifically, it fetches traffic data information from the GMaps API, creates a Kafka producer, and posts the stream of data on the topic `traffic.topic` so that Kafka consumers can subscribe and obtain the messages with the data as soon as the cluster makes it available to them.

## Produced fields

The generator publishes to the above-specified topic and produces the following data fields:

- `measurement_id`: id for the measurement
- `timestamp`: date-time object referring to when the measurement occurred
- `stop_id`: unique identifier of the stop
- `route`: unique identifier of the route the bus is traveling on
- `bus_id`: unique identifier of the bus for which traffic data was requested
- `trip_id`: unique identifier of the trip
- `traffic_level`: the amount of traffic estimated by GMaps further discretized into four categories

1. `no traffic/low`: 10% additional travel time on route compared to traveling without any traffic
2. `medium`: 30% additional travel time on route compared to traveling without any traffic
3. `heavy`: doubled travel time on route compared to traveling without any traffic
4. `severe/standstill`: more than doubled travel time on route compared to traveling without any traffic
