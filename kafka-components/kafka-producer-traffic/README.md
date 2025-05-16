# Get Traffic Data

This component of the application is responsible for generating traffic data. It fetches traffic information from the Google Maps API, creates a Kafka producer, and streams the data to the `traffic.topic` Kafka topic. Consumers can subscribe to this topic to receive real-time traffic data as soon as it becomes available.

## Role of Redis

Redis is utilized in this component as a scalable cache manager to enforce rate limiting on API requests. By storing timestamps of recent requests, Redis helps ensure that the application does not exceed the maximum number of allowed requests per second to the Google Maps API. This is crucial for maintaining compliance with API usage policies and preventing service disruptions due to rate limiting.

## Produced Fields

The generator publishes to the specified topic and produces the following data fields:

- `measurement_id`: A unique identifier for the measurement.
- `timestamp`: A date-time object indicating when the measurement occurred.
- `stop_id`: A unique identifier for the stop.
- `route`: A unique identifier for the route the bus is traveling on.
- `bus_id`: A unique identifier for the bus for which traffic data was requested.
- `trip_id`: A unique identifier for the trip.
- `traffic_level`: The estimated amount of traffic, categorized into four levels based on the additional travel time compared to traveling without any traffic.

### Traffic Levels

1. `no traffic/low`: Up to 10% additional travel time.
2. `medium`: Up to 30% additional travel time.
3. `heavy`: Travel time is doubled.
4. `severe/standstill`: Travel time is more than doubled.

This component is designed to efficiently manage traffic data generation and distribution, leveraging both Kafka for data streaming and Redis for scalable caching and rate limiting.
