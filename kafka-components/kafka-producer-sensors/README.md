# Sensors Producer

This is the Kafka producer of sensors data, it gets data from the stream exposed by the `kafka-consumers-passengers` API on port `8000` at endpoint `\stream`.

With the stream of data, it then simulates the number of sensors getting activated by people getting out of the bus (courtesy of `predicted_passengers_out` from the original stream)

# Produced values

- `measurement_id`: identifier of the sensor activation
- `timestamp`: timestamp when sensor activated
- `stop_id`: the unique identifier of the stop at which the sensor was activated
- `route`: the route on which the sensor was activated
- `status`: the activation status of the sensor (1: active, missing value: not active)
- `activation_type`: whether the measurement refers to the front (1) or back door of the bus (2)
- `bus_id`: the unique identifier of the bus
- `trip_id`: the unique identifier of the trip

The dictionary of values is then sent to the `sensors.topic` on Kafka.
