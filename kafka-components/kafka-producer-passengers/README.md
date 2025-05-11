# Generate Passenger Data

This a data generator component of the application. Specifically, it generates passenger data, creates a Kafka producer, and posts the stream of data on the topic `bus.passenger.predictions` so that Kafka consumers can subscribe and obtain the messages with the data as soon as the cluster makes it available to them.

## Produced fields

The generator publishes to the above-specified topic and produces the following data fields:

`prediction_id`: pred_id
`timestamp`: sim_time.isoformat()
`stop_id`: str(stop)
`route`: str(route)
`predicted_passengers_in`: passenger_in
`predicted_passengers_out`: passenger_out
`shape_id`: str(shape_id)
`trip_id`: str(trip_idx)
`stop_sequence`: str(sequence)
