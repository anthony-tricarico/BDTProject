# Tickets Producer

This is the Kafka producer of sensors data, it gets data from the stream exposed by the `kafka-consumers-passengers` API on port `8000` at endpoint `\stream`.

Thanks to the predicted number of passengers `predicted_passengers_in`, this producer generates fictitious data from tickets.

# Produced values

`ticket_id`: identifier of the single ticket
`timestamp`: time when the ticket is activated (coincides with bus departure)
`stop_id`: unique identifier of the stop at which the ticket was created
`route`: name of the route
`passenger_type`: one of adult, student, senior (influences ticket fare)
`fare`: the cost of the ticket
`bus_id`: the unique identifier of the bus
`trip_id`: the unique identifier of the trip

The dictionary of values is then sent to the `ticketing.topic` on Kafka.
