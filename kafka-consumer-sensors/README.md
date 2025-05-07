# Sensors API (Legacy)

This API exposes the following endpoints on port 8002 of local host:

1. `/latest_sensors` -> returns the latest measurement
2. `/stream_sensors` -> returns the latest 100 measurements

## Examples

`/latest_sensors` and `/stream_sensors` are easily accessible by sending requests to `http://localhost:8002/latest_sensors` and `http://localhost:8002/stream_sensors`, respectively.
