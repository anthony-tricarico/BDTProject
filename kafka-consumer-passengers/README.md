# Passengers API

This API exposes the following endpoints on port 8000 of local host:

1. /latest -> returns the latest measurement
2. /stream -> returns the latest 100 measurements
3. /filter-by-route -> allows to filter the latest 100 measurements by route

## Examples

`/stream` and `/latest` are easily accessible by sending requests to `http://localhost:8000/stream` and `http://localhost:8000/latest`, respectively.

You can use `/filter-by-route` by sending requests in this format after connection to `http://localhost:8000/filter-by-route`. For example to get predictions for route L1:

```
http://localhost:8000/filter-by-route?route=L1
```
