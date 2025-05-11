# Get GPS Data

This is a data generator component of the application. Specifically, it fetches hourly weather forecast data for the city of Trento from the freely available [Open-meteo API](https://open-meteo.com/en/docs?latitude=46.0679&longitude=11.1211&forecast_days=1&hourly=temperature_2m,precipitation_probability,rain,showers,snowfall,snow_depth,weather_code#weather_variable_documentation), creates a Kafka producer, and posts the stream of data on the topic `weather.topic` so that Kafka consumers can subscribe and obtain the messages with the data as soon as the cluster makes it available to them.

## Produced fields

The generator publishes to the above-specified topic and produces the following data fields:

- `measurement_id`: unique id for the measurement
- `timestamp`: date-time object referring to when the call to the API was made
- `latitude`: indicates the latitude for the measurement
- `longitude`: indicates the longitude for the measurement
- `weather_code`: a WMO code referring to the weather conditions at a specific hour. More information in the table below.
- `precipitation_probability`: probability of precipitation at a given hour
- `temperature`: temperature at a given hour
- `hour`: hour for which the forecast is made

| Code(s)    | Description                                      |
| ---------- | ------------------------------------------------ |
| 0          | Clear sky                                        |
| 1, 2, 3    | Mainly clear, partly cloudy, and overcast        |
| 45, 48     | Fog and depositing rime fog                      |
| 51, 53, 55 | Drizzle: Light, moderate, and dense intensity    |
| 56, 57     | Freezing Drizzle: Light and dense intensity      |
| 61, 63, 65 | Rain: Slight, moderate and heavy intensity       |
| 66, 67     | Freezing Rain: Light and heavy intensity         |
| 71, 73, 75 | Snow fall: Slight, moderate, and heavy intensity |
| 77         | Snow grains                                      |
| 80, 81, 82 | Rain showers: Slight, moderate, and violent      |
| 85, 86     | Snow showers: Slight and heavy                   |
| 95 \*      | Thunderstorm: Slight or moderate                 |
| 96, 99 \*  | Thunderstorm with slight and heavy hail          |
