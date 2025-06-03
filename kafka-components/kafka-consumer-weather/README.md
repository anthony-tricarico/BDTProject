# Kafka Consumer Weather

This component is a Kafka consumer designed to process weather data and update a PostgreSQL database table with the received information. It is part of a larger system that manages bus occupancy data, integrating weather information to enhance data analysis.

## Overview

The `weather_consumer.py` script connects to a Kafka topic to consume weather data messages. It then processes these messages and updates a PostgreSQL database table named `weather` with the relevant weather information.

## Features

- **Kafka Consumer**: Listens to the `weather.topic` on a Kafka server for incoming weather data.
- **PostgreSQL Integration**: Connects to a PostgreSQL database to update the `weather` table with the latest weather data.
- **Data Processing**: Extracts weather information such as timestamp, latitude, longitude, temperature, precipitation probability, and weather code from the consumed messages.
- **Error Handling**: Includes basic error handling to retry Kafka connection if the server is not ready.

## Setup

1. **Kafka Consumer**: The script initializes a Kafka consumer to listen to the `weather.topic`. It retries the connection every 3 seconds if the Kafka server is not ready.

2. **PostgreSQL Connection**: Connects to a PostgreSQL database named `raw_data` using the credentials provided in the script. Ensure the database and the `weather` table are set up correctly.

3. **Environment**: The script is designed to run in a Docker environment. Ensure Docker is installed and configured to use the provided `Dockerfile`.

## Usage

- **Running the Consumer**: The script continuously listens for messages on the Kafka topic and updates the database table with the received data.
- **Data Insertion**: Before inserting new data, the script truncates the `weather` table to ensure only the latest data is stored.

## Dependencies

- `kafka-python`: For Kafka consumer functionality.
- `psycopg2`: For PostgreSQL database interaction.
- `json`: For parsing JSON messages.

## Docker

A `Dockerfile` is provided to containerize the application. Ensure Docker is installed and configured to build and run the container.
