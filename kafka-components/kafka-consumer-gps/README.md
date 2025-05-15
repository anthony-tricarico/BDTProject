# Kafka Consumer GPS

This component is a Kafka consumer designed to process GPS location data from buses and store it in a PostgreSQL database table. It is part of a larger system that manages bus occupancy data, integrating location tracking to enhance data analysis and visualization.

## Overview

The `gps_consumer.py` script connects to a Kafka topic to consume GPS data messages. It then processes these messages and stores them in a PostgreSQL database table named `gps`.

## Features

- **Kafka Consumer**: Listens to the `gps.topic` on a Kafka server for incoming GPS data.
- **PostgreSQL Integration**: Connects to a PostgreSQL database to store GPS data in the `gps` table.
- **Data Processing**: Extracts location information including latitude, longitude, bus identification, and route information from the consumed messages.
- **Error Handling**: Includes basic error handling to retry Kafka connection if the server is not ready.
- **Table Creation**: Automatically creates the `gps` table if it doesn't exist in the database.

## Setup

1. **Kafka Consumer**: The script initializes a Kafka consumer to listen to the `gps.topic`. It retries the connection every 3 seconds if the Kafka server is not ready.

2. **PostgreSQL Connection**: Connects to a PostgreSQL database named `raw_data` using the credentials provided in the script. Ensures the `gps` table is set up correctly.

3. **Environment**: The script is designed to run in a Docker environment. Ensure Docker is installed and configured to use the provided `Dockerfile`.

## Usage

- **Running the Consumer**: The script continuously listens for messages on the Kafka topic and inserts the received data into the database table.
- **Data Insertion**: The script uses the `measurement_id` as a primary key to avoid duplicates, with an "ON CONFLICT DO NOTHING" strategy.

## Dependencies

- `kafka-python`: For Kafka consumer functionality.
- `psycopg2`: For PostgreSQL database interaction.
- `json`: For parsing JSON messages.

## Docker

A `Dockerfile` is provided to containerize the application. Ensure Docker is installed and configured to build and run the container.

## Notes

- Ensure the Kafka server and PostgreSQL database are running and accessible from the environment where the script is executed.
- The GPS data provides real-time location tracking of buses, which can be used for route visualization, delay analysis, and integration with other data sources.
- Adjust the database credentials and Kafka server details in the script as needed for your environment.
