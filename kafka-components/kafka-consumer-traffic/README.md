# Kafka Consumer Traffic

This component is a Kafka consumer designed to process traffic data and store it in a PostgreSQL database table. It is part of a larger system that manages bus occupancy data, integrating traffic information to enhance data analysis.

## Overview

The `traffic_consumer.py` script connects to a Kafka topic to consume traffic data messages. It then processes these messages and stores them in a PostgreSQL database table named `traffic`.

## Features

- **Kafka Consumer**: Listens to the `traffic.topic` on a Kafka server for incoming traffic data.
- **PostgreSQL Integration**: Connects to a PostgreSQL database to store traffic data in the `traffic` table.
- **Data Processing**: Extracts traffic information such as traffic level, normal travel time, and traffic-affected travel time from the consumed messages.
- **Error Handling**: Includes basic error handling to retry Kafka connection if the server is not ready.
- **Table Creation**: Automatically creates the `traffic` table if it doesn't exist in the database.

## Setup

1. **Kafka Consumer**: The script initializes a Kafka consumer to listen to the `traffic.topic`. It retries the connection every 3 seconds if the Kafka server is not ready.

2. **PostgreSQL Connection**: Connects to a PostgreSQL database named `raw_data` using the credentials provided in the script. Ensures the `traffic` table is set up correctly.

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
