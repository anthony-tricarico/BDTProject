# SQL Scripts for Occupancy Management

This component contains SQL scripts and a Docker setup to manage and update bus occupancy data in a PostgreSQL database. The scripts calculate the current occupancy of buses based on ticket and sensor data, and update the database with the latest occupancy information.

## Contents

- `update_occupancy.sql`: SQL script to create views and update the total occupancy of buses.
- `Dockerfile`: Dockerfile to set up a container that runs the SQL update script at regular intervals.
- `run_sql.sh`: Shell script to execute the SQL script using `psql`.

## Setup

1. **Build the Docker Image**

   Navigate to the `sql-scripts` directory and build the Docker image:

   ```bash
   docker build -t sql-scripts .
   ```

2. **Run the Docker Container**

   Start the container to execute the SQL script at regular intervals:

   ```bash
   docker run --name sql-scripts-container sql-scripts
   ```

## Usage

- The `update_occupancy.sql` script creates views to calculate the number of passengers on each bus and updates the `total_occupancy` table with the latest data.
- The `bus_occupancy` view provides the current occupancy percentage for each bus, calculated as the number of passengers divided by the bus's total capacity.

## Environment Variables

Ensure the following environment variables are set for database connection:

- `DB_NAME`: Name of the PostgreSQL database (e.g., `raw_data`)
- `DB_USER`: PostgreSQL user (e.g., `postgres`)
- `DB_HOST`: Hostname of the PostgreSQL server (e.g., `db`)
- `DB_PORT`: Port number of the PostgreSQL server (default is `5432`)
- `DB_PASSWORD`: Password for the PostgreSQL user

## Notes

- The `run_sql.sh` script runs in an infinite loop, executing the SQL script every 5 seconds to keep the occupancy data up-to-date.
- Ensure that the PostgreSQL service is running and accessible from the Docker container.
