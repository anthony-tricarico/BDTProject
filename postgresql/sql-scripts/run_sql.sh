#!/bin/bash

# Database connection details
DB_NAME="raw_data"
DB_USER="postgres"
DB_HOST="db"  # This should match the service name in your docker-compose.yml
DB_PORT="5432"
DB_PASSWORD="example"

# Export the password so psql can use it
export PGPASSWORD=$DB_PASSWORD

# Infinite loop to run the SQL script every 5 seconds
while true; do
    # Run the SQL script using psql
    psql -h $DB_HOST -U $DB_USER -d $DB_NAME -f /app/update_occupancy.sql

    # Sleep for 5 seconds
    sleep 5
done