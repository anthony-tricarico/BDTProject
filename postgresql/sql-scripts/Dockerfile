FROM ubuntu:20.04

# Copy SQL scripts and the shell script into the container
WORKDIR /app

# COPY sql-scripts/ sql-scripts/
# COPY run_sql.sh .
COPY . .

# Install PostgreSQL client
RUN apt-get update && \
    apt-get install -y postgresql-client

# Give execute permissions to the shell script
RUN chmod +x run_sql.sh

# Run the shell script on container startup
CMD ["./run_sql.sh"]