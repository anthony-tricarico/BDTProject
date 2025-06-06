from kafka import KafkaConsumer
import psycopg2
import json
from datetime import datetime
import time
import logging
import sys

# Set up logging to both file and console
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG level to get all messages
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),  # Print to console
        logging.FileHandler('weather_consumer.log')  # Save to file
    ]
)
logger = logging.getLogger(__name__)

print("Weather consumer starting...")  # Direct print for immediate feedback

def verify_database_setup(max_retries=30, retry_delay=10):
    """Verify database and table existence with retries."""
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            print(f"Attempting database connection (attempt {retry_count + 1}/{max_retries})...")
            test_conn = psycopg2.connect(
                dbname='raw_data',
                user='postgres',
                password='example',
                host='db'
            )
            test_cur = test_conn.cursor()
            
            # Check if weather table exists
            test_cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'weather'
                );
            """)
            table_exists = test_cur.fetchone()[0]
            
            if not table_exists:
                print("Weather table does not exist, creating it...")
                logger.info("Creating weather table")
                test_cur.execute("""
                    CREATE TABLE IF NOT EXISTS weather (
                        measurement_id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMP,
                        latitude FLOAT,
                        longitude FLOAT,
                        weather_code INTEGER,
                        precipitation_probability FLOAT,
                        temperature FLOAT,
                        hour TIMESTAMP
                    );
                """)
                test_conn.commit()
                print("Weather table created successfully")
            
            # Test insert
            test_cur.execute("""
                INSERT INTO weather (timestamp, latitude, longitude, weather_code, 
                                   precipitation_probability, temperature, hour)
                VALUES (NOW(), 0, 0, 0, 0, 0, NOW())
                RETURNING measurement_id;
            """)
            test_id = test_cur.fetchone()[0]
            test_conn.commit()
            
            # Test select
            test_cur.execute("SELECT * FROM weather WHERE measurement_id = %s", (test_id,))
            test_result = test_cur.fetchone()
            
            # Clean up test data
            test_cur.execute("DELETE FROM weather WHERE measurement_id = %s", (test_id,))
            test_conn.commit()
            
            test_cur.close()
            test_conn.close()
            
            print("Database and table verified successfully")
            return True
            
        except Exception as e:
            retry_count += 1
            print(f"Database verification attempt {retry_count}/{max_retries} failed: {e}")
            logger.error(f"Database verification attempt {retry_count}/{max_retries} failed: {e}")
            
            if retry_count < max_retries:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Maximum retry attempts reached. Could not verify database setup.")
                return False

# Verify database setup before starting
print("Starting database verification...")
if not verify_database_setup():
    print("Failed to verify database setup after maximum retries. Exiting...")
    sys.exit(1)

print("Attempting to connect to Kafka...")

# Kafka consumer setup with more detailed error reporting
retry_count = 0
max_retries = 30  # 5 minutes of retries
consumer = None
while consumer is None and retry_count < max_retries:
    try:
        consumer = KafkaConsumer(
            'weather.topic',
            bootstrap_servers='kafka:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id='weather_consumer_group',  # Add consumer group
            session_timeout_ms=30000,  # 30 seconds
            heartbeat_interval_ms=10000  # 10 seconds
        )
        print("Successfully connected to Kafka")
        logger.info("Successfully connected to Kafka")
        
        # Test if we can actually list topics
        topics = consumer.topics()
        print(f"Available topics: {topics}")
        if 'weather.topic' not in topics:
            print("Warning: weather.topic not found in available topics")
            logger.warning("weather.topic not found in available topics")
        
    except Exception as e:
        retry_count += 1
        print(f"Kafka connection attempt {retry_count}/{max_retries} failed: {e}")
        logger.error(f"Kafka connection attempt {retry_count}/{max_retries} failed: {e}")
        time.sleep(10)  # Wait 10 seconds between retries

if consumer is None:
    print("Failed to connect to Kafka after maximum retries. Exiting...")
    sys.exit(1)

# PostgreSQL connection setup
def get_db_connection():
    conn = None
    retry_count = 0
    max_retries = 5
    
    while conn is None and retry_count < max_retries:
        try:
            conn = psycopg2.connect(
                dbname='raw_data',
                user='postgres',
                password='example',
                host='db'
            )
            print("Successfully connected to PostgreSQL")
            logger.info("Successfully connected to PostgreSQL")
            return conn
        except Exception as e:
            retry_count += 1
            print(f"Database connection attempt {retry_count}/{max_retries} failed: {e}")
            logger.error(f"Database connection attempt {retry_count}/{max_retries} failed: {e}")
            if retry_count < max_retries:
                time.sleep(5)
    
    if conn is None:
        raise Exception("Failed to connect to database after maximum retries")

conn = get_db_connection()
cur = conn.cursor()

def validate_weather_data(weather_data):
    """Validate the weather data structure and content."""
    required_fields = ['timestamp', 'latitude', 'longitude', 'hour', 
                      'temperature', 'precipitation_probability', 'weather_code']
    
    # Check if all required fields exist
    for field in required_fields:
        if field not in weather_data:
            raise ValueError(f"Missing required field: {field}")
    
    # Check if arrays have the same length
    array_fields = ['hour', 'temperature', 'precipitation_probability', 'weather_code']
    array_lengths = [len(weather_data[field]) for field in array_fields]
    if len(set(array_lengths)) != 1:
        raise ValueError("Array fields must have the same length")
    
    # Validate data types and ranges
    if not isinstance(weather_data['latitude'], (int, float)):
        raise ValueError("Latitude must be a number")
    if not isinstance(weather_data['longitude'], (int, float)):
        raise ValueError("Longitude must be a number")
    
    # Validate arrays
    for temp in weather_data['temperature']:
        if not isinstance(temp, (int, float)):
            raise ValueError("Temperature values must be numbers")
    
    for prob in weather_data['precipitation_probability']:
        if not isinstance(prob, (int, float)) or prob < 0 or prob > 100:
            raise ValueError("Precipitation probability must be between 0 and 100")
    
    return True

def verify_data_written(cur, timestamp):
    """Verify that data was written to the database."""
    try:
        cur.execute("SELECT COUNT(*) FROM weather WHERE timestamp = %s", (timestamp,))
        count = cur.fetchone()[0]
        return count > 0
    except Exception as e:
        logger.error(f"Error verifying data write: {e}")
        return False

def update_weather_table(weather_data):
    """Update weather table with new data and verify the update."""
    try:
        # Validate the incoming data
        validate_weather_data(weather_data)
        print(f"Processing weather data for timestamp: {weather_data['timestamp']}")
        logger.info(f"Processing weather data for timestamp: {weather_data['timestamp']}")

        timestamp = weather_data['timestamp']
        latitude = weather_data['latitude']
        longitude = weather_data['longitude']
        hours = weather_data['hour']
        temperatures = weather_data['temperature']
        precipitation_probabilities = weather_data['precipitation_probability']
        weather_codes = weather_data['weather_code']

        # Begin transaction
        cur.execute("BEGIN")
        
        # Clear the table before inserting new data
        cur.execute("TRUNCATE TABLE weather")
        print("Cleared existing weather data")

        # Insert new data
        inserted_count = 0
        for hour, temperature, precipitation_probability, weather_code in zip(
            hours, temperatures, precipitation_probabilities, weather_codes):
            cur.execute("""
                INSERT INTO weather (timestamp, latitude, longitude, weather_code, 
                                   precipitation_probability, temperature, hour)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                timestamp,
                latitude,
                longitude,
                weather_code,
                precipitation_probability,
                temperature,
                hour
            ))
            inserted_count += 1
            if inserted_count % 10 == 0:  # Log progress every 10 records
                print(f"Inserted {inserted_count} records...")

        # Verify the data was written
        if verify_data_written(cur, timestamp):
            conn.commit()
            print(f"Successfully inserted {inserted_count} weather records")
            logger.info(f"Successfully inserted {inserted_count} weather records")
        else:
            conn.rollback()
            print("Failed to verify data write, rolling back transaction")
            logger.error("Failed to verify data write, rolling back transaction")
            raise Exception("Data verification failed")

    except Exception as e:
        conn.rollback()
        print(f"Error updating weather table: {e}")
        logger.error(f"Error updating weather table: {e}")
        raise

# Main loop
print("Starting main consumer loop...")
try:
    logger.info("Starting weather consumer...")
    print("Waiting for messages...")
    
    # Test if we're actually getting messages
    for message in consumer:
        print(f"Received message: {message.value}")
        try:
            weather_data = message.value
            update_weather_table(weather_data)
            consumer.commit()
            print("Message processed and committed successfully")
        except Exception as e:
            print(f"Error processing message: {e}")
            logger.error(f"Error processing message: {e}")
            continue

except KeyboardInterrupt:
    print("Shutting down weather consumer...")
    logger.info("Shutting down weather consumer...")
except Exception as e:
    print(f"Unexpected error in main loop: {e}")
    logger.error(f"Unexpected error in main loop: {e}")
finally:
    # Close connections
    try:
        cur.close()
        conn.close()
        print("Database connections closed")
    except Exception as e:
        print(f"Error closing database connections: {e}")
    
    try:
        consumer.close()
        print("Kafka consumer closed")
    except Exception as e:
        print(f"Error closing Kafka consumer: {e}")

print("Weather consumer shutdown complete")