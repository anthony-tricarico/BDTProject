from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col, expr, to_timestamp, count, when, window, coalesce, lit, avg, sum
import time

# === Configuration ===
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPICS = ["sensors.topic", "ticketing.topic"]  # Topics for sensor and ticket data
CHECKPOINT_LOCATION = "/tmp/checkpoint"  # Location to store offset information
WATERMARK_DELAY = "1 hour"  # How long to wait for late data
WINDOW_DURATION = "5 minutes"  # Increased window size for aggregation
SLIDE_DURATION = "1 minute"  # How often to update the window

# PostgreSQL configuration
POSTGRES_URL = "jdbc:postgresql://db:5432/raw_data"
POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "example",
    "driver": "org.postgresql.Driver"
}
POSTGRES_TABLE = "bus_passenger_summary"

# Schema definitions
TICKET_SCHEMA = StructType([
    StructField("ticket_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("stop_id", StringType(), True),
    StructField("route", StringType(), True),
    StructField("passenger_type", StringType(), True),
    StructField("fare", DoubleType(), True),
    StructField("bus_id", IntegerType(), True),
    StructField("trip_id", StringType(), True)
])

SENSOR_SCHEMA = StructType([
    StructField("measurement_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("stop_id", StringType(), True),
    StructField("route", StringType(), True),
    StructField("status", IntegerType(), True),
    StructField("activation_type", IntegerType(), True),
    StructField("bus_id", IntegerType(), True),
    StructField("trip_id", StringType(), True)
])

def create_spark_session():
    """Initialize Spark Session with necessary configurations"""
    try:
        # Configure logging level
        import logging
        logging.getLogger("org.apache.spark").setLevel(logging.ERROR)
        logging.getLogger("org.apache.kafka").setLevel(logging.ERROR)
        
        return SparkSession.builder \
            .appName("KafkaToPostgresStreaming") \
            .config("spark.jars.packages", ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",  # Kafka connector
                "org.postgresql:postgresql:42.7.1"                    # PostgreSQL driver
            ])) \
            .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.hadoop.fs.defaultFS", "file:///") \
            .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
            .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
            .getOrCreate()
    except Exception as e:
        print(f"Failed to create Spark session: {str(e)}")
        raise

def read_kafka_stream(spark, topic, schema):
    """Read from Kafka with deduplication handling"""
    try:
        kafka_options = {
            "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "subscribe": topic,
            "startingOffsets": "earliest",
            "failOnDataLoss": "false",
            "maxOffsetsPerTrigger": "10000",
            # Kafka security and performance tuning
            "kafka.security.protocol": "PLAINTEXT",
            "fetchOffset.numRetries": "5",
            "kafka.request.timeout.ms": "120000",
            "kafka.session.timeout.ms": "60000"
        }
        
        print(f"\nDEBUG: Connecting to Kafka topic {topic} with options:")
        print(kafka_options)
        
        # Read from Kafka and handle deserialization explicitly
        raw_stream = spark.readStream \
            .format("kafka") \
            .options(**kafka_options) \
            .load()
        
        # Show raw Kafka data for debugging
        print(f"\nDEBUG: Raw Kafka stream schema for {topic}:")
        raw_stream.printSchema()
        
        # Deserialize the value column and parse JSON
        parsed_stream = raw_stream \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json("value", schema).alias("data")) \
            .select("data.*")
            
        print(f"\nDEBUG: Parsed stream schema for {topic}:")
        parsed_stream.printSchema()
        
        # Print parsed data for debugging
        parsed_stream.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .start()
            
        # Add timestamp conversion
        timestamped_stream = parsed_stream \
            .withColumn("timestamp", to_timestamp("timestamp"))
            
        print(f"\nDEBUG: Final stream schema for {topic} (after timestamp conversion):")
        timestamped_stream.printSchema()

        # Use measurement_id for sensors and ticket_id for tickets as deduplication keys
        dedup_key = "measurement_id" if "measurement_id" in schema.fieldNames() else "ticket_id"
        final_stream = timestamped_stream.dropDuplicates([dedup_key])
        
        return final_stream
            
    except Exception as e:
        print(f"Failed to read from Kafka topic {topic}: {str(e)}")
        if "Connection refused" in str(e):
            print(f"Could not connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS}. Is Kafka running?")
        raise

def process_stream(spark):
    """Process streaming data from Kafka and write to PostgreSQL"""
    try:
        # Read sensor data from Kafka with deduplication
        try:
            print("\nDEBUG: Attempting to connect to sensor topic...")
            sensors_df = read_kafka_stream(spark, KAFKA_TOPICS[0], SENSOR_SCHEMA)
            print(f"Successfully connected to sensor topic: {KAFKA_TOPICS[0]}")
        except Exception as e:
            print(f"Failed to read from sensor topic: {str(e)}")
            raise

        # Read ticket data from Kafka with deduplication
        try:
            print("\nDEBUG: Attempting to connect to ticket topic...")
            tickets_df = read_kafka_stream(spark, KAFKA_TOPICS[1], TICKET_SCHEMA)
            print(f"Successfully connected to ticket topic: {KAFKA_TOPICS[1]}")
        except Exception as e:
            print(f"Failed to read from ticket topic: {str(e)}")
            raise

        print("\nDEBUG: Window configuration:")
        print(f"Window duration: {WINDOW_DURATION}")
        print(f"Slide duration: {SLIDE_DURATION}")
        print(f"Watermark delay: {WATERMARK_DELAY}")

        # Add watermark to handle late data
        sensors_df = sensors_df.withWatermark("timestamp", WATERMARK_DELAY)
        tickets_df = tickets_df.withWatermark("timestamp", WATERMARK_DELAY)

        # Group sensor data by window and required fields
        sensor_counts = sensors_df \
            .withColumn("event_time", col("timestamp")) \
            .withColumn("window", 
                window(col("timestamp"), WINDOW_DURATION, SLIDE_DURATION)) \
            .groupBy("window", "stop_id", "route", "bus_id", "trip_id") \
            .agg(
                count(when(col("status") == 1, True)).alias("passengers_off")
            )

        # Group ticket data by window and required fields
        ticket_counts = tickets_df \
            .withColumn("window", 
                window(col("timestamp"), WINDOW_DURATION, SLIDE_DURATION)) \
            .groupBy("window", "stop_id", "route", "bus_id", "trip_id") \
            .agg(
                count("*").alias("passengers_on"),
                sum("fare").alias("total_fare"),
                avg("fare").alias("avg_fare")
            )

        # Join the aggregated streams
        joined_df = sensor_counts.join(
            ticket_counts,
            ["window", "stop_id", "route", "bus_id", "trip_id"],
            "inner"
        )

        # Final selection and processing
        final_df = joined_df \
            .select(
                "stop_id",
                "route",
                "bus_id",
                "trip_id",
                col("window.end").alias("timestamp"),
                "passengers_on",
                "passengers_off",
                "total_fare",
                "avg_fare"
            ) \
            .withColumn("occupancy_change", 
                col("passengers_on") - col("passengers_off"))

        # Function to write to PostgreSQL
        def write_to_postgres(batch_df, batch_id):
            if batch_df.isEmpty():
                print(f"Batch {batch_id}: No new data to process")
                return

            try:
                print(f"\nDEBUG: Processing batch {batch_id}")
                row_count = batch_df.count()
                print(f"Number of records: {row_count}")
                
                if row_count > 0:
                    print("\nSample of data to be written to PostgreSQL:")
                    batch_df.show(5, truncate=False)
                    
                    print(f"\nWriting {row_count} records to PostgreSQL...")
                    print(f"Target table: {POSTGRES_TABLE}")
                    
                    batch_df.write \
                        .mode("append") \
                        .jdbc(
                            url=POSTGRES_URL,
                            table=POSTGRES_TABLE,
                            properties=POSTGRES_PROPERTIES
                        )
                    print(f"Successfully wrote batch {batch_id} to PostgreSQL")
                    
                    # Verify write by counting records
                    try:
                        count_query = f"(SELECT COUNT(*) as count FROM {POSTGRES_TABLE}) as verify"
                        result = spark.read.jdbc(
                            url=POSTGRES_URL,
                            table=count_query,
                            properties=POSTGRES_PROPERTIES
                        ).collect()[0][0]
                        print(f"Total records in table after write: {result}")
                    except Exception as e:
                        print(f"Warning: Could not verify record count: {str(e)}")
            except Exception as e:
                print(f"\nERROR: Failed to write batch {batch_id} to PostgreSQL:")
                print(f"Error type: {type(e).__name__}")
                print(f"Error message: {str(e)}")
                raise

        # Start the streaming query
        query = final_df.writeStream \
            .foreachBatch(write_to_postgres) \
            .outputMode("append") \
            .option("checkpointLocation", CHECKPOINT_LOCATION) \
            .trigger(processingTime="1 minute") \
            .start()

        print("\nDEBUG: Streaming query started successfully")
        return query

    except Exception as e:
        print(f"Error in stream processing: {str(e)}")
        raise

def test_postgres_connection(spark):
    """Test PostgreSQL connection before starting the streaming process"""
    try:
        # Create a small test DataFrame
        test_df = spark.createDataFrame([(1,)], ["test"])
        
        print("\nDEBUG: Testing PostgreSQL connection...")
        print(f"PostgreSQL URL: {POSTGRES_URL}")
        print(f"Database: {POSTGRES_URL.split('/')[-1]}")
        print("Connection properties:", POSTGRES_PROPERTIES)
        
        # Try to write and immediately delete test data
        test_df.write \
            .mode("append") \
            .jdbc(
                url=POSTGRES_URL,
                table="connection_test",
                properties=POSTGRES_PROPERTIES
            )
        
        # If we get here, connection was successful
        print("Successfully connected to PostgreSQL!")
        
        # Clean up test table
        spark.read.jdbc(
            url=POSTGRES_URL,
            table="(SELECT 1) AS cleanup",
            properties=POSTGRES_PROPERTIES
        )
        
        return True
    except Exception as e:
        print(f"\nERROR: Failed to connect to PostgreSQL:")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        print("\nPlease verify:")
        print("1. PostgreSQL container is running")
        print("2. Database 'raw_data' exists")
        print("3. User 'postgres' has required permissions")
        print("4. Password is correct")
        print("5. Network connection between containers is working")
        return False

def main():
    """Main function to run the streaming job"""
    spark = None
    query = None
    
    try:
        # Set log levels
        import logging
        logging.getLogger("org").setLevel(logging.ERROR)
        logging.getLogger("akka").setLevel(logging.ERROR)
        
        # Create Spark session only if it doesn't exist
        spark = SparkSession.getActiveSession()
        if not spark:
            spark = create_spark_session()
            # Set Spark log level after session creation
            spark.sparkContext.setLogLevel("ERROR")
            print("Successfully created new Spark session")
        else:
            print("Using existing Spark session")
        
        # Test PostgreSQL connection before starting
        if not test_postgres_connection(spark):
            print("ERROR: Cannot proceed without PostgreSQL connection")
            return
        
        try:
            # Start the streaming process
            query = process_stream(spark)
            print("Successfully started streaming query")
            
            # Wait for the streaming to finish
            query.awaitTermination()
        except Exception as e:
            print(f"Error in streaming process: {str(e)}")
            if query:
                try:
                    query.stop()
                    print("Streaming query stopped")
                except:
                    pass
            raise
    except Exception as e:
        print(f"Fatal error in main function: {str(e)}")
        raise
    finally:
        # Clean up resources
        if query:
            try:
                query.stop()
                print("Streaming query stopped")
            except:
                pass
        if spark:
            try:
                spark.stop()
                print("Spark session stopped")
            except:
                pass

if __name__ == "__main__":
    # time.sleep(10)  # Commented out as it's not needed
    main() 