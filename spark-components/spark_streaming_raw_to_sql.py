from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col, to_timestamp
import time

# === Configuration ===
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPICS = ["sensors.topic", "ticketing.topic"]  # Topics for sensor and ticket data
CHECKPOINT_LOCATION = "/tmp/checkpoint"  # Location to store offset information

# PostgreSQL configuration
POSTGRES_URL = "jdbc:postgresql://db:5432/raw_data"
POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "example",
    "driver": "org.postgresql.Driver"
}

# Schema definitions
TICKET_SCHEMA = StructType([
    StructField("ticket_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("stop_id", StringType(), True),
    StructField("route", StringType(), True),
    StructField("passenger_type", StringType(), True),
    StructField("fare", DoubleType(), True),
    StructField("bus_id", IntegerType(), True),
    StructField("trip_id", StringType(), True),
    StructField("peak_hour", IntegerType(), True),
    # StructField("event", StringType(), True),
    StructField("hospital", IntegerType(), True), 
    StructField("school", IntegerType(), True)
])

SENSOR_SCHEMA = StructType([
    StructField("measurement_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("stop_id", StringType(), True),
    StructField("route", StringType(), True),
    StructField("status", IntegerType(), True),
    StructField("activation_type", IntegerType(), True),
    StructField("bus_id", IntegerType(), True),
    StructField("trip_id", StringType(), True),
    StructField("peak_hour", IntegerType(), True),
    # StructField("event", StringType(), True),
    StructField("hospital", IntegerType(), True), 
    StructField("school", IntegerType(), True)
])

def create_spark_session():
    """Initialize Spark Session with necessary configurations"""
    try:
        import logging
        logging.getLogger("org.apache.spark").setLevel(logging.ERROR)
        logging.getLogger("org.apache.kafka").setLevel(logging.ERROR)
        
        return SparkSession.builder \
            .appName("KafkaToPostgresRawStreaming") \
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
    """Read from Kafka and parse JSON data returning a timestamped stream"""
    try:
        kafka_options = {
            "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "subscribe": topic,
            "startingOffsets": "earliest",
            "failOnDataLoss": "false",
            "maxOffsetsPerTrigger": "10000",
            "kafka.security.protocol": "PLAINTEXT",
            "fetchOffset.numRetries": "5",
            "kafka.request.timeout.ms": "120000",
            "kafka.session.timeout.ms": "60000"
        }
        
        # print(f"\nDEBUG: Connecting to Kafka topic {topic} with options:")
        # print(kafka_options)
        
        raw_stream = spark.readStream \
            .format("kafka") \
            .options(**kafka_options) \
            .load()
        
        # print(f"\nDEBUG: Raw Kafka stream schema for {topic}:")
        # raw_stream.printSchema()
        
        parsed_stream = raw_stream \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json("value", schema).alias("data")) \
            .select("data.*")
        
        # print(f"\nDEBUG: Parsed stream schema for {topic}:")
        # parsed_stream.printSchema()
        
        timestamped_stream = parsed_stream \
            .withColumn("timestamp", to_timestamp("timestamp"))
        
        return timestamped_stream
    except Exception as e:
        print(f"Failed to read from Kafka topic {topic}: {str(e)}")
        if "Connection refused" in str(e):
            print(f"Could not connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS}. Is Kafka running?")
        raise

def write_to_postgres(batch_df, batch_id, table_name):
    """Write batch to PostgreSQL"""
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: No new data to process")
        return

    try:
        # print(f"\nDEBUG: Processing batch {batch_id}")
        row_count = batch_df.count()
        print(f"Number of records: {row_count}")
        
        if row_count > 0:
            print("\nSample of data to be written to PostgreSQL:")
            batch_df.show(5, truncate=False)
            
            print(f"\nWriting {row_count} records to PostgreSQL table {table_name}...")
            
            batch_df.write \
                .mode("append") \
                .jdbc(
                    url=POSTGRES_URL,
                    table=table_name,
                    properties=POSTGRES_PROPERTIES
                )
            print(f"Successfully wrote batch {batch_id} to PostgreSQL table {table_name}")
    except Exception as e:
        print(f"\nERROR: Failed to write batch {batch_id} to PostgreSQL table {table_name}:")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        raise

def process_stream(spark):
    """Process streaming data from Kafka and write raw data to PostgreSQL"""
    try:
        # Read sensor data from Kafka
        sensors_df = read_kafka_stream(spark, KAFKA_TOPICS[0], SENSOR_SCHEMA)
        
        # Read ticket data from Kafka
        tickets_df = read_kafka_stream(spark, KAFKA_TOPICS[1], TICKET_SCHEMA)

        # Write sensor data to PostgreSQL
        sensor_query = sensors_df.writeStream \
            .foreachBatch(lambda df, epochId: write_to_postgres(df, epochId, "raw_sensors")) \
            .outputMode("append") \
            .option("checkpointLocation", CHECKPOINT_LOCATION + "/sensors") \
            .start()

        # Write ticket data to PostgreSQL
        ticket_query = tickets_df.writeStream \
            .foreachBatch(lambda df, epochId: write_to_postgres(df, epochId, "raw_tickets")) \
            .outputMode("append") \
            .option("checkpointLocation", CHECKPOINT_LOCATION + "/tickets") \
            .start()

        print("\nDEBUG: Streaming queries started successfully")
        return sensor_query, ticket_query

    except Exception as e:
        print(f"Error in stream processing: {str(e)}")
        raise

def main():
    """Main function to run the streaming job"""
    spark = None
    sensor_query = None
    ticket_query = None
    
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
        # if not test_postgres_connection(spark):
        #     print("ERROR: Cannot proceed without PostgreSQL connection")
        #     return
        
        try:
            # Start the streaming process
            sensor_query, ticket_query = process_stream(spark)
            print("Successfully started streaming queries")
            
            # Wait for the streaming to finish
            sensor_query.awaitTermination()
            ticket_query.awaitTermination()
        except Exception as e:
            print(f"Error in streaming process: {str(e)}")
            if sensor_query:
                try:
                    sensor_query.stop()
                    print("Sensor streaming query stopped")
                except:
                    pass
            if ticket_query:
                try:
                    ticket_query.stop()
                    print("Ticket streaming query stopped")
                except:
                    pass
            raise
    except Exception as e:
        print(f"Fatal error in main function: {str(e)}")
        raise
    finally:
        # Clean up resources
        if sensor_query:
            try:
                sensor_query.stop()
                print("Sensor streaming query stopped")
            except:
                pass
        if ticket_query:
            try:
                ticket_query.stop()
                print("Ticket streaming query stopped")
            except:
                pass
        if spark:
            try:
                spark.stop()
                print("Spark session stopped")
            except:
                pass

if __name__ == "__main__":
    main() 