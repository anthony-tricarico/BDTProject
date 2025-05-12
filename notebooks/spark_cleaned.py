import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pymongo import MongoClient

# MongoDB direct connection check - verify MongoDB is accessible
try:
    client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=5000)
    # print("Available MongoDB databases:", client.list_database_names())
except Exception as e:
    print("MongoDB connection error:", e)

mongodb_connector = "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"

# IMPORTANT: Stop any existing SparkSession
SparkSession.builder.getOrCreate().stop()

# Create a fresh Spark session with explicit packages
# Adding clearer configuration for MongoDB
spark = SparkSession.builder \
    .appName("MongoSparkStreamingIntegration") \
    .config("spark.jars.packages", mongodb_connector) \
    .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/raw.passengers") \
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/raw.passengers") \
    .getOrCreate()

try:
    # Try batch read with more explicit options
    mongo_df = spark.read \
        .format("mongodb") \
        .option("database", "raw") \
        .option("collection", "passengers") \
        .load(), 
    
    # .option("spark.mongodb.read.database", "raw") \
        # .option("spark.mongodb.read.collection", "passengers") \
    print("MongoDB batch read schema:")
    # mongo_df.printSchema()
    print("Sample data:")
    # Use take() instead of show() to avoid potential console formatting issues
    sample_data = mongo_df#.collect()
    for row in sample_data:
        print(row)
    
    # If we get here, setup streaming with MongoDB batch reads
    rate_stream = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 1) \
        .load()
    
    def process_batch(batch_df, batch_id):
        print(f"Processing batch {batch_id}")
        
        try:
            # Read the current data from MongoDB
            current_data = spark.read \
                .format("mongodb") \
                .option("spark.mongodb.read.connection.uri", "mongodb://localhost:27017") \
                .option("spark.mongodb.read.database", "raw") \
                .option("spark.mongodb.read.collection", "passengers") \
                .load()
            
            if current_data.count() > 0:
                sample = current_data.select("bus_id", "predicted_passengers_in", "route", "stop_sequence") \
                    .withColumn("timestamp", current_timestamp()) \
                    .limit(10).collect()
                
                print(f"Retrieved {len(sample)} rows from MongoDB")
                for row in sample:
                    print(row)
            else:
                print("No data found in MongoDB")
        except Exception as e:
            print(f"Error in batch processing: {e}")
    
    # Start the streaming query with corrected parameter name
    query = rate_stream.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .trigger(processingTime="5 seconds") \
        .start()
    
    print("Streaming query started with MongoDB connector. Waiting for termination...")
    # Fix the parameter name from 'timeoutMs' to 'timeout'
    query.awaitTermination(timeout=60) # 60 seconds timeout
    
except Exception as e:
    print(f"Error with MongoDB connector approach: {e}")
    print("Falling back to direct PyMongo approach...")

# Clean shutdown
print("Shutting down Spark session...")
spark.stop()