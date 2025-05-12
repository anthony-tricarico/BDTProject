import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pymongo import MongoClient
import os

# Print Spark version for debugging
print("PySpark version:", pyspark.__version__)

# Get your Spark version
spark_version = pyspark.__version__

# MongoDB direct connection check - verify MongoDB is accessible
try:
    client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=5000)
    # print("Available MongoDB databases:", client.list_database_names())
except Exception as e:
    print("MongoDB connection error:", e)

# Determine the correct MongoDB connector version for your Spark version
# Using a different approach based on the error message
# if spark_version.startswith('3.5'):
#     # For Spark 3.5, we need to explicitly use a compatible connector
#     # The error suggests compatibility issues with the connector selection logic
#     mongodb_connector = "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"
# elif spark_version.startswith('3.4'):
#     mongodb_connector = "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"
# elif spark_version.startswith('3.3'):
#     mongodb_connector = "org.mongodb.spark:mongo-spark-connector_2.12:10.1.0"
# elif spark_version.startswith('3.2'):
#     mongodb_connector = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2"
# else:
#     # Default for older versions
#     mongodb_connector = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"

mongodb_connector = "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"
# print(f"Using MongoDB connector: {mongodb_connector}")

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

# spark.sparkContext.setLogLevel("INFO")

# Print diagnostic information
# print("\n===== SPARK CONFIGURATION =====")
# print(f"Spark version: {spark.version}")
# print(f"Scala version: {spark.sparkContext._jvm.scala.util.Properties.versionString()}")
# print(f"Master: {spark.sparkContext.master}")

# Try to read from MongoDB directly through PyMongo first
# print("\n===== DIRECT PYMONGO CONNECTION TEST =====")
# try:
#     client = MongoClient("mongodb://localhost:27017")
#     db = client["raw"]
#     collection = db["passengers"]
    
#     sample_docs = list(collection.find({}).limit(2))
#     if sample_docs:
#         print(f"Successfully read {len(sample_docs)} documents from MongoDB using PyMongo")
#         print("Sample document:", sample_docs[0])
#     else:
#         print("No documents found in MongoDB collection")
# except Exception as e:
#     print(f"PyMongo direct connection failed: {e}")

# Try the Spark-MongoDB connector approach with better error handling
print("\n===== TRYING SPARK MONGODB CONNECTOR =====")
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
    
    # # Fallback to PyMongo direct approach
    # rate_stream = spark.readStream \
    #     .format("rate") \
    #     .option("rowsPerSecond", 1) \
    #     .load()
    
    # def fallback_process_batch(batch_df, batch_id):
    #     print(f"Processing batch {batch_id} (fallback method)")
        
    #     try:
    #         client = MongoClient("mongodb://localhost:27017")
    #         db = client["raw"]
    #         collection = db["passengers"]
            
    #         docs = list(collection.find({}, {"bus_id": 1, "predicted_passengers_in": 1, "_id": 0}).limit(10))
            
    #         if docs:
    #             print(f"Retrieved {len(docs)} documents directly from MongoDB:")
    #             for doc in docs:
    #                 print(doc)
    #         else:
    #             print("No data found in MongoDB")
    #     except Exception as e:
    #         print(f"Error in fallback batch processing: {e}")
    
    # query = rate_stream.writeStream \
    #     .foreachBatch(fallback_process_batch) \
    #     .outputMode("update") \
    #     .trigger(processingTime="5 seconds") \
    #     .start()
    
    # print("Fallback streaming query started. Waiting for termination...")
    # # Use 'timeout' parameter instead of 'timeoutMs'
    # query.awaitTermination(timeout=60) # 60 seconds timeout

# Clean shutdown
print("Shutting down Spark session...")
spark.stop()