import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pymongo import MongoClient

# MongoDB connection parameters
MONGO_URI = "mongodb://localhost:27017"
DATABASE = "raw"
COLLECTIONS = ["passengers", "sensors"]  # Add more collections as needed
POLL_INTERVAL_SECONDS = 5
MAX_ITERATIONS = 12  # Poll for 1 minute (12 * 5s)

# Step 1: Verify MongoDB connection
try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    client.server_info()  # Forces a call to test connection
    print("Connected to MongoDB.")
except Exception as e:
    print(f"MongoDB connection error: {e}")
    exit(1)

# Step 2: Initialize SparkSession with MongoDB connector
spark = SparkSession.builder \
    .appName("MongoDBPollingAggregation") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0") \
    .getOrCreate()

# Helper: Read a collection into a DataFrame
def read_collection(collection_name):
    try:
        df = spark.read.format("mongodb") \
            .option("spark.mongodb.read.connection.uri", MONGO_URI) \
            .option("spark.mongodb.read.database", DATABASE) \
            .option("spark.mongodb.read.collection", collection_name) \
            .load()
        print(f"✅ Successfully read collection '{collection_name}'")
        return df
    except Exception as e:
        print(f"❌ Failed to read collection '{collection_name}': {e}")
        return None

# Step 3: Polling loop (simulate streaming)
for i in range(MAX_ITERATIONS):
    print(f"\n--- Iteration {i + 1} ---")

    try:
        # Read and combine collections
        dfs = [read_collection(col_name) for col_name in COLLECTIONS]
        dfs = [df for df in dfs if df is not None]

        if not dfs:
            print("No data loaded from any collection.")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        # Combine all collections
        combined_df = dfs[0]
        for df in dfs[1:]:
            combined_df = combined_df.unionByName(df, allowMissingColumns=True)

        # Perform aggregation: count of observations grouped by trip_id and stop_id
        aggregated_df = combined_df.groupBy("stop_id", "route", "timestamp", "trip_id").agg(
            count("*").alias("passenger_count")
        )

        print("🚌 Aggregated passenger counts:")
        aggregated_df.show(100)
    except Exception as e:
        print(f"❌ Error during aggregation: {e}")

    time.sleep(POLL_INTERVAL_SECONDS)

# Cleanup
spark.stop()
print("✅ Spark session stopped.")
