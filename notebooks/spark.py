import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pymongo import MongoClient

# === Configuration ===

# MongoDB connection parameters
MONGO_URI = "mongodb://localhost:27017"
DATABASE = "raw"
SENSOR_COLLECTION = "sensors"
TICKET_COLLECTION = "tickets"
POLL_INTERVAL_SECONDS = 5
MAX_ITERATIONS = 12  # e.g., 1 minute of polling if interval = 5s

# PostgreSQL connection parameters (adjust as needed)
POSTGRES_URL = "jdbc:postgresql://localhost:5432/raw_data"
POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "example",
    "driver": "org.postgresql.Driver"
}
POSTGRES_TABLE = "bus_passenger_summary"

# === Step 1: Verify MongoDB connection ===
try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    client.server_info()
    print("✅ Connected to MongoDB.")
except Exception as e:
    print(f"❌ MongoDB connection error: {e}")
    exit(1)

# === Step 2: Initialize SparkSession with MongoDB connector ===
spark = SparkSession.builder \
    .appName("MongoToPostgresAggregation") \
    .config("spark.jars.packages", ",".join([
        "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0",
        "org.postgresql:postgresql:42.7.1"
    ])) \
    .getOrCreate()

# === Helper: Read MongoDB collection into Spark DataFrame ===
def read_collection(collection_name):
    try:
        df = spark.read.format("mongodb") \
            .option("spark.mongodb.read.connection.uri", MONGO_URI) \
            .option("spark.mongodb.read.database", DATABASE) \
            .option("spark.mongodb.read.collection", collection_name) \
            .load()
        print(f"✅ Loaded '{collection_name}' collection.")
        return df
    except Exception as e:
        print(f"❌ Failed to load '{collection_name}': {e}")
        return None

# === Helper: Read existing data from PostgreSQL to avoid duplicates ===
def read_postgres_data():
    try:
        df = spark.read.jdbc(url=POSTGRES_URL, 
                             table=POSTGRES_TABLE, 
                             properties=POSTGRES_PROPERTIES)
        print(f"✅ Loaded existing data from PostgreSQL.")
        return df
    except Exception as e:
        print(f"❌ Failed to read PostgreSQL data: {e}")
        return None

# === Step 3: Polling Loop for Processing Data ===
for i in range(MAX_ITERATIONS):
    print(f"\n🔄 --- Iteration {i + 1} ---")

    # Load both collections
    sensors_df = read_collection(SENSOR_COLLECTION)
    tickets_df = read_collection(TICKET_COLLECTION)

    # If either collection failed to load, skip iteration
    if sensors_df is None or tickets_df is None:
        print("⚠️ One or both collections failed to load. Retrying...")
        time.sleep(POLL_INTERVAL_SECONDS)
        continue

    try:
        # === Step 4: Aggregation ===

        # Group sensor data to count "getting off" passengers
        sensor_agg = sensors_df.groupBy("bus_id", "stop_id", "route", "timestamp", "trip_id") \
            .agg(count("*").alias("passengers_off"))

        # Group ticket data to count "getting on" passengers
        ticket_agg = tickets_df.groupBy("bus_id", "stop_id", "route", "timestamp", "trip_id") \
            .agg(count("*").alias("passengers_on"))

        # === Step 5: Join both aggregations ===
        summary_df = sensor_agg.join(ticket_agg,
                                     on=["stop_id", "route", "timestamp", "trip_id"],
                                     how="outer").fillna(0)

        # === Step 6: Deduplicate (Filter out existing data from PostgreSQL) ===
        existing_data_df = read_postgres_data()

        if existing_data_df is not None:
            # Perform a left anti join to remove rows already present in PostgreSQL
            new_data_df = summary_df.join(existing_data_df, 
                                          on=["stop_id", "route", "timestamp", "trip_id"], 
                                          how="left_anti")  # keep only new rows
            print(f"✅ Filtered out existing rows, keeping {new_data_df.count()} new rows.")
        else:
            # If no existing data, we will write all records
            new_data_df = summary_df
            print("⚠️ No existing data in PostgreSQL. Writing all data.")

        # Optional: Preview the new data
        new_data_df.show(100, truncate=False)

        # === Step 7: Save the new data to PostgreSQL ===
        new_data_df.write \
            .jdbc(url=POSTGRES_URL,
                  table=POSTGRES_TABLE,
                  mode="append",  # Append only the new records
                  properties=POSTGRES_PROPERTIES)
        print(f"✅ Successfully wrote new data to PostgreSQL table '{POSTGRES_TABLE}'.")

    except Exception as e:
        print(f"❌ Error during aggregation or PostgreSQL write: {e}")

    time.sleep(POLL_INTERVAL_SECONDS)

# === Cleanup ===
spark.stop()
print("🛑 Spark session stopped.")
