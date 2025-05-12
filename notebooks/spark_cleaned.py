from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pymongo import MongoClient

# ---- MongoDB Connectivity Check ----
try:
    client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=5000)
    print("MongoDB connection successful. Databases:", client.list_database_names())
except Exception as e:
    print("MongoDB connection error:", e)

# ---- Spark Session Setup ----
mongodb_connector = "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"

spark = SparkSession.builder \
    .appName("MongoSparkMultiCollectionAggregator") \
    .config("spark.jars.packages", mongodb_connector) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---- Configurable Collections List ----
collections = ["passengers", "sensors"]  # Add more collections here
db_name = "raw"
mongo_uri_base = "mongodb://localhost:27017"

# ---- Function to Read and Tag DataFrame ----
def read_collection(collection_name):
    try:
        df = spark.read \
            .format("mongodb") \
            .option("spark.mongodb.read.connection.uri", mongo_uri_base) \
            .option("spark.mongodb.read.database", db_name) \
            .option("spark.mongodb.read.collection", collection_name) \
            .load() \
            .withColumn("source_collection", lit(collection_name))
        return df
    except Exception as e:
        print(f"Failed to read collection '{collection_name}': {e}")
        return None


# ---- Streaming Batch Processor ----
def process_batch(batch_df, batch_id):
    print(f"\n--- Processing batch {batch_id} ---")
    try:
        dataframes = []
        for collection in collections:
            df = read_collection(collection)
            if df is not None:
                dataframes.append(df)

        if dataframes:
            combined_df = dataframes[0]
            for df in dataframes[1:]:
                combined_df = combined_df.unionByName(df, allowMissingColumns=True)

            print(f"Combined row count: {combined_df.count()}")
            combined_df.withColumn("timestamp", current_timestamp()).show(truncate=False)
        else:
            print("No data to aggregate.")
    except Exception as e:
        print(f"Error processing batch {batch_id}: {e}")

# ---- Dummy Stream Trigger ----
try:
    rate_stream = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 1) \
        .load()

    query = rate_stream.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .trigger(processingTime="10 seconds") \
        .start()

    print("Streaming query started. Waiting for termination...")
    query.awaitTermination()

except Exception as e:
    print(f"Streaming query error: {e}")

finally:
    print("Shutting down Spark session...")
    spark.stop()
