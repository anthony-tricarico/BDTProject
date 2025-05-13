import time
import uuid
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StringType, NumericType
from pyspark.sql.functions import col, count, lit, when, coalesce
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values

# === Configuration ===

# MongoDB connection parameters
MONGO_URI = "mongodb://localhost:27017"
DATABASE = "raw"
SENSOR_COLLECTION = "sensors"
TICKET_COLLECTION = "tickets"
POLL_INTERVAL_SECONDS = 5
MAX_ITERATIONS = 12  # e.g., 1 minute of polling if interval = 5s

# PostgreSQL connection parameters
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

# === Step 2: Initialize SparkSession ===
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

# === Step 3: Polling Loop for Processing Data ===
for i in range(MAX_ITERATIONS):
    print(f"\n🔄 --- Iteration {i + 1} ---")

    # Load both collections
    sensors_df = read_collection(SENSOR_COLLECTION)
    tickets_df = read_collection(TICKET_COLLECTION)

    if sensors_df is None or tickets_df is None:
        print("⚠️ One or both collections failed to load. Retrying...")
        time.sleep(POLL_INTERVAL_SECONDS)
        continue

    try:
        # === Step 4: Aggregation ===
        sensor_agg = sensors_df.groupBy("stop_id", "route", "timestamp", "trip_id", "bus_id") \
            .agg(count("*").alias("passengers_off"))

        ticket_agg = tickets_df.groupBy("stop_id", "route", "timestamp", "trip_id", "bus_id") \
            .agg(count("*").alias("passengers_on"))

        # === Step 5: Join both aggregations ===
        summary_df = sensor_agg.join(ticket_agg,
                                     on=["stop_id", "route", "timestamp", "trip_id", "bus_id"],
                                     how="outer").fillna(0)

        # === Step 6: Load previous occupancy ===
        try:
            previous_df = spark.read.jdbc(
                url=POSTGRES_URL,
                table=POSTGRES_TABLE,
                properties=POSTGRES_PROPERTIES
            ).select("stop_id", "route", "timestamp", "trip_id", "occupancy")
            print("✅ Loaded previous occupancy data.")
        except Exception as e:
            previous_df = None
            print("⚠️ Could not load previous occupancy data (first run or table empty).")

        # === Step 7: Compute new occupancy ===
        if previous_df is not None:
            summary_df = summary_df.join(
                previous_df,
                on=["stop_id", "route", "timestamp", "trip_id"],
                how="left"
            )
            # Use coalesce to replace null occupancy with 0
            summary_df = summary_df.withColumn(
                "occupancy",
                coalesce(col("occupancy"), lit(0)) + col("passengers_on") - col("passengers_off")
            )
        else:
            summary_df = summary_df.withColumn(
                "occupancy",
                col("passengers_on") - col("passengers_off")
            )

        # === Step 8: Join with bus table to get total_capacity ===
        try:
            # Read the bus table with bus_id and total_capacity
            bus_df = spark.read.jdbc(
                url=POSTGRES_URL,
                table="bus",
                properties=POSTGRES_PROPERTIES
            ).select("bus_id", "total_capacity")

            bus_df = bus_df.withColumn("total_capacity", col("total_capacity").cast("double"))

            print("✅ Loaded 'bus' table.")

            # Make sure both sides have consistent bus_id types
            summary_df = summary_df.withColumn("bus_id", col("bus_id").cast(NumericType()))
            bus_df = bus_df.withColumn("bus_id", col("bus_id").cast(NumericType()))

            # Join with bus table
            summary_df = summary_df.join(bus_df, on="bus_id", how="left")

            # Check if the join worked
            if "total_capacity" not in summary_df.columns:
                raise Exception("Join with 'bus' table failed. 'total_capacity' column missing.")

            # Compute occupancy ratio
            summary_df = summary_df.withColumn(
                "occupancy_ratio",
                when(col("total_capacity").isNotNull() & (col("total_capacity") > 0),
                    (col("occupancy") / col("total_capacity")).cast("double"))
                .otherwise(None)
            )

            print("✅ Joined with bus table and computed occupancy ratio.")

        except Exception as e:
            print(f"❌ Failed to join with bus table or compute ratio: {e}") 
            
        # === Step 8.5: Use more reliable approach to handle duplicates ===
        try:
            # Instead of filtering, let's use a more reliable approach:
            # 1. Create a temporary table with our data
            # 2. Use SQL's ON CONFLICT DO NOTHING semantics
            
            # Generate a unique temp table name to avoid conflicts
            import uuid
            temp_table_name = f"temp_bus_data_{uuid.uuid4().hex[:8]}"
            
            print(f"ℹ️ Creating temporary table '{temp_table_name}'")
            
            # Write to a temporary table first
            summary_df.write \
                .jdbc(url=POSTGRES_URL,
                      table=temp_table_name,
                      mode="overwrite",
                      properties=POSTGRES_PROPERTIES)
            
            # Now use the PostgreSQL connection to execute a SQL INSERT with ON CONFLICT DO NOTHING
            # This will only insert rows that don't violate the primary key constraint
            
            from pyspark.sql import Row
            import psycopg2
            from psycopg2.extras import execute_values
            
            # Extract connection details from the JDBC URL
            # Parse the JDBC URL to extract connection details
            # Format: jdbc:postgresql://localhost:5432/raw_data
            jdbc_parts = POSTGRES_URL.replace("jdbc:postgresql://", "").split(":")
            host = jdbc_parts[0]
            port_db = jdbc_parts[1].split("/")
            port = port_db[0]
            db_name = port_db[1]
            
            # Establish a direct Postgres connection
            conn = psycopg2.connect(
                dbname=db_name,
                user=POSTGRES_PROPERTIES["user"],
                password=POSTGRES_PROPERTIES["password"],
                host=host,
                port=port
            )
            
            try:
                with conn.cursor() as cursor:
                    # Execute the INSERT with ON CONFLICT DO NOTHING
                    # Only include columns that exist in the target table
                    cursor.execute(f"""
                        INSERT INTO {POSTGRES_TABLE} (
                            stop_id, route, timestamp, trip_id, bus_id, 
                            passengers_off, passengers_on, occupancy
                        )
                        SELECT 
                            stop_id, route, timestamp, trip_id, bus_id, 
                            passengers_off, passengers_on, occupancy
                        FROM {temp_table_name}
                        ON CONFLICT (stop_id, route, timestamp, trip_id) DO NOTHING
                    """)
                    
                    # Get count of inserted rows
                    inserted_rows = cursor.rowcount
                    conn.commit()
                    print(f"✅ Inserted {inserted_rows} new records (skipped duplicates)")
                    
            except Exception as e:
                conn.rollback()
                print(f"❌ Error during SQL insert: {e}")
            finally:
                # Clean up temporary table
                cursor = conn.cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                conn.commit()
                conn.close()
                
            # Skip the standard write operation since we handled it with psycopg2
            continue

        except Exception as e:
            print(f"⚠️ Could not filter existing records: {e}")
            print("⚠️ Will attempt insert anyway which may cause duplicates.")

        # === Step 9: Write to PostgreSQL ===
        if summary_df.count() > 0:
            summary_df.write \
                .jdbc(url=POSTGRES_URL,
                      table=POSTGRES_TABLE,
                      mode="append",
                      properties=POSTGRES_PROPERTIES)
            print(f"✅ Wrote {summary_df.count()} records to PostgreSQL: '{POSTGRES_TABLE}'")
        else:
            print("ℹ️ No new records to write.")

    except Exception as e:
        print(f"❌ Error during aggregation or writing: {e}")

    time.sleep(POLL_INTERVAL_SECONDS)

# === Cleanup ===
spark.stop()
print("🛑 Spark session stopped.")