from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# === Configuration ===
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPICS = ["sensors.topic", "ticketing.topic", "traffic.topic"]
CHECKPOINT_LOCATION = "/tmp/checkpoint_ml"

POSTGRES_URL = "jdbc:postgresql://db:5432/feature_data"
POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "example",
    "driver": "org.postgresql.Driver"
}

# === Schema Definitions ===
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
    StructField("hospital", IntegerType(), True), 
    StructField("school", IntegerType(), True)
])

TRAFFIC_SCHEMA = StructType([
    StructField("trip_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("traffic_level", StringType(), True),
    StructField("normal", IntegerType(), True),
    StructField("traffic", IntegerType(), True)
])

# === Spark Session ===
spark = SparkSession.builder \
    .appName("KafkaToPostgresFeatureStreaming") \
    .config("spark.jars.packages", ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.postgresql:postgresql:42.7.1"
    ])) \
    .getOrCreate()

# === Read Kafka Stream ===
def read_kafka_stream(topic, schema):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp("timestamp"))

sensors_df = read_kafka_stream(KAFKA_TOPICS[0], SENSOR_SCHEMA)
tickets_df = read_kafka_stream(KAFKA_TOPICS[1], TICKET_SCHEMA)
traffic_df = read_kafka_stream(KAFKA_TOPICS[2], TRAFFIC_SCHEMA).withColumn("timestamp", to_timestamp("timestamp"))

# === Static tables: trips, weather, events ===
trips_df = spark.read \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", "trips") \
    .option("user", POSTGRES_PROPERTIES["user"]) \
    .option("password", POSTGRES_PROPERTIES["password"]) \
    .load()

weather_df = spark.read \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", "weather") \
    .option("user", POSTGRES_PROPERTIES["user"]) \
    .option("password", POSTGRES_PROPERTIES["password"]) \
    .load() \
    .withColumn("hour", to_timestamp("hour"))

events_df = spark.read \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", "events") \
    .option("user", POSTGRES_PROPERTIES["user"]) \
    .option("password", POSTGRES_PROPERTIES["password"]) \
    .load() \
    .filter(col("latitude").isNotNull()) \
    .dropDuplicates(["day_event"]) \
    .withColumn("day_event", to_date("day_event"))

# === Aggregations ===
tickets_agg = tickets_df \
    .withWatermark("timestamp", "1 day") \
    .groupBy("trip_id", date_trunc("day", "timestamp").alias("day")) \
    .agg(
        count("fare").alias("passengers_in"),
        spark_max("school").alias("school"),
        spark_max("hospital").alias("hospital"),
        spark_max("peak_hour").alias("peak_hour"),
        spark_max("timestamp").alias("last_ticket_time")
    )

sensors_agg = sensors_df \
    .withWatermark("timestamp", "1 day") \
    .groupBy("trip_id", date_trunc("day", "timestamp").alias("day")) \
    .agg(
        count("status").alias("passengers_out"),
        spark_max("timestamp").alias("last_sensor_time")
    )

# === Enrichment with trips to get shape_id ===
tickets_enriched = tickets_agg \
    .join(trips_df.select("trip_id", "shape_id"), on="trip_id", how="left")

# === Join with traffic on shape_id (realigned) ===
traffic_enriched = traffic_df \
    .join(trips_df.select("trip_id", "shape_id"), on="trip_id", how="left") \
    .select("trip_id", "shape_id", "timestamp", "traffic_level", "normal", "traffic") \
    .withColumn("day", date_trunc("day", "timestamp"))

# === Merge tickets + sensors + traffic ===
combined = tickets_enriched \
    .join(sensors_agg, ["trip_id", "day"], how="outer") \
    .join(traffic_enriched, ["trip_id", "day"], how="left")

# === Weather "asof-style" join using window ===
weather_window = Window.partitionBy("trip_id").orderBy(col("hour").desc())

combined_with_weather = combined \
    .join(weather_df, expr("last_ticket_time >= hour"), "left") \
    .withColumn("rn", row_number().over(weather_window)) \
    .filter(col("rn") == 1).drop("rn")

# === Events join on date ===
combined_with_weather = combined_with_weather \
    .withColumn("event_date", to_date("last_ticket_time")) \
    .join(events_df, col("event_date") == col("day_event"), "left")

# === Null Safety ===
final_df = combined_with_weather.fillna({
    "passengers_in": 0,
    "passengers_out": 0,
    "peak_hour": 0,
    "school": 0,
    "hospital": 0
})

# === Write to PostgreSQL ===
def write_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    batch_df.write \
        .mode("append") \
        .jdbc(
            url=POSTGRES_URL,
            table="feature_table",
            properties=POSTGRES_PROPERTIES
        )

# === Start Query ===
query = final_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .option("checkpointLocation", CHECKPOINT_LOCATION + "/features") \
    .start()

query.awaitTermination()