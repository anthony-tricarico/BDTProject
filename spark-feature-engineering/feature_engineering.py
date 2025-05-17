import pyspark.pandas as ps
from pyspark.sql import SparkSession
import time

while True:

    # --- Spark and DB Config ---
    POSTGRES_URL = "jdbc:postgresql://db:5432/raw_data"
    POSTGRES_USER = "postgres"
    POSTGRES_PWD = "example"

    spark = SparkSession.builder \
        .appName("PandasOnSparkFeatureEngineering") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
        .getOrCreate()

    # --- Read Data from PostgreSQL ---
    def read_table(table, index_col):
        sdf = spark.read.format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", table) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PWD) \
            .load()
        return sdf.pandas_api().set_index(index_col)

    tickets = read_table("raw_tickets", "ticket_id")
    sensors = read_table("raw_sensors", "measurement_id")
    trips = read_table("trips", "trip_id")
    traffic = read_table("traffic", "trip_id")
    weather = read_table("weather", "measurement_id")
    events = read_table("events", "event_id")

    # --- Feature Engineering (Pandas API on Spark) ---
    tickets['timestamp'] = ps.to_datetime(tickets['timestamp'])
    tickets['day'] = tickets['timestamp'].dt.date
    agg_tickets = tickets.groupby(['trip_id', 'day']).agg({
        'fare': 'count',
        'school': 'max',
        'hospital': 'max',
        'peak_hour': 'max',
        'timestamp': 'max'
    }).rename(columns={'fare': 'passengers_in'}).reset_index()

    sensors['timestamp'] = ps.to_datetime(sensors['timestamp'])
    sensors['day'] = sensors['timestamp'].dt.date
    agg_sensors = sensors.groupby(['trip_id', 'day']).agg({
        'status': 'count',
        'timestamp': 'max'
    }).rename(columns={'status': 'passengers_out'}).reset_index()

    # Merge tickets and sensors
    merged = agg_tickets.merge(agg_sensors, on=['trip_id', 'day', 'timestamp'], how='inner')
    merged = merged.merge(trips.reset_index(), on='trip_id', how='left')
    traffic['timestamp'] = ps.to_datetime(traffic['timestamp'])
    merged = merged.merge(traffic.reset_index(), on=['trip_id', 'timestamp'], how='left')

    # Weather asof merge (convert to pandas for this step)
    merged_pd = merged.to_pandas()
    weather_pd = weather.to_pandas()
    weather_pd['hour'] = ps.to_datetime(weather_pd['hour'])
    merged_pd['timestamp'] = ps.to_datetime(merged_pd['timestamp'])
    merged_pd = ps.merge_asof(
        merged_pd.sort_values('timestamp'),
        weather_pd[['hour', 'temperature', 'precipitation_probability', 'weather_code', 'latitude', 'longitude']].sort_values('hour'),
        left_on='timestamp', right_on='hour', direction='backward'
    )

    # Merge with events (by date)
    events_pd = events.to_pandas()
    events_pd['day_event'] = ps.to_datetime(events_pd['day_event']).dt.date
    merged_pd['day'] = ps.to_datetime(merged_pd['day']).dt.date
    final = merged_pd.merge(events_pd, left_on='day', right_on='day_event', how='left')
    final['event_dummy'] = final['day_event'].notna().astype(int)

    # Example: Add congestion feature
    if 'bus_capacity' in final.columns:
        final['occupancy'] = (final['passengers_in'] - final['passengers_out']).cumsum()
        final['congestion_rate'] = final['occupancy'] / final['bus_capacity']

    # --- Write to PostgreSQL ---
    final_ps = ps.from_pandas(final)
    final_sdf = final_ps.to_spark()
    final_sdf.write.format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "feature_table") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PWD) \
        .mode("overwrite") \
        .save()

    print("Feature table created and saved to PostgreSQL.")
    time.sleep(100)