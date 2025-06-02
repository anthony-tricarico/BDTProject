import streamlit as st
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

st.title("Congestion Tracker")
# --------------------------
# Real Data from PostgreSQL
# --------------------------
POSTGRES_URL = "postgresql+psycopg2://postgres:example@db:5432/raw_data"
engine = create_engine(POSTGRES_URL)

@st.cache_data(ttl=60)
def load_congestion_data():
    df = pd.read_sql("SELECT * FROM congestion_by_route ORDER BY timestamp_x DESC", engine)
    return df

def update_congestion_by_route():
    query = """
    SELECT 
        f.timestamp_x,
        f.congestion_rate,
        f.traffic_level,
        r.route_short_name
    FROM feature_table f
    JOIN trips t ON f.trip_id_x = t.trip_id
    JOIN routes r ON t.route_id = r.route_id
    """
    df_new = pd.read_sql(query, engine)
    df_new.dropna(inplace=True)
    df_new.to_sql("congestion_by_route", engine, if_exists="replace", index=False)
    return len(df_new)

if st.button("🔄 Refresh"):
    with st.spinner("Loading new data..."):
        rows = update_congestion_by_route()
    st.success(f"✅ Table updated with {rows} rows.")

try:
    df = load_congestion_data()
except Exception as e:
    st.error(f"Error while loading data: {e}")
    st.stop()

st.subheader("📋 Filter data by route")
selected_route = st.selectbox("Select a route", df["route_short_name"].unique())
filtered_df = df[df["route_short_name"] == selected_route]

if filtered_df.empty:
    st.warning("No data is available for this route.")
else:
    # shows latest 10 values of the dataframe
    st.dataframe(filtered_df.sort_values("timestamp_x", ascending=False).head(10), hide_index=True)