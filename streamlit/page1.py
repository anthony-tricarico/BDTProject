import streamlit as st
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import altair as alt

st.title("Real-Time Congestion Tracker")

# --------------------------
# Real data from PostgreSQL
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
    WHERE f.congestion_rate IS NOT NULL
    """
    df_new = pd.read_sql(query, engine)
    df_new.dropna(inplace=True)
    df_new.to_sql("congestion_by_route", engine, if_exists="replace", index=False)
    return len(df_new)

st.info("This table shows real-time congestion rates for each trip, showing the route relative to the trip and the timestamp")

if st.button("🔄 Refresh"):
    with st.spinner("Loading new data..."):
        rows = update_congestion_by_route()
    st.success(f"✅ Table updated with {rows} rows.")

try:
    df = load_congestion_data()
except Exception as e:
    st.error(f"Error while loading data: {e}")
    st.stop()

st.subheader("📋 Select Route")
df["timestamp_x"] = pd.to_datetime(df["timestamp_x"])

# Get unique routes without sorting
available_routes = df["route_short_name"].unique().tolist()
selected_route = st.selectbox("Select a route", available_routes)

st.subheader("📅 Select Date")
available_dates = df[df["route_short_name"] == selected_route]["timestamp_x"].dt.date.unique()
selected_date = st.selectbox("Available dates", sorted(available_dates))

# Filter data by route and exact date
filtered_df = df[(df["route_short_name"] == selected_route) & (df["timestamp_x"].dt.date == selected_date)].copy()
filtered_df["hour"] = filtered_df["timestamp_x"].dt.hour

if filtered_df.empty:
    st.warning("No data is available for this route and date.")
else:
    st.dataframe(filtered_df.sort_values("timestamp_x", ascending=False), hide_index=True)

    st.subheader("📊 Congestion Overview by Hour")
    hourly_avg = filtered_df.groupby("hour")["congestion_rate"].mean().reset_index()

    chart = alt.Chart(hourly_avg).mark_bar(color="steelblue").encode(
        x=alt.X("hour:O", title="Hour of Day", axis=alt.Axis(labelAngle=0)),
        y=alt.Y("congestion_rate:Q", title="Average Congestion Rate")
    ).properties(
        width=700,
        height=400,
        title=f"Average Congestion Rate by Hour - {selected_date} - {selected_route}"
    ) + alt.Chart(pd.DataFrame({"y": [1.0]})).mark_rule(color="red", strokeDash=[4, 4]).encode(y="y")

    st.altair_chart(chart, use_container_width=True)

    st.subheader("🚌 Suggested Additional Buses")
    high_cong_hours = hourly_avg[hourly_avg["congestion_rate"] > 1.0]["hour"].tolist()

    merged_blocks = []
    if high_cong_hours:
        block = [high_cong_hours[0]]
        for h in high_cong_hours[1:]:
            if h == block[-1] + 1:
                block.append(h)
            else:
                merged_blocks.append((block[0], block[-1]))
                block = [h]
        merged_blocks.append((block[0], block[-1]))

    suggestions = []
    for start, end in merged_blocks:
        suggestions.append({
            "Date": selected_date.strftime("%A, %d %B %Y"),
            "Time Interval": f"{start}:00 - {end + 1}:00",
            "Suggested Buses": 1,
            "Reason": "Avg. congestion > 1.0"
        })

    if suggestions:
        suggestions_df = pd.DataFrame(suggestions)
        st.dataframe(suggestions_df, use_container_width=True, hide_index=True)
    else:
        st.success("✅ No time intervals require additional buses on this date.")