import streamlit as st
import pandas as pd
import numpy as np

st.set_page_config(layout="wide")
st.title("📈 Congestion Forecast")

# Sidebar
st.sidebar.markdown("🛠 Settings")
st.sidebar.markdown("Adjust model & prediction windows (future feature)")

# --------------------
# Simulated Forecast
# --------------------
st.subheader("Congestion Forecast")
st.info("Model predictions for upcoming congestion at key stops.")

# Simple congestion trend at stops (visual only)
st.line_chart({
    "Stop A": [30, 55, 70, 80],
    "Stop B": [20, 35, 45, 60]
})

# Detailed forecast for bus lines
forecast_df = pd.DataFrame({
    "Time": pd.date_range("2025-05-19 14:00", periods=6, freq="5T"),
    "Route A": [0.2, 0.4, 0.6, 0.9, 0.8, 0.5],
    "Route B": [0.3, 0.3, 0.5, 0.6, 0.7, 0.8],
    "Route C": [0.1, 0.2, 0.4, 0.6, 0.5, 0.3]
})
st.line_chart(forecast_df.set_index("Time"))

st.info("Showing predicted congestion for the next 30 minutes.")

# ---------------------
# Bus Allocation Logic
# ---------------------
st.subheader("🚍 Suggested Bus Allocation")

# Calculate average congestion over next 30 minutes
avg_congestion = forecast_df.drop(columns="Time").mean()

# Simple rule: allocate more buses to routes with higher congestion
total_buses_available = 10

# Normalize to allocate based on relative congestion
weights = avg_congestion / avg_congestion.sum()
allocations = (weights * total_buses_available).round().astype(int)

# Display allocation
allocation_df = pd.DataFrame({
    "Avg Congestion": avg_congestion.round(2),
    "Suggested Buses": allocations
}).sort_values("Avg Congestion", ascending=False)

st.table(allocation_df)

# Optional recommendation summary
most_congested = avg_congestion.idxmax()
st.success(f"📢 Recommendation: Add more buses to **{most_congested}**, which is forecasted to be the most congested route.")
