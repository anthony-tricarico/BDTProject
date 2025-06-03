import streamlit as st
from sqlalchemy import create_engine
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px

st.title("🚦Traffic Anomalies and Congestion")

st.subheader("⚠️ Anomaly Detection")
st.info("This page shows unusual traffic patterns based on different routes and time of the day.")

# --------------------------
# Real Data from PostgreSQL
# --------------------------
POSTGRES_URL = "postgresql+psycopg2://postgres:example@db:5432/raw_data"
engine = create_engine(POSTGRES_URL)

@st.cache_data(ttl=60)
def load_congestion_data():
    df = pd.read_sql("SELECT * FROM congestion_by_route ORDER BY timestamp_x DESC", engine)
    return df

def update_traffic_by_route():
    query = """
    SELECT 
        f.timestamp_x,
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


# Always show refresh button first
if st.button("🔄 Refresh"):
    with st.spinner("Loading new data..."):
        rows = update_traffic_by_route()
    st.success(f"✅ Table updated with {rows} rows.")

# Then attempt to load data
try:
    df = load_congestion_data()
except Exception as e:
    st.error(f"Error while loading data: {e}")
    st.stop()
   
    
df["timestamp_x"] = pd.to_datetime(df["timestamp_x"])  # ✅ ensure datetime
df["hour"] = df["timestamp_x"].dt.hour
mapping = {'no traffic/low': 0, 'medium': 1, 'heavy': 2, 'severe/standstill': 3}
df['traffic_level'] = df['traffic_level'].map(mapping)

heatmap_data = df.groupby(["route_short_name", "hour"])["traffic_level"].mean().reset_index()
pivot = heatmap_data.pivot(index="route_short_name", columns="hour", values="traffic_level")

########################################## 
if pivot.empty:
    st.warning("⚠️ No traffic data available to display the heatmap.")
else:
    st.subheader("📊 Average Traffic Level by Route and Hour")

    # Set custom style
    sns.set_theme(style="whitegrid", font_scale=1.1)

    fig, ax = plt.subplots(figsize=(12, 6))

    # Draw heatmap
    sns.heatmap(
        pivot,
        annot=True,
        cmap="YlGnBu",  # better contrast for readability
        fmt=".1f",
        linewidths=0.6,
        linecolor="white",
        cbar_kws={"label": "Traffic Level (0=Low, 3=Severe)"}
    )

    # Enhance labels
    ax.set_xlabel("Hour of Day", fontsize=12, labelpad=10)
    ax.set_ylabel("Route", fontsize=12, labelpad=10)
    ax.set_title("Hourly Average Traffic Level per Route", fontsize=14, pad=15)

    # Improve spacing
    fig.tight_layout()
    st.pyplot(fig)
    

##########################################    
st.subheader("📈 Traffic Level Over Time")

# Interactive controls
available_routes = df["route_short_name"].unique()
selected_routes = st.multiselect("Select routes to display", available_routes, default=available_routes.tolist())

# Filter by selected routes
filtered_df = df[df["route_short_name"].isin(selected_routes)]

# Round timestamps and group
filtered_df["timestamp_hour"] = filtered_df["timestamp_x"].dt.floor("H")
time_df = (
    filtered_df.groupby(["route_short_name", "timestamp_hour"])["traffic_level"]
    .mean()
    .reset_index()
)

fig = px.line(
    time_df,
    x="timestamp_hour",
    y="traffic_level",
    color="route_short_name",
    title="Hourly Average Traffic Level per Route",
    markers=True,
    labels={
        "timestamp_hour": "Time",
        "traffic_level": "Traffic Level",
        "route_short_name": "Route"
    }
)

fig.update_layout(
    height=400,
    margin=dict(t=50, b=40, l=40, r=20),
    hovermode="x unified",
    yaxis_range=[0,3]
)
st.plotly_chart(fig, use_container_width=True)

########################################## 
#Traffic alert
latest = df.groupby("route_short_name")["traffic_level"].last()
anomalies = latest[latest > 0.8]

if not anomalies.empty:
    for route, level in anomalies.items():
        st.warning(f"⚠️ Route {route} shows high traffic level: {level:.2f}")
else:
    st.success("✅ No traffic anomalies detected.")
