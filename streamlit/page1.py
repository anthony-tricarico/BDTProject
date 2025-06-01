import streamlit as st
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

# UI iniziale
st.set_page_config(page_title="Congestion Tracker", layout="wide")
st.title("Congestion Tracker")
st.sidebar.markdown("Congestion Tracker")
st.info("🚦 Smart Route Suggestion: Least Congested Bus Route")

# --------------------------
# Simulated route data
# --------------------------
routes = {
    "Route A": [
        {"lat": 46.06612, "lon": 11.15504},
        {"lat": 46.06639, "lon": 11.15629},
        {"lat": 46.06642, "lon": 11.15679},
    ],
    "Route B": [
        {"lat": 46.06612, "lon": 11.15504},
        {"lat": 46.06636, "lon": 11.15713},
        {"lat": 46.06475, "lon": 11.15872},
    ],
    "Route C": [
        {"lat": 46.06612, "lon": 11.15504},
        {"lat": 46.06462, "lon": 11.15916},
        {"lat": 46.06482, "lon": 11.15947},
    ],
}

# --------------------------
# Simulate congestion levels
# --------------------------
def simulate_congestion(route):
    levels = np.random.choice([0, 1, 2], size=len(route)-1, p=[0.5, 0.3, 0.2])
    score = np.mean(levels)
    return levels, score

origin = st.selectbox("Select origin stop:", ["Piazza Dante"])
destination = st.selectbox("Select destination stop:", ["Università Centrale"])

scores = {}
congestion_by_route_simulated = {}

for name, coords in routes.items():
    levels, score = simulate_congestion(coords)
    scores[name] = score
    congestion_by_route_simulated[name] = levels

best_route = min(scores, key=scores.get)
st.success(f"🚍 Recommended Route: **{best_route}** (Least congested)")

# --------------------------
# Dati reali da PostgreSQL
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

if st.button("🔄 Aggiorna"):
    with st.spinner("Aggiornamento in corso..."):
        rows = update_congestion_by_route()
    st.success(f"✅ Tabella aggiornata con {rows} righe.")

try:
    df = load_congestion_data()
except Exception as e:
    st.error(f"Errore nel caricamento dei dati: {e}")
    st.stop()

st.subheader("📋 Filtra dati per route")
selected_route = st.selectbox("Seleziona una route", df["route_short_name"].unique())
filtered_df = df[df["route_short_name"] == selected_route]

if filtered_df.empty:
    st.warning("Nessun dato disponibile per questa route.")
else:
    st.dataframe(filtered_df.sort_values("timestamp_x", ascending=False))
