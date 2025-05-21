import streamlit as st
import numpy as np
import pandas as pd

st.title("Congestion Tracker")

#st.markdown("# Page 1 ❄️")

#Sidebar settings
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
# Simulate congestion levels for each segment in each route
# --------------------------
congestion_colors = {
    0: [0, 255, 0],
    1: [255, 255, 0],
    2: [255, 0, 0]
}

def simulate_congestion(route):
    levels = np.random.choice([0, 1, 2], size=len(route)-1, p=[0.5, 0.3, 0.2])
    score = np.mean(levels)  # Average congestion
    return levels, score

# --------------------------
# User input
# --------------------------
origin = st.selectbox("Select origin stop:", ["Piazza Dante"])
destination = st.selectbox("Select destination stop:", ["Università Centrale"])

# --------------------------
# Calculate congestion scores for all routes
# --------------------------
scores = {}
congestion_by_route = {}

for name, coords in routes.items():
    levels, score = simulate_congestion(coords)
    scores[name] = score
    congestion_by_route[name] = levels

# --------------------------
# Recommend route
# --------------------------
best_route = min(scores, key=scores.get)
st.success(f"🚍 Recommended Route: **{best_route}** (Least congested)")