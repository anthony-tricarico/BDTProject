# import streamlit as st
# import pandas as pd
# import pydeck as pdk
# import time
# import numpy as np

# st.set_page_config(layout="wide")
# st.title("🚌 Bus Animation with Route Trail")

# # ✅ Use a valid transparent PNG bus icon
# BUS_ICON_URL = "https://img.icons8.com/emoji/48/bus-emoji.png"

# # Simulated GPS route

# route = [
#     {"lat": 46.06612, "lon": 11.15504},
#     {"lat": 46.06612, "lon": 11.15504},
#     {"lat": 46.06639, "lon": 11.15629},
#     {"lat": 46.06642, "lon": 11.15679},
#     {"lat": 46.06636, "lon": 11.15713},
#     {"lat": 46.06625, "lon": 11.15734},
#     {"lat": 46.06475, "lon": 11.15872},
#     {"lat": 46.06463, "lon": 11.15899},
#     {"lat": 46.06462, "lon": 11.15916},
#     {"lat": 46.06464, "lon": 11.15928},
#     {"lat": 46.06475, "lon": 11.15942},
#     {"lat": 46.06475, "lon": 11.15942},
#     {"lat": 46.06482, "lon": 11.15947}
# ]

# # Add icon data to each point
# for point in route:
#     point["icon_data"] = {
#         "url": BUS_ICON_URL,
#         "width": 128,
#         "height": 128,
#         "anchorY": 128
#     }

# df_route = pd.DataFrame(route)
# map_placeholder = st.empty()

# # Define bus stops
# stops = [
#     {"lat": 46.06612, "lon": 11.15504, "name": "Piazza Dante"},
#     {"lat": 46.06475, "lon": 11.15872, "name": "Via Verdi"},
#     {"lat": 46.06482, "lon": 11.15947, "name": "Università Centrale"}
# ]

# PIN_ICON_URL = "https://img.icons8.com/color/48/marker.png"

# for stop in stops:
#     stop["icon_data"] = {
#         "url": PIN_ICON_URL,
#         "width": 128,
#         "height": 128,
#         "anchorY": 128
#     }

# df_stops = pd.DataFrame(stops)

# # Animate the bus and build the path
# for i in range(1, len(df_route) + 1):
#     current_position = df_route.iloc[[i - 1]]  # One-row DataFrame with current bus
#     path_so_far = df_route.iloc[:i]            # Path covered so far

#     # ✅ Format the path data for LineLayer
#     path_layer_data = [{
#         "path": path_so_far[["lon", "lat"]].values.tolist()
#     }]

#     # Layers
#     icon_layer = pdk.Layer(
#         "IconLayer",
#         data=current_position,
#         get_icon="icon_data",
#         get_size=4,
#         size_scale=15,
#         get_position='[lon, lat]',
#         pickable=False,
#     )

#     line_layer = pdk.Layer(
#         "PathLayer",
#         data=path_layer_data,
#         get_path="path",
#         get_color=[255, 0, 0],
#         width_scale=20,
#         width_min_pixels=2,
#     )

#     stop_layer = pdk.Layer(
#         "IconLayer",
#         data=df_stops,
#         get_icon="icon_data",
#         get_size=3,
#         size_scale=8,
#         get_position='[lon, lat]',
#         pickable=True,
#         tooltip={"text": "{name}"}
#     )

#     view_state = pdk.ViewState(
#         latitude=current_position["lat"].values[0],
#         longitude=current_position["lon"].values[0],
#         zoom=15,
#         pitch=45
#     )

#     r = pdk.Deck(
#         layers=[line_layer, icon_layer, stop_layer],
#         initial_view_state=view_state,
#         tooltip={"text": "{name}\nLat: {lat}, Lon: {lon}"}
#     )

#     map_placeholder.pydeck_chart(r)
#     time.sleep(1)

import streamlit as st
import pandas as pd
import pydeck as pdk
import time
import numpy as np

st.set_page_config(layout="wide")
st.title("🚌 Live Bus Congestion Map with Color Coding")
st.markdown("🔴 High Congestion | 🟠 Medium Congestion | 🟢 Low Congestion")


# ✅ Use a valid transparent PNG bus icon
BUS_ICON_URL = "https://img.icons8.com/emoji/48/bus-emoji.png"
PIN_ICON_URL = "https://img.icons8.com/color/48/marker.png"

# Simulated GPS route
route = [
    {"lat": 46.06612, "lon": 11.15504},
    {"lat": 46.06639, "lon": 11.15629},
    {"lat": 46.06642, "lon": 11.15679},
    {"lat": 46.06636, "lon": 11.15713},
    {"lat": 46.06625, "lon": 11.15734},
    {"lat": 46.06475, "lon": 11.15872},
    {"lat": 46.06463, "lon": 11.15899},
    {"lat": 46.06462, "lon": 11.15916},
    {"lat": 46.06464, "lon": 11.15928},
    {"lat": 46.06475, "lon": 11.15942},
    {"lat": 46.06482, "lon": 11.15947}
]

# Add simulated congestion levels (0=low, 1=med, 2=high)
congestion_levels = np.random.choice([0, 1, 2], size=len(route)-1, p=[0.5, 0.3, 0.2])
congestion_colors = {
    0: [0, 255, 0],     # Green
    1: [255, 255, 0],   # Yellow
    2: [255, 0, 0]      # Red
}

# Add bus icon to each point
for point in route:
    point["icon_data"] = {
        "url": BUS_ICON_URL,
        "width": 128,
        "height": 128,
        "anchorY": 128
    }

df_route = pd.DataFrame(route)
map_placeholder = st.empty()

# Define interactive stops
stops = [
    {"lat": 46.06612, "lon": 11.15504, "name": "Piazza Dante"},
    {"lat": 46.06475, "lon": 11.15872, "name": "Via Verdi"},
    {"lat": 46.06482, "lon": 11.15947, "name": "Università Centrale"}
]
for stop in stops:
    stop["icon_data"] = {
        "url": PIN_ICON_URL,
        "width": 128,
        "height": 128,
        "anchorY": 128
    }
df_stops = pd.DataFrame(stops)

# Animate the bus and build dynamic colored path
for i in range(1, len(df_route)):
    current_position = df_route.iloc[[i]]  # One-row DataFrame with current bus
    path_so_far = df_route.iloc[:i+1]

    # Create a list of line segments with colors based on congestion
    path_segments = []
    for j in range(i):
        path_segments.append({
            "path": [
                [df_route.iloc[j]["lon"], df_route.iloc[j]["lat"]],
                [df_route.iloc[j+1]["lon"], df_route.iloc[j+1]["lat"]]
            ],
            "color": congestion_colors[congestion_levels[j]]
        })

    # Layers
    icon_layer = pdk.Layer(
        "IconLayer",
        data=current_position,
        get_icon="icon_data",
        get_size=4,
        size_scale=15,
        get_position='[lon, lat]',
        pickable=False,
    )

    colored_line_layer = pdk.Layer(
        "PathLayer",
        data=path_segments,
        get_path="path",
        get_color="color",
        width_scale=20,
        width_min_pixels=3,
    )

    stop_layer = pdk.Layer(
        "IconLayer",
        data=df_stops,
        get_icon="icon_data",
        get_size=3,
        size_scale=8,
        get_position='[lon, lat]',
        pickable=True,
        tooltip={"text": "{name}"}
    )

    view_state = pdk.ViewState(
        latitude=current_position["lat"].values[0],
        longitude=current_position["lon"].values[0],
        zoom=15,
        pitch=45
    )

    r = pdk.Deck(
        layers=[colored_line_layer, icon_layer, stop_layer],
        initial_view_state=view_state,
        tooltip={"text": "{name}\nLat: {lat}, Lon: {lon}"}
    )

    map_placeholder.pydeck_chart(r)
    time.sleep(1)
