import streamlit as st
import pandas as pd
import altair as alt
import numpy as np
import pydeck as pdk

st.title("Analytics")

#st.markdown("# Page 1 ❄️")

#Sidebar settings
st.sidebar.markdown("Analytics")


st.subheader("📊 Passenger Flow Analytics")
df = pd.DataFrame({
    "Hour": list(range(6, 22)),
    "Passengers": [30, 50, 90, 120, 150, 170, 200, 180, 160, 140, 100, 80, 60, 50, 40, 30]
})
chart = alt.Chart(df).mark_area(opacity=0.4).encode(
    x="Hour",
    y="Passengers"
)
st.altair_chart(chart, use_container_width=True)




st.subheader("📊 Passenger Flow Analytics")

# Filters
bus_line = st.selectbox("Select Bus Line", ["Route A", "Route B", "Route C"])
stop = st.selectbox("Select Stop", ["All", "Piazza Dante", "Via Verdi", "Università Centrale"])

# Dummy analytics
df = pd.DataFrame({
    "Hour": range(6, 24),
    "Passengers": [10, 15, 30, 45, 60, 80, 100, 85, 70, 50, 40, 30, 20, 15, 10, 5, 3, 2]
})

st.bar_chart(df.set_index("Hour"))

#Map showing number of people at each stop on the base of the time of the day

st.write("Map showing number of people at each stop on the base of the time of the day")

chart_data = pd.DataFrame(
   np.random.randn(1000, 2) / [50, 50] + [37.76, -122.4],
   columns=['lat', 'lon'])

st.pydeck_chart(pdk.Deck(
    map_style=None,
    initial_view_state=pdk.ViewState(
        latitude=46.066666,
        longitude=11.116667,
        zoom=11,
        pitch=50,
    ),
    layers=[
        pdk.Layer(
           'HexagonLayer',
           data=chart_data,
           get_position='[lon, lat]',
           radius=200,
           elevation_scale=4,
           elevation_range=[0, 1000],
           pickable=True,
           extruded=True,
        ),
        pdk.Layer(
            'ScatterplotLayer',
            data=chart_data,
            get_position='[lon, lat]',
            get_color='[200, 30, 0, 160]',
            get_radius=200,
        ),
    ],
))