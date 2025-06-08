import streamlit as st
import pandas as pd
import pydeck as pdk
import time
from sqlalchemy import create_engine, text
import numpy as np
import psycopg2
from psycopg2 import Error as PostgresError

st.set_page_config(layout="wide")
st.title("🚌 Live Bus Route Visualization with Congestion")
st.markdown("🔴 High Congestion | 🟡 Medium Congestion | 🟢 Low Congestion")
st.info("This map shows real-time congestion levels for each Trip, showing the route relative to the trip and the different stops")
st.warning("This feature is still in the experimental phase. Future app versions will solve all the visualizations bugs.")


# Icons
BUS_ICON_URL = "https://img.icons8.com/emoji/48/bus-emoji.png"
PIN_ICON_URL = "https://img.icons8.com/color/48/marker.png"

# Database connection settings
db_user = 'postgres'
db_pass = 'example'
db_host = 'db'
db_port = '5432'
db_name = 'raw_data'

# Congestion color mapping
def get_congestion_color(congestion_rate):
    if congestion_rate >= 0.7:
        return [255, 0, 0]  # Red for high congestion
    elif congestion_rate >= 0.4:
        return [255, 255, 0]  # Yellow for medium congestion
    else:
        return [0, 255, 0]  # Green for low congestion

# Create database connection
@st.cache_resource
def get_db_connection():
    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')
    return engine

# Get available routes with feature_table filtering
@st.cache_data
def get_route_options():
    """Fetch available route options from the database"""
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_pass,
            host=db_host,
            port=db_port
        )
        cur = conn.cursor()
        
        # First try with feature_table join
        query = """
        SELECT DISTINCT r.route_short_name, r.route_long_name
        FROM routes r
        JOIN trips t ON r.route_id = t.route_id
        JOIN feature_table ft ON t.trip_id = ft.trip_id_x
        ORDER BY r.route_short_name;
        """
        
        cur.execute(query)
        routes = cur.fetchall()
        
        # If no results, try without feature_table join
        if not routes:
            query = """
            SELECT DISTINCT r.route_short_name, r.route_long_name
            FROM routes r
            JOIN trips t ON r.route_id = t.route_id
            ORDER BY r.route_short_name;
            """
            cur.execute(query)
            routes = cur.fetchall()
        
        return [(route[0], f"{route[0]} - {route[1]}") for route in routes]
    except PostgresError as e:
        st.error(f"Database error: {str(e)}")
        return []
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

# Get route data with congestion information
def get_route_data(route_short_name):
    engine = get_db_connection()
    
    query = """
    WITH route_shapes AS (
        SELECT DISTINCT r.route_id, r.route_short_name, r.route_long_name, 
               t.trip_id, t.shape_id
        FROM routes r
        JOIN trips t ON r.route_id = t.route_id
        WHERE r.route_short_name = :route_short_name
        AND t.shape_id IS NOT NULL
        LIMIT 1
    ),
    latest_congestion AS (
        SELECT 
            trip_id_x as trip_id,
            congestion_rate,
            ROW_NUMBER() OVER (PARTITION BY trip_id_x ORDER BY timestamp_x DESC) as rn
        FROM feature_table
        WHERE congestion_rate IS NOT NULL
    )
    SELECT 
        s.shape_pt_lat as lat,
        s.shape_pt_lon as lon,
        s.shape_pt_sequence as sequence,
        NULL as stop_id,
        NULL as stop_name,
        'route' as point_type,
        COALESCE(lc.congestion_rate, 0.3) as congestion_rate
    FROM route_shapes rs
    JOIN shapes s ON rs.shape_id = s.shape_id
    LEFT JOIN latest_congestion lc ON lc.trip_id = rs.trip_id AND lc.rn = 1
    UNION ALL
    SELECT 
        st.stop_lat as lat,
        st.stop_lon as lon,
        stimes.stop_sequence as sequence,
        st.stop_id,
        st.stop_name,
        'stop' as point_type,
        NULL as congestion_rate
    FROM route_shapes rs
    JOIN stop_times stimes ON rs.trip_id = stimes.trip_id
    JOIN stops st ON stimes.stop_id = st.stop_id
    ORDER BY sequence;
    """
    
    return pd.read_sql(text(query), engine, params={"route_short_name": route_short_name})

# Get route options
route_options = get_route_options()

# Route selection
selected_route = st.selectbox(
    "Select a bus route:",
    options=[r[0] for r in route_options],
    format_func=lambda x: next((r[1] for r in route_options if r[0] == x), x)
)

if selected_route:
    # Get route data
    route_data = get_route_data(selected_route)
    
    if not route_data.empty:
        # Separate route points and stops
        route_points = route_data[route_data['point_type'] == 'route']
        stops = route_data[route_data['point_type'] == 'stop']
        
        # Create map placeholder for animation
        map_placeholder = st.empty()
        
        # Animate the bus along the route
        for i in range(len(route_points)):
            current_position = route_points.iloc[[i]]
            path_so_far = route_points.iloc[:i+1]
            
            # Create path segments with congestion colors
            path_segments = []
            for j in range(i):
                path_segments.append({
                    "path": [
                        [route_points.iloc[j]['lon'], route_points.iloc[j]['lat']],
                        [route_points.iloc[j+1]['lon'], route_points.iloc[j+1]['lat']]
                    ],
                    "color": get_congestion_color(route_points.iloc[j]['congestion_rate'])
                })
            
            # Bus icon layer
            bus_data = [{
                "lat": current_position['lat'].values[0],
                "lon": current_position['lon'].values[0],
                "icon_data": {
                    "url": BUS_ICON_URL,
                    "width": 128,
                    "height": 128,
                    "anchorY": 128
                }
            }]
            
            # Stops data
            stops_data = stops.apply(
                lambda x: {
                    "name": x['stop_name'],
                    "lat": x['lat'],
                    "lon": x['lon'],
                    "icon_data": {
                        "url": PIN_ICON_URL,
                        "width": 128,
                        "height": 128,
                        "anchorY": 128
                    }
                },
                axis=1
            ).tolist()
            
            # Create layers
            bus_layer = pdk.Layer(
                "IconLayer",
                data=bus_data,
                get_icon="icon_data",
                get_size=4,
                size_scale=15,
                get_position=["lon", "lat"],
                pickable=False,
            )
            
            path_layer = pdk.Layer(
                "PathLayer",
                data=path_segments,
                get_path="path",
                get_color="color",
                width_scale=20,
                width_min_pixels=2,
            )
            
            stop_layer = pdk.Layer(
                "IconLayer",
                data=stops_data,
                get_icon="icon_data",
                get_size=3,
                size_scale=8,
                get_position=["lon", "lat"],
                pickable=True,
                tooltip={"text": "{name}"}
            )
            
            # Calculate view state
            view_state = pdk.ViewState(
                latitude=current_position['lat'].values[0],
                longitude=current_position['lon'].values[0],
                zoom=14,
                pitch=45
            )
            
            # Create and display the map
            r = pdk.Deck(
                layers=[path_layer, stop_layer, bus_layer],
                initial_view_state=view_state,
                tooltip={"text": "{name}"}
            )
            
            map_placeholder.pydeck_chart(r)
            time.sleep(0.5)  # Adjust animation speed
            
        # Display stops information
        st.subheader("Bus Stops")
        stops_info = stops[['stop_name', 'sequence']].sort_values('sequence')
        st.dataframe(stops_info, hide_index=True)
    else:
        st.error("No route data found for the selected route.")
