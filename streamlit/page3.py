import streamlit as st
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
import time
from typing import List, Dict
import pytz
import altair as alt

st.set_page_config(layout="wide")
st.title("📈 Congestion Forecast")

# Define Italy timezone
ITALY_TZ = pytz.timezone('Europe/Rome')

# Database connection parameters
DB_PARAMS = {
    'dbname': 'raw_data',
    'user': 'postgres',
    'password': 'example',
    'host': 'db',
    'port': '5432'
}

def get_route_options():
    """Fetch available route options from the database"""
    import psycopg2
    from psycopg2 import Error as PostgresError
    
    try:
        conn = psycopg2.connect(**DB_PARAMS)
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

def calculate_sine_time(timestamp: datetime) -> float:
    """Calculate sine of time for cyclical encoding"""
    seconds = timestamp.hour * 3600 + timestamp.minute * 60 + timestamp.second
    return np.sin(2 * np.pi * seconds / (24 * 3600))

def get_location_features(route_short_name: str) -> dict:
    """Get school and hospital features for a route from the database"""
    import psycopg2
    from psycopg2 import Error as PostgresError
    
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        
        query = """
        SELECT DISTINCT ft.school, ft.hospital
        FROM feature_table ft
        JOIN trips t ON ft.trip_id_x = t.trip_id
        JOIN routes r ON t.route_id = r.route_id
        WHERE r.route_short_name = %s
        LIMIT 1;
        """
        
        cur.execute(query, (route_short_name,))
        result = cur.fetchone()
        
        if result is None:
            return {'school': False, 'hospital': False}
        
        return {
            'school': bool(result[0]),
            'hospital': bool(result[1])
        }
    except PostgresError as e:
        st.error(f"Database error: {str(e)}")
        return {'school': False, 'hospital': False}
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def get_predictions_for_timeframe(
    route_short_name: str,
    start_time: datetime,
    num_predictions: int,
    interval_minutes: int,
    temperature: float,
    precipitation_probability: float,
    weather_code: int,
    traffic_level: int,
    event_dummy: bool
) -> List[Dict]:
    """Get predictions for multiple future timestamps"""
    predictions = []
    location_features = get_location_features(route_short_name)
    
    for i in range(num_predictions):
        timestamp = start_time + timedelta(minutes=i * interval_minutes)
        
        # Calculate time-based features
        is_weekend = timestamp.weekday() >= 5
        is_peak_hour = (timestamp.hour >= 7 and timestamp.hour <= 9) or (timestamp.hour >= 17 and timestamp.hour <= 19)
        sine_time = calculate_sine_time(timestamp)
        
        # Prepare prediction request
        prediction_data = {
            "trip_id": str(route_short_name)[0],
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "peak_hour": is_peak_hour,
            "sine_time": sine_time,
            "temperature": temperature,
            "precipitation_probability": precipitation_probability,
            "weather_code": weather_code,
            "traffic_level": traffic_level,
            "event_dummy": event_dummy,
            "school": location_features['school'],
            "hospital": location_features['hospital'],
            "weekend": is_weekend
        }
        
        try:
            # Make prediction request
            response = requests.post(
                "http://prediction-service:8006/predict",
                json=prediction_data,
                timeout=5
            )
            
            if response.status_code == 200:
                result = response.json()
                predictions.append({
                    "timestamp": timestamp,
                    "prediction": result["prediction"]
                })
            else:
                # st.error(f"Error making prediction: {response.text}")
                st.warning("Waiting for the prediction service to be ready...")
                return []
                
        except Exception as e:
            # st.error(f"Error: {str(e)}")
            st.warning("Waiting for the prediction service to be ready...")
            return []
            
    return predictions


# Get route options
route_options = get_route_options()

# Route selection
selected_route = st.selectbox(
    "Select Route",
    options=[r[0] for r in route_options],
    format_func=lambda x: next((r[1] for r in route_options if r[0] == x), x)
)

# Forecast settings
col1, col2 = st.columns(2)

with col1:
    forecast_window = st.slider(
        "Forecast Window (minutes)",
        min_value=15,
        max_value=120,
        value=30,
        step=15
    )
    
    interval = st.slider(
        "Prediction Interval (minutes)",
        min_value=5,
        max_value=15,
        value=5,
        step=5
    )

with col2:
    temperature = st.number_input("Temperature (°C)", min_value=-50.0, max_value=50.0, value=20.0)
    precipitation_prob = st.slider("Precipitation Probability", 0.0, 100.0, 0.0) / 100.0
    weather_code = st.number_input("Weather Code", min_value=0, max_value=100, value=1)
    traffic_level = st.slider("Traffic Level (1-3)", 0, 3, 1)
    event_dummy = st.checkbox("Is there an event scheduled nearby?")

if selected_route:
    st.subheader("Congestion Forecast")
    st.info("Model predictions for upcoming congestion on the selected route.")
    
    # Calculate number of predictions needed
    num_predictions = forecast_window // interval
    
    # Get predictions using Italy timezone
    predictions = get_predictions_for_timeframe(
        route_short_name=selected_route,
        start_time=datetime.now(ITALY_TZ),
        num_predictions=num_predictions,
        interval_minutes=interval,
        temperature=temperature,
        precipitation_probability=precipitation_prob,
        weather_code=weather_code,
        traffic_level=traffic_level,
        event_dummy=event_dummy
    )
    
    if predictions:
        # Create forecast DataFrame
        forecast_df = pd.DataFrame(predictions)
        
        # Create base chart with selection
        selection = alt.selection_interval(bind='scales')  # Enable zoom and pan
        
        # Create Altair chart with 24h time format and interactivity
        chart = alt.Chart(forecast_df).mark_line(
            point=True  # Add points at each prediction
        ).encode(
            x=alt.X('timestamp:T',
                   title='Time',
                   axis=alt.Axis(
                       format='%H:%M',  # 24h time format
                       labelAngle=-45,
                       grid=True
                   )),
            y=alt.Y('prediction:Q',
                   title='Congestion Level',
                   scale=alt.Scale(domain=[0, 1])),
            tooltip=[
                alt.Tooltip('timestamp:T', title='Time', format='%H:%M'),
                alt.Tooltip('prediction:Q', title='Congestion', format='.2f')
            ]
        ).add_selection(
            selection  # Add zoom and pan interaction
        ).configure_axis(
            grid=True,
            gridOpacity=0.2,
            domainOpacity=0.8,
            labelColor='#FAFAFA',
            titleColor='#FAFAFA'
        ).configure_view(
            strokeWidth=0
        ).properties(
            height=400
        )
        
        # Display the chart
        st.altair_chart(chart, use_container_width=True)
        
        st.info(f"Showing predicted congestion for the next {forecast_window} minutes.")
        
        # Calculate average congestion
        avg_congestion = forecast_df["prediction"].mean()
        
        # Congestion level interpretation
        if avg_congestion >= 0.8:
            congestion_level = "High"
            color = "🔴"
        elif avg_congestion >= 0.3:
            congestion_level = "Moderate"
            color = "🟡"
        else:
            congestion_level = "Low"
            color = "🟢"
        
        # Display current congestion status
        st.subheader("Current Status")
        st.markdown(f"### {color} {congestion_level} Congestion")
        st.metric("Average Congestion Level", f"{avg_congestion:.2f}")
        
        # Recommendations based on congestion level
        st.subheader("📋 Recommendations")
        if congestion_level == "High":
            st.error("Consider deploying additional buses or suggesting alternative routes to passengers.")
        elif congestion_level == "Moderate":
            st.warning("Monitor the situation closely and prepare for potential congestion increases.")
        else:
            st.success("Normal operations - no immediate actions required.")
            
else:
    st.error("Please select a valid route to view predictions.")
