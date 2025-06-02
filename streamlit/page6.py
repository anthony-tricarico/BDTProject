import streamlit as st
import requests
import psycopg2
from datetime import datetime, time, date
import pandas as pd
import numpy as np
from typing import Optional
import time as time_module
from psycopg2 import Error as PostgresError

# Database connection parameters
DB_PARAMS = {
    'dbname': 'raw_data',
    'user': 'postgres',
    'password': 'example',
    'host': 'db',
    'port': '5432'
}

def execute_with_retry(operation_name: str, query_func, max_retries: int = 5, retry_delay: int = 60):
    """
    Execute a database operation with retry logic
    
    Args:
        operation_name: Name of the operation for error messages
        query_func: Function that performs the database operation
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries in seconds
    """
    for attempt in range(max_retries):
        try:
            return query_func()
        except PostgresError as e:
            if attempt == max_retries - 1:
                st.error(f"Failed to {operation_name} after {max_retries} attempts: {str(e)}")
                return None
            else:
                st.warning(f"Attempt {attempt + 1}/{max_retries} to {operation_name} failed. Retrying in {retry_delay} seconds...")
                time_module.sleep(retry_delay)
                continue
        except Exception as e:
            st.error(f"Unexpected error while trying to {operation_name}: {str(e)}")
            return None

def get_route_options():
    """Fetch available route options from the database"""
    def query_func():
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        
        try:
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
        finally:
            cur.close()
            conn.close()
    
    result = execute_with_retry("fetch route options", query_func)
    return result if result else [("", "No routes available")]

def create_predictions_table():
    """Create the predictions table if it doesn't exist"""
    def query_func():
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        
        try:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS predictions (
                id SERIAL PRIMARY KEY,
                route_short_name VARCHAR,
                timestamp VARCHAR,
                peak_hour BOOLEAN,
                sine_time FLOAT,
                temperature FLOAT,
                precipitation_probability FLOAT,
                weather_code INTEGER,
                traffic_level INTEGER,
                event_dummy BOOLEAN,
                school BOOLEAN,
                hospital BOOLEAN,
                weekend BOOLEAN,
                prediction FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            cur.execute(create_table_query)
            conn.commit()
        finally:
            cur.close()
            conn.close()
    
    execute_with_retry("create predictions table", query_func)

def get_location_features(route_short_name: str) -> dict:
    """Get school and hospital features for a route from the database"""
    def query_func():
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        
        try:
            # First try with feature_table
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
                # If no result from feature_table, return default values
                return {
                    'school': False,
                    'hospital': False
                }
            
            return {
                'school': bool(result[0]),
                'hospital': bool(result[1])
            }
        finally:
            cur.close()
            conn.close()
    
    result = execute_with_retry("fetch location features", query_func)
    return result if result else {'school': False, 'hospital': False}

def save_prediction(prediction_data: dict, prediction_result: float):
    """Save the prediction to the database"""
    def query_func():
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        
        try:
            insert_query = """
            INSERT INTO predictions (
                route_short_name, timestamp, peak_hour, sine_time, temperature,
                precipitation_probability, weather_code, traffic_level,
                event_dummy, school, hospital, weekend, prediction
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            
            # Format timestamp as string
            timestamp_str = prediction_data['timestamp']
            if isinstance(timestamp_str, datetime):
                timestamp_str = timestamp_str.isoformat()
            
            # Convert numpy values to Python native types
            values = (
                str(prediction_data['trip_id']),
                timestamp_str,  # Using formatted timestamp string
                bool(prediction_data['peak_hour']),
                float(prediction_data['sine_time']),
                float(prediction_data['temperature']),
                float(prediction_data['precipitation_probability']),
                int(prediction_data['weather_code']),
                int(prediction_data['traffic_level']),
                bool(prediction_data['event_dummy']),
                bool(prediction_data['school']),
                bool(prediction_data['hospital']),
                bool(prediction_data['weekend']),
                float(prediction_result)
            )
            
            cur.execute(insert_query, values)
            conn.commit()
        finally:
            cur.close()
            conn.close()
    
    execute_with_retry("save prediction", query_func)

def calculate_sine_time(timestamp: datetime) -> float:
    """Calculate sine of time for cyclical encoding"""
    seconds = timestamp.hour * 3600 + timestamp.minute * 60 + timestamp.second
    return np.sin(2 * np.pi * seconds / (24 * 3600))

def main():
    st.title("Bus Congestion Prediction")
    
    # Create the predictions table if it doesn't exist
    create_predictions_table()
    
    # Get route options
    route_options = get_route_options()
    
    # Input form
    with st.form("prediction_form"):
        st.subheader("Enter Trip Details")
        
        # Route selection dropdown
        route_short_name = st.selectbox(
            "Select Route",
            options=[r[0] for r in route_options],
            format_func=lambda x: next((r[1] for r in route_options if r[0] == x), x)
        )
        
        # Date and Time inputs
        selected_date = st.date_input("Date", date.today())
        selected_time = st.time_input("Time", time(10, 0))
        
        # Combine date and time into timestamp
        timestamp = datetime.combine(selected_date, selected_time)
        
        # Calculate sine_time from timestamp
        sine_time = calculate_sine_time(timestamp)
        
        # Determine if it's a weekend
        is_weekend = selected_date.weekday() >= 5  # 5 is Saturday, 6 is Sunday
        
        # Peak hour (determined by time)
        peak_hour = True if (selected_time.hour >= 7 and selected_time.hour <= 9) or (selected_time.hour >= 17 and selected_time.hour <= 19) else False
        
        # Weather and traffic conditions
        temperature = st.number_input("Temperature (°C)", min_value=-50.0, max_value=50.0, value=20.0)
        precipitation_probability = st.slider("Precipitation Probability", 0.0, 100.0, 0.0) / 100.0
        weather_code = st.number_input("Weather Code", min_value=0, max_value=100, value=1)
        
        traffic_level = st.slider("Traffic Level (1-3)", 0, 3, 0)
        
        # Event dummy only (weekend is determined from date)
        event_dummy = st.checkbox("Is there an event scheduled nearby?")
        
        # Display weekend status (informational)
        st.info(f"Selected date is a {'weekend' if is_weekend else 'weekday'}")
        
        submitted = st.form_submit_button("Predict Congestion")
        
        if submitted and route_short_name:
            # Get location features for the selected route
            location_features = get_location_features(route_short_name)
            
            # Format timestamp as string in ISO format
            timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
            
            # Prepare prediction data
            prediction_data = {
                "trip_id": route_short_name,
                "timestamp": timestamp_str,  
                "peak_hour": peak_hour,
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
                    json=prediction_data
                )
                
                if response.status_code == 200:
                    result = response.json()
                    prediction = result["prediction"]
                    
                    # Save prediction to database
                    save_prediction(prediction_data, prediction)
                    
                    # Display result
                    st.success(f"Predicted Congestion Level: {prediction:.2f}")
                    
                    # Show interpretation
                    st.info("""
                    Congestion Level Interpretation:
                    - 0-0.3: Low congestion
                    - 0.3-0.8: Moderate congestion
                    - Higher than 0.8: High congestion
                    """)
                else:
                    st.error(f"Error making prediction: {response.text}")
                    
            except Exception as e:
                st.error(f"Error: {str(e)}")
        elif submitted:
            st.error("Please select a valid route before submitting.")

if __name__ == "__main__":
    main() 