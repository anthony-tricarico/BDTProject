import streamlit as st
import requests
import psycopg2
from datetime import datetime, time
import pandas as pd
import numpy as np
from typing import Optional

# Database connection parameters
DB_PARAMS = {
    'dbname': 'raw_data',
    'user': 'postgres',
    'password': 'example',
    'host': 'db',
    'port': '5432'
}

def create_predictions_table():
    """Create the predictions table if it doesn't exist"""
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS predictions (
        id SERIAL PRIMARY KEY,
        trip_id INTEGER,
        timestamp TIMESTAMP,
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
    cur.close()
    conn.close()

def save_prediction(prediction_data: dict, prediction_result: float):
    """Save the prediction to the database"""
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    
    insert_query = """
    INSERT INTO predictions (
        peak_hour, sine_time, temperature,
        precipitation_probability, weather_code, traffic_level,
        event_dummy, school, hospital, weekend, prediction
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    # Convert numpy values to Python native types
    values = (
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
    cur.close()
    conn.close()

def calculate_sine_time(timestamp: datetime) -> float:
    """Calculate sine of time for cyclical encoding"""
    seconds = timestamp.hour * 3600 + timestamp.minute * 60 + timestamp.second
    return np.sin(2 * np.pi * seconds / (24 * 3600))

def main():
    st.title("Bus Congestion Prediction")
    
    # Create the predictions table if it doesn't exist
    create_predictions_table()
    
    # Input form
    with st.form("prediction_form"):
        st.subheader("Enter Trip Details")
        
        # Trip ID
        trip_id = st.number_input("Trip ID", min_value=1, step=1)
        
        # Timestamp
        timestamp = st.time_input("Timestamp", time(10, 0))
        
        # Calculate sine_time from timestamp
        sine_time = calculate_sine_time(timestamp)
        
        # Peak hour (determined by time)
        peak_hour = st.checkbox("Is this during peak hours? (7-9 AM or 5-7 PM)")
        
        # Weather and traffic conditions
        temperature = st.number_input("Temperature (°C)", min_value=-50.0, max_value=50.0, value=20.0)
        precipitation_probability = st.slider("Precipitation Probability", 0.0, 100.0, 0.0) / 100.0
        weather_code = st.number_input("Weather Code", min_value=0, max_value=100, value=1)
        
        traffic_level = st.slider("Traffic Level (1-5)", 0, 3, 0)
        
        # Location and time features
        col1, col2 = st.columns(2)
        with col1:
            event_dummy = st.checkbox("Is there an event nearby?")
            school = st.checkbox("Is there a school nearby?")
        with col2:
            hospital = st.checkbox("Is there a hospital nearby?")
            weekend = st.checkbox("Is it weekend?")
        
        submitted = st.form_submit_button("Predict Congestion")
        
        if submitted:
            # Prepare prediction data
            prediction_data = {
                "trip_id": trip_id,
                "timestamp": timestamp.isoformat(),
                "peak_hour": peak_hour,
                "sine_time": sine_time,
                "temperature": temperature,
                "precipitation_probability": precipitation_probability,
                "weather_code": weather_code,
                "traffic_level": traffic_level,
                "event_dummy": event_dummy,
                "school": school,
                "hospital": hospital,
                "weekend": weekend
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
                    - 0.3-0.6: Moderate congestion
                    - 0.6-1.0: High congestion
                    """)
                else:
                    st.error(f"Error making prediction: {response.text}")
                    
            except Exception as e:
                st.error(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 