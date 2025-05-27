from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import joblib
import io
from minio import Minio
import numpy as np
from typing import List, Optional
import time
import pandas as pd
import xgboost as xgb  # Add XGBoost import

app = FastAPI(title="Bus Congestion Prediction Service")

# Initialize MinIO client
minio_client = Minio(
    "minio:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# Global variable to store the loaded model
model = None
last_model_load_time = 0
MODEL_RELOAD_INTERVAL = 300  # Reload model every 5 minutes

class PredictionInput(BaseModel):
    trip_id: int
    timestamp: str
    peak_hour: bool
    sine_time: float
    # seconds_from_midnight: int
    temperature: float
    precipitation_probability: float
    weather_code: int
    traffic_level: int
    event_dummy: bool
    school: bool
    hospital: bool
    weekend: bool

def load_model():
    """Load the champion model from MinIO"""
    global model, last_model_load_time
    
    try:
        # Get the model file from MinIO
        response = minio_client.get_object("models", "champion/latest_rf.pkl")
        model_data = response.read()
        
        # Load the model using joblib
        model = joblib.load(io.BytesIO(model_data))
        last_model_load_time = time.time()
        print("Successfully loaded champion model")
        return True
    except Exception as e:
        print(f"Error loading model: {e}")
        return False

@app.on_event("startup")
async def startup_event():
    """Load the model when the service starts"""
    while model is None:
        try:
            load_model()
        except Exception as e:
            print(f"Error loading model: {e}, retrying in 60 seconds...")
            time.sleep(60)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "model_loaded": model is not None}

@app.post("/predict")
async def predict(input_data: PredictionInput):
    """Make predictions using the champion model"""
    global model, last_model_load_time
    
    # Check if we need to reload the model
    if time.time() - last_model_load_time > MODEL_RELOAD_INTERVAL:
        if not load_model():
            raise HTTPException(status_code=500, detail="Failed to reload model")
    
    if model is None:
        raise HTTPException(status_code=500, detail="Model not loaded")
    
    try:
        # Prepare input features
        features = pd.DataFrame(
            {
                "peak_hour": [input_data.peak_hour],
                # "seconds_from_midnight": [input_data.seconds_from_midnight],
                "sine_time": [input_data.sine_time],
                "temperature": [input_data.temperature],
                "precipitation_probability": [input_data.precipitation_probability],
                "weather_code": [input_data.weather_code],
                "traffic_level": [input_data.traffic_level],
                "event_dummy": [input_data.event_dummy],
                "school": [input_data.school],
                "hospital": [input_data.hospital],
                "weekend": [input_data.weekend]
            }
        )
        
        # Convert to DMatrix for XGBoost
        dmatrix = xgb.DMatrix(features)
        
        # Make prediction
        prediction = model.predict(dmatrix)[0]
        
        return {
            "prediction": float(prediction),
            "model_timestamp": last_model_load_time
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8006) 