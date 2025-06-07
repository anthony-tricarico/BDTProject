from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from training_pipeline import run_training_pipeline
import uvicorn
import asyncio
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import os
import time

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create a thread pool executor
executor = ThreadPoolExecutor(max_workers=1)

async def run_training_with_timeout():
    """Run training pipeline with timeout"""
    try:
        # Run training in a separate thread with a timeout
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, run_training_pipeline)
        return result
    except Exception as e:
        print(f"Error during training: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/train")
async def train_model():
    """Endpoint to trigger model training"""
    try:
        # Set a timeout of 10 minutes for the entire training process
        result = await asyncio.wait_for(run_training_with_timeout(), timeout=600)
        if not result:
            raise HTTPException(status_code=500, detail="Training failed with no result")
        return result
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Training timed out after 10 minutes")
    except Exception as e:
        print(f"Error in train_model endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8007)