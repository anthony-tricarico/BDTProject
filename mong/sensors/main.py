from fastapi import FastAPI
from pymongo import MongoClient, UpdateOne
import httpx

app = FastAPI()

# MongoDB connection
client = MongoClient("mongodb://mongodb:27017")
db = client["raw"]
collection_sensors = db["sensors"]
collection_tickets = db["tickets"]

# API endpoint: sensors
SOURCE_API_SENSORS = "http://kafka-consumer-sensors:8002/stream_sensors"
SOURCE_API_TICKETS = "http://kafka-consumer-tickets:8001/stream_tickets"

@app.get("/")
def health():
    return {"status": "ok"}

# post data from sensors API to MongoDB
@app.post("/pull-and-store-sensors")
async def pull_and_store():
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(SOURCE_API_SENSORS)
            response.raise_for_status()
            try:
                data = response.json()
            except Exception as e:
                return {"status": "error", "detail": f"JSON parse error: {str(e)}"}

        if not data:
            return {"status": "error", "detail": "No data received"}

        if isinstance(data, list):
            bulk_ops = []
            for entry in data:
                sensor_id = entry.get("measurement_id")
                if not sensor_id:
                    continue
                bulk_ops.append(
                    UpdateOne({"sensor_id": sensor_id}, {"$set": entry}, upsert=True)
                )
            if bulk_ops:
                result = collection_sensors.bulk_write(bulk_ops)
                return {"status": "stored", "inserted": result.upserted_count, "updated": result.modified_count}
        else:
            sensor_id = data.get("measurement_id")
            if sensor_id:
                collection_sensors.update_one({"sensor_id": sensor_id}, {"$set": data}, upsert=True)
                return {"status": "stored", "records": 1}
            return {"status": "error", "detail": "Missing measurement_id"}

    except Exception as e:
        # Log or return full error for debugging
        return {"status": "error", "detail": f"Unhandled exception: {str(e)}"}

@app.post("/pull-and-store-tickets")
async def pull_and_store():
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(SOURCE_API_TICKETS)
            response.raise_for_status()
            try:
                data = response.json()
            except Exception as e:
                return {"status": "error", "detail": f"JSON parse error: {str(e)}"}

        if not data:
            return {"status": "error", "detail": "No data received"}

        if isinstance(data, list):
            bulk_ops = []
            for entry in data:
                ticket_id = entry.get("ticket_id")
                if not ticket_id:
                    continue
                bulk_ops.append(
                    UpdateOne({"ticket_id": ticket_id}, {"$set": entry}, upsert=True)
                )
            if bulk_ops:
                result = collection_tickets.bulk_write(bulk_ops)
                return {"status": "stored", "inserted": result.upserted_count, "updated": result.modified_count}
        else:
            ticket_id = data.get("ticket_id")
            if ticket_id:
                collection_tickets.update_one({"sensor_id": ticket_id}, {"$set": data}, upsert=True)
                return {"status": "stored", "records": 1}
            return {"status": "error", "detail": "Missing measurement_id"}

    except Exception as e:
        # Log or return full error for debugging
        return {"status": "error", "detail": f"Unhandled exception: {str(e)}"}