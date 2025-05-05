FROM python:3.10-slim

WORKDIR /app
COPY main.py .

# RUN pip install fastapi pymongo httpx uvicorn
RUN pip install kafka-python pymongo

# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8003"]
CMD ["python", "main.py"]