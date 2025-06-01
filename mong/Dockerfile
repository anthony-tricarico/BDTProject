FROM python:3.10-slim

WORKDIR /app
COPY main.py .
COPY requirements.txt .

# RUN pip install fastapi pymongo httpx uvicorn
RUN pip install -r requirements.txt

# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8003"]
CMD ["python", "main.py"]