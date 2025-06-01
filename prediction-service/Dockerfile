FROM python:3.10-slim

WORKDIR /app

COPY prediction_service.py .
COPY requirements.txt .

RUN pip install -r requirements.txt

# Force Python to run unbuffered
ENV PYTHONUNBUFFERED=1

# Run Python with -u flag for unbuffered output
CMD ["python", "-u", "prediction_service.py"] 