FROM python:3.10-slim

WORKDIR /app

COPY prediction_service.py .

RUN pip install fastapi uvicorn scikit-learn joblib minio numpy pydantic pandas xgboost>=1.7.0

# Force Python to run unbuffered
ENV PYTHONUNBUFFERED=1

# Run Python with -u flag for unbuffered output
CMD ["python", "-u", "prediction_service.py"] 