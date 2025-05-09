FROM python:3.10-slim

WORKDIR /app
# COPY app.py .
COPY weather.py .

RUN pip install kafka-python requests numpy

CMD ["python", "weather.py"]