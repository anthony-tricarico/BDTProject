FROM python:3.10-slim

WORKDIR /app
# COPY app.py .
COPY kafka-producer-gps/gps.py .
COPY utils/ utils/
COPY kafka-producer-gps/requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "-u", "gps.py"]