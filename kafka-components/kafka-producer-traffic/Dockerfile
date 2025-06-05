FROM python:3.10

WORKDIR /app
# COPY app.py .
COPY kafka-producer-traffic/traffic.py .
# COPY kafka-producer-traffic/.env .
COPY utils/ utils/
COPY kafka-producer-traffic/requirements.txt .

# RUN apt-get update && apt-get install -y gcc g++ build-essential
RUN pip install -r requirements.txt

CMD ["python", "-u", "traffic.py"]