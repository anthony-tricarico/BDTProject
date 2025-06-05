FROM python:3.10

WORKDIR /app
# COPY app.py .
COPY kafka-producer-gps/gps.py .
COPY utils/ utils/
COPY kafka-producer-gps/requirements.txt .

# RUN apt-get update && apt-get install -y gcc g++ build-essential
RUN pip install -r requirements.txt

CMD ["python", "-u", "gps.py"]