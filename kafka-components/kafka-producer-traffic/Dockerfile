FROM python:3.10-slim

WORKDIR /app
# COPY app.py .
COPY kafka-producer-traffic/traffic.py .
COPY kafka-producer-traffic/.env .
COPY utils/ utils/

RUN pip install kafka-python requests sqlalchemy psycopg2-binary python-dotenv redis tenacity

CMD ["python", "-u", "traffic.py"]