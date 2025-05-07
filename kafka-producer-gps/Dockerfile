FROM python:3.10-slim

WORKDIR /app
# COPY app.py .
COPY gps.py .

RUN pip install kafka-python requests sqlalchemy psycopg2-binary numpy

CMD ["python", "-u", "gps.py"]