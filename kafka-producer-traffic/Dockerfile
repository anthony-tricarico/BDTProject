FROM python:3.10-slim

WORKDIR /app
# COPY app.py .
COPY traffic.py .

RUN pip install kafka-python requests sqlalchemy psycopg2-binary

CMD ["python", "-u", "traffic.py"]