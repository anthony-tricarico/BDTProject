FROM python:3.10-slim

WORKDIR /app
COPY kafka-producer-passengers/app.py .
COPY kafka-producer-passengers/passengers_with_shapes.csv .
COPY utils/ utils/
COPY kafka-producer-passengers/requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "app.py"]