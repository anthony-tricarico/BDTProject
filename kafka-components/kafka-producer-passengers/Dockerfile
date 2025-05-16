FROM python:3.10-slim

WORKDIR /app
# COPY app.py .
COPY kafka-producer-passengers/app.py .
COPY kafka-producer-passengers/treemodel.pkl .
COPY kafka-producer-passengers/treemodel_out.pkl .
COPY kafka-producer-passengers/labelencoder.pkl .
# COPY kafka-producer-passengers/stop_times_passengers_shapes.csv .
COPY kafka-producer-passengers/passengers_with_shapes.csv .
COPY utils/ utils/

RUN pip install pandas numpy kafka-python scikit-learn sqlalchemy psycopg2-binary

CMD ["python", "app.py"]