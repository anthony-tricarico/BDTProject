FROM python:3.10-slim

WORKDIR /app
# COPY app.py .
COPY app_test.py .
COPY treemodel.pkl .
COPY treemodel_out.pkl .
COPY labelencoder.pkl .
COPY route_to_stops.json .
COPY stop_times_passengers_shapes.csv .

RUN pip install pandas numpy kafka-python scikit-learn

# CMD ["python", "app.py"]
CMD ["python", "app_test.py"]