FROM python:3.10-slim

WORKDIR /app
COPY app.py .
COPY treemodel.pkl .
COPY treemodel_out.pkl .
COPY labelencoder.pkl .
COPY route_to_stops.json .

RUN pip install pandas numpy kafka-python scikit-learn

CMD ["python", "app.py"]