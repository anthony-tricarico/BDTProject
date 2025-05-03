FROM python:3.10-slim

WORKDIR /app
COPY app.py .
COPY treemodel.pkl .
COPY labelencoder.pkl .

RUN pip install pandas numpy kafka-python scikit-learn

CMD ["python", "app.py"]