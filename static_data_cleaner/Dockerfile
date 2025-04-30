FROM python:latest

WORKDIR /app

COPY . .

RUN pip install pandas

# this is basically the command which will be run in the terminal once the container starts up
CMD ["python", "data_cleaner.py"]