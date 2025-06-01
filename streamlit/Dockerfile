FROM python:3.9-slim

WORKDIR /app

# RUN apt-get update && apt-get install -y \
#     build-essential \
#     curl \
#     software-properties-common \
#     git \
#     && rm -rf /var/lib/apt/lists/*

# COPY requirements.txt .
# COPY streamlit_app.py .

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["streamlit", "run", "manager.py", "--server.port=8501", "--server.address=0.0.0.0"]