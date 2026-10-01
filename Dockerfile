FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY producer/ ./producer/
COPY consumer/ ./consumer/
COPY config/ ./config/