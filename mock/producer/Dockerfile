FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mock/producer mock/producer

COPY backend/ backend/

ENV PYTHONPATH=/app


CMD ["python", "-m", "mock.producer.producer"]