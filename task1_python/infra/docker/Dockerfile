FROM python:3.14-slim

# Do not write bytecode files and keep logs unbuffered
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# System dependencies (psycopg likes these)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY ../../requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Project files
COPY ../.. .

CMD ["python", "app.py"]
