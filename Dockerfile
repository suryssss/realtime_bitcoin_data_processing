# ==============================================================================
# MarketPulse — Real-Time Coinbase Ingestion Service
# ==============================================================================
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application artifacts
COPY schemas/ ./schemas/
COPY ingestion/ ./ingestion/

# Run as non-privileged service user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Start streaming ingestion service
CMD ["python", "ingestion/websocket_client.py"]

