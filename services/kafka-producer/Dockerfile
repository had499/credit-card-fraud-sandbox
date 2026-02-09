FROM python:3.11-slim

WORKDIR /app

# Install only essential build dependencies (cached layer)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Start FastAPI app using uvicorn with workers for better performance
CMD ["uvicorn", "producer:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--log-level", "info"]
