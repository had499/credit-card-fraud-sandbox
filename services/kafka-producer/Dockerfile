FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for numpy/scipy
RUN apt-get update && apt-get install -y \
    build-essential \
    gfortran \
    libopenblas-dev \
    liblapack-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Start FastAPI app using uvicorn
CMD ["uvicorn", "producer:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "debug"]
