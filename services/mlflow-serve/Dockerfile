# Base Python image
FROM python:3.12-slim

# Minimal system dependencies
RUN apt-get update && apt-get install -y bash curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install MLflow and any base dependencies
RUN pip install --no-cache-dir mlflow==2.22.1 uvicorn imbalanced-learn==0.13.0

# Copy the entrypoint into /app
COPY entrypoint.sh /app/entrypoint.sh

# Make it executable
RUN chmod +x /app/entrypoint.sh

EXPOSE 1234

ENV MLFLOW_TRACKING_URI=http://mlflow:5000

ENTRYPOINT ["bash", "/app/entrypoint.sh"]
