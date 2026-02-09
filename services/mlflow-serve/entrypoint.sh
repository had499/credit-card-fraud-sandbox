#!/bin/bash
set -e  # Exit on any error

MODEL_URI="models:/credit-card-fraud/2"
PORT=1234
MAX_RETRIES=3
RETRY_DELAY=5

echo "Resolving dependencies for $MODEL_URI ..."

# Retry logic for downloading model
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    # Ensure MLflow is installed
    if ! command -v mlflow &> /dev/null; then
      echo "Installing MLflow..."
      pip install --no-cache-dir mlflow uvicorn
    fi

    # Use Python to extract model dependencies dynamically
    python3 <<'PYCODE'
import mlflow
import os
import subprocess
import sys

model_uri = "models:/credit-card-fraud/2"
print(f"Downloading model from {model_uri}...")

try:
    # Download the model locally (temporary dir)
    local_dir = "/tmp/model"
    mlflow.artifacts.download_artifacts(artifact_uri=model_uri, dst_path=local_dir)

    req_path = os.path.join(local_dir, "requirements.txt")
    if os.path.exists(req_path):
        print(f"Installing dependencies from {req_path} ...")
        result = subprocess.run(["pip", "install", "--no-cache-dir", "-r", req_path], check=False)
        if result.returncode != 0:
            print("Warning: Some dependencies failed to install, continuing anyway...")
    else:
        print("No requirements.txt found in model artifacts.")
    
    sys.exit(0)
except Exception as e:
    print(f"Error downloading model: {e}", file=sys.stderr)
    sys.exit(1)
PYCODE

    if [ $? -eq 0 ]; then
        echo "Successfully downloaded and configured model"
        break
    fi
    
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
        echo "Failed to download model, retrying in ${RETRY_DELAY}s (attempt $((RETRY_COUNT+1))/$MAX_RETRIES)..."
        sleep $RETRY_DELAY
    else
        echo "Failed to download model after $MAX_RETRIES attempts"
        exit 1
    fi
done

echo "Starting MLflow model server on port $PORT ..."
exec mlflow models serve -m "$MODEL_URI" -p "$PORT" --host 0.0.0.0 --no-conda
