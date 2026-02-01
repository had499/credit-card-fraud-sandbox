#!/bin/bash
set -ex

MODEL_URI="models:/credit-card-fraud/2"
PORT=1234

echo "Resolving dependencies for $MODEL_URI ..."

# Ensure MLflow is installed
if ! command -v mlflow &> /dev/null; then
  echo "Installing MLflow..."
  pip install --no-cache-dir mlflow uvicorn
fi

# Use Python to extract model dependencies dynamically
python3 <<'PYCODE'
import mlflow, os, subprocess

model_uri = "models:/credit-card-fraud/2"
print(f"Downloading model from {model_uri}...")

# Download the model locally (temporary dir)
local_dir = "/tmp/model"
mlflow.artifacts.download_artifacts(artifact_uri=model_uri, dst_path=local_dir)

req_path = os.path.join(local_dir, "requirements.txt")
if os.path.exists(req_path):
    print(f"Installing dependencies from {req_path} ...")
    subprocess.run(["pip", "install", "--no-cache-dir", "-r", req_path], check=False)
else:
    print("No requirements.txt found in model artifacts.")
PYCODE

echo "Starting MLflow model server ..."
exec mlflow models serve -m "$MODEL_URI" -p "$PORT" --host 0.0.0.0 --no-conda
