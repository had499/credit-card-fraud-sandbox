FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
build-essential \
curl \
iputils-ping \
&& apt-get clean

COPY requirements.txt .
COPY constraints.txt .
# Install Python packages
RUN pip install --upgrade pip 
RUN pip install -r requirements.txt -c constraints.txt

CMD ["mlflow", "server", "--backend-store-uri", "sqlite:///mlflow.db", "--default-artifact-root", "./mlruns", "--host", "0.0.0.0", "--port", "5000"]
