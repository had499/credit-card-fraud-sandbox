FROM python:3.12

WORKDIR /app

RUN apt-get update && apt-get install -y \
build-essential \
curl \
iputils-ping \
&& apt-get clean

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_CACHE_DIR=/root/.cache/pip

COPY requirements.txt .
COPY constraints.txt .

# Install everything *except* mlflow
RUN pip install --no-cache-dir -r requirements.txt -c constraints.txt

# Now install mlflow separately
RUN pip install --no-cache-dir mlflow==2.22.1


COPY . .

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--ServerApp.token=''", "--ServerApp.password=''"]


