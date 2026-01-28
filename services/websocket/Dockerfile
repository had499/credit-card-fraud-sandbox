FROM python:3.11-slim

WORKDIR /app
COPY ws_server.py .
RUN pip install "uvicorn[standard]" fastapi

EXPOSE 8002
CMD ["uvicorn", "ws_server:app", "--host", "0.0.0.0", "--port", "8002"]
