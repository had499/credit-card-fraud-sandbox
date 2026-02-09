FROM python:3.11-slim

WORKDIR /app
COPY ws_server.py .
RUN pip install --no-cache-dir "uvicorn[standard]>=0.23" "fastapi>=0.95"

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8002/health')" || exit 1

EXPOSE 8002
CMD ["uvicorn", "ws_server:app", "--host", "0.0.0.0", "--port", "8002", "--log-level", "info"]
