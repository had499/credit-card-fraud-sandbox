# kafka_consumer_ws.py
import json
import os
import asyncio
import pandas as pd
import requests
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import websockets
import time
import sys

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "transactions"
MLFLOW_PREDICT_URL = "http://mlflow-serve:1234/invocations"
WS_SERVER_URL = "ws://websocket:8002/ws"

# Kafka connection settings with timeouts
KAFKA_CONNECTION_TIMEOUT = 10000  # 10 seconds
KAFKA_SESSION_TIMEOUT = 30000  # 30 seconds
KAFKA_HEARTBEAT_INTERVAL = 10000  # 10 seconds

# Global WebSocket connection
ws_connection = None


def predict_with_mlflow(df: pd.DataFrame):
    """Send features to MLflow for fraud prediction."""
    headers = {"Content-Type": "application/json"}
    payload = {"inputs": df.to_dict(orient="list")}
    try:
        resp = requests.post(MLFLOW_PREDICT_URL, headers=headers, json=payload, timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"[ERROR] MLflow request failed: {e}", flush=True)
        return None


async def maintain_ws_connection():
    """Maintain a persistent WebSocket connection, reconnecting if needed."""
    global ws_connection
    while True:
        try:
            print(f"🔌 Connecting to WebSocket server {WS_SERVER_URL}...", flush=True)
            async with websockets.connect(WS_SERVER_URL) as websocket:
                ws_connection = websocket
                
                # Identify as a producer (sends data, doesn't receive)
                await websocket.send(json.dumps({"type": "identify", "role": "producer"}))
                print(f"Connected to WebSocket server as producer", flush=True)
                
                # Keep connection alive - this will block until connection closes
                await websocket.wait_closed()
        except Exception as e:
            print(f"[ERROR] WebSocket connection lost: {e}, reconnecting in 3s...", flush=True)
            ws_connection = None
            await asyncio.sleep(3)


async def publish_to_ws(message: dict):
    """Send message through persistent WebSocket connection."""
    global ws_connection
    if ws_connection is None:
        print(f"[WARN] WebSocket not connected, message not sent", flush=True)
        return
    
    try:
        await ws_connection.send(json.dumps(message))
        print(f"Sent message via WebSocket", flush=True)
    except Exception as e:
        print(f"[ERROR] Failed to send WebSocket message: {e}", flush=True)
        ws_connection = None


def kafka_listener(loop: asyncio.AbstractEventLoop):
    """Kafka consumer loop running in sync, scheduling async WebSocket sends."""
    import time
    
    retry_count = 0
    max_retries = 5
    
    while True:
        try:
            retry_count = 0
            # Use unique consumer group per session to always read new messages
            group_id = f"ml-inference-consumer-{int(time.time())}"
            
            print(f"[INFO] Attempting to connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...", flush=True)
            
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id=group_id,
                # Connection timeouts to prevent hanging
                connections_max_idle_ms=KAFKA_CONNECTION_TIMEOUT,
                session_timeout_ms=KAFKA_SESSION_TIMEOUT,
                heartbeat_interval_ms=KAFKA_HEARTBEAT_INTERVAL,
                request_timeout_ms=15000,  # 15 second request timeout
                api_version_auto_discovery_interval_ms=10000,
            )
            
            print(f"[SUCCESS] Connected to Kafka. Listening on topic '{TOPIC}' with group_id={group_id}...", flush=True)

            for msg in consumer:
                try:
                    data = msg.value
                    print(f"[DEBUG] Received message: transaction_index={data.get('transaction_index')}, run_id={data.get('run_id')}", flush=True)
                    
                    samples = data.get("features")
                    columns = data.get("feature_names")
                    if not samples or not columns:
                        print(f"[WARN] Skipping message - missing features or feature_names", flush=True)
                        continue

                    df = pd.DataFrame([samples], columns=columns)
                    prediction = predict_with_mlflow(df)

                    result = {
                        "transaction_index": data.get("transaction_index"),
                        "prediction": prediction,
                        "is_fraud": data.get("is_fraud"),
                        "amount": data.get("amount"),
                        "timestamp": data.get("timestamp"),
                        "run_id": data.get("run_id"),
                    }

                    # Schedule WebSocket send in the async loop
                    asyncio.run_coroutine_threadsafe(publish_to_ws(result), loop)
                except KafkaError as e:
                    print(f"[ERROR] Kafka error during message processing: {e}, will attempt reconnection", flush=True)
                    raise  # Re-raise to trigger reconnection
                except Exception as e:
                    print(f"[ERROR] Message processing failed: {e}", flush=True)
                    # Continue on non-Kafka errors
        
        except KafkaError as e:
            retry_count += 1
            wait_time = min(2 ** retry_count, 30)  # Exponential backoff, max 30 seconds
            print(f"[ERROR] Kafka connection failed (attempt {retry_count}/{max_retries}): {e}", flush=True)
            print(f"[INFO] Retrying in {wait_time}s...", flush=True)
            
            if retry_count >= max_retries:
                print(f"[CRITICAL] Failed to connect to Kafka after {max_retries} attempts. Exiting.", flush=True)
                sys.exit(1)
            
            time.sleep(wait_time)
        except Exception as e:
            retry_count += 1
            wait_time = min(2 ** retry_count, 30)
            print(f"[ERROR] Unexpected error (attempt {retry_count}/{max_retries}): {e}", flush=True)
            print(f"[INFO] Retrying in {wait_time}s...", flush=True)
            
            if retry_count >= max_retries:
                print(f"[CRITICAL] Max retries exceeded. Exiting.", flush=True)
                sys.exit(1)
            
            time.sleep(wait_time)


async def main():
    """Main async function to run both WebSocket connection and Kafka consumer."""
    loop = asyncio.get_event_loop()
    
    # Set up signal handlers for graceful shutdown
    def signal_handler(sig, frame):
        print(f"[INFO] Received signal {sig}, initiating graceful shutdown...", flush=True)
        sys.exit(0)
    
    import signal
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Start WebSocket connection maintenance in background
    ws_task = asyncio.create_task(maintain_ws_connection())
    
    # Wait a bit for initial WebSocket connection
    await asyncio.sleep(2)
    
    # Start Kafka consumer in a separate thread (since it's blocking)
    import threading
    kafka_thread = threading.Thread(target=kafka_listener, args=(loop,), daemon=False)
    kafka_thread.start()
    
    # Keep running
    await ws_task


if __name__ == "__main__":
    asyncio.run(main())
