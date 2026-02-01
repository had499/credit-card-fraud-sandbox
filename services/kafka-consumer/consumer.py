# kafka_consumer_ws.py
import json
import os
import asyncio
import pandas as pd
import requests
from kafka import KafkaConsumer
import websockets

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "transactions"
MLFLOW_PREDICT_URL = "http://mlflow-serve:1234/invocations"
WS_SERVER_URL = "ws://websocket:8002/ws"

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
    # Use unique consumer group per session to always read new messages
    group_id = f"ml-inference-consumer-{int(time.time())}"
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",  
        enable_auto_commit=True,
        group_id=group_id,
    )
    print(f"Listening on Kafka topic '{TOPIC}' with group_id={group_id}...", flush=True)

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
        except Exception as e:
            print(f"[ERROR] Kafka processing failed: {e}", flush=True)


async def main():
    """Main async function to run both WebSocket connection and Kafka consumer."""
    loop = asyncio.get_event_loop()
    
    # Start WebSocket connection maintenance in background
    ws_task = asyncio.create_task(maintain_ws_connection())
    
    # Wait a bit for initial WebSocket connection
    await asyncio.sleep(2)
    
    # Start Kafka consumer in a separate thread (since it's blocking)
    import threading
    kafka_thread = threading.Thread(target=kafka_listener, args=(loop,), daemon=True)
    kafka_thread.start()
    
    # Keep running
    await ws_task


if __name__ == "__main__":
    asyncio.run(main())
    loop.run_forever()
