import asyncio
import json
import signal
import sys
from typing import Dict, Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import uvicorn

app = FastAPI()

# Track consumers by their subscribed run_ids: {run_id: set(WebSocket)}
subscriptions: Dict[str, Set[WebSocket]] = {}
# Track all producers
producers: Set[WebSocket] = set()

# Graceful shutdown flag
shutdown_event: asyncio.Event = None


@app.on_event("startup")
def startup_event():
    """Set up signal handlers for graceful shutdown."""
    def signal_handler(sig, frame):
        print(f"[INFO] Received signal {sig}, initiating shutdown...", flush=True)
        sys.exit(0)
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    print(f"[INFO] WebSocket server started, ready for connections", flush=True)


@app.get("/health")
def health_check():
    """Health check endpoint for container orchestration."""
    return {
        "status": "healthy",
        "producers": len(producers),
        "total_subscribers": sum(len(subs) for subs in subscriptions.values()),
        "subscriptions": len(subscriptions)
    }


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """WebSocket endpoint - first message determines if producer or consumer."""
    await ws.accept()
    client_type = None
    subscribed_run_ids = set()
    
    try:
        # First message should identify the client type
        first_msg = await ws.receive_text()
        data = json.loads(first_msg)
        
        # Check if this is a type identification message
        if data.get("type") == "identify":
            client_type = data.get("role")  # "producer" or "consumer"
            if client_type == "producer":
                producers.add(ws)
                total_subs = sum(len(subs) for subs in subscriptions.values())
                print(f"✅ Producer connected (producers: {len(producers)}, total_subscribers: {total_subs}, unique_run_ids: {len(subscriptions)})", flush=True)
                await ws.send_text(json.dumps({"status": "identified", "role": "producer"}))
            elif client_type == "consumer":
                # Consumer should specify which run_id(s) they want to subscribe to
                run_ids = data.get("run_ids", [])
                if isinstance(run_ids, str):
                    run_ids = [run_ids]
                
                # Only subscribe to wildcard if explicitly requested with "*"
                # Empty list means NO subscription (client will subscribe later)
                if "*" in run_ids:
                    subscribed_run_ids.add("*")
                    if "*" not in subscriptions:
                        subscriptions["*"] = set()
                    subscriptions["*"].add(ws)
                    print(f"✅ Consumer connected, subscribed to ALL runs (wildcard)", flush=True)
                elif run_ids:
                    # Subscribe to specific run_ids
                    for run_id in run_ids:
                        if run_id not in subscriptions:
                            subscriptions[run_id] = set()
                        subscriptions[run_id].add(ws)
                        subscribed_run_ids.add(run_id)
                    print(f"✅ Consumer connected, subscribed to {len(subscribed_run_ids)} run(s): {list(subscribed_run_ids)[:3]}...", flush=True)
                else:
                    # Empty list - no subscription yet
                    print(f"✅ Consumer connected, NO subscriptions (will subscribe later)", flush=True)
                
                await ws.send_text(json.dumps({"status": "identified", "role": "consumer", "subscribed_run_ids": list(subscribed_run_ids)}))
        else:
            # If no identification, treat first message as data and assume producer
            client_type = "producer"
            producers.add(ws)
            total_subs = sum(len(subs) for subs in subscriptions.values())
            print(f"✅ Producer connected (no identify) (producers: {len(producers)}, total_subscribers: {total_subs})", flush=True)
            # Broadcast this first message
            run_id = data.get("run_id", "unknown")
            await broadcast_to_subscribers(data, run_id)
        
        # Continue listening for messages
        while True:
            data = await ws.receive_text()
            message = json.loads(data)
            
            if client_type == "producer":
                # Producers send data to subscribers of that run_id
                run_id = message.get("run_id", "unknown")
                count = await broadcast_to_subscribers(message, run_id)
                print(f"📤 Broadcasted run_id={run_id} to {count} subscribers", flush=True)
            elif client_type == "consumer":
                # Consumers can send subscription updates
                if message.get("type") == "subscribe":
                    new_run_ids = message.get("run_ids", [])
                    if isinstance(new_run_ids, str):
                        new_run_ids = [new_run_ids]
                    
                    for run_id in new_run_ids:
                        if run_id not in subscriptions:
                            subscriptions[run_id] = set()
                        subscriptions[run_id].add(ws)
                        subscribed_run_ids.add(run_id)
                    
                    await ws.send_text(json.dumps({"status": "subscribed", "run_ids": new_run_ids}))
                    print(f"📝 Consumer updated subscriptions: {list(subscribed_run_ids)[:3]}...", flush=True)
            
    except WebSocketDisconnect:
        print(f"[INFO] Client disconnected gracefully", flush=True)
    except Exception as e:
        print(f"[ERROR] WebSocket error: {e}", flush=True)
    finally:
        producers.discard(ws)
        # Remove from all subscriptions
        for run_id in subscribed_run_ids:
            if run_id in subscriptions:
                subscriptions[run_id].discard(ws)
                if not subscriptions[run_id]:
                    del subscriptions[run_id]
        total_subs = sum(len(subs) for subs in subscriptions.values())
        print(f"❌ Client disconnected (producers: {len(producers)}, total_subscribers: {total_subs}, unique_run_ids: {len(subscriptions)})", flush=True)


@app.get("/status")
def status():
    """Get the current status of connected WebSocket clients."""
    return {
        "producers": len(producers),
        "subscriptions": {run_id: len(subs) for run_id, subs in subscriptions.items()},
        "total_subscribers": sum(len(subs) for subs in subscriptions.values()),
        "status": "healthy"
    }


async def broadcast_to_subscribers(message: dict, run_id: str):
    """Send a message to all consumers subscribed to this run_id or to '*' (all)."""
    # Send to wildcard subscribers (those subscribed to all runs)
    targets = set()
    if "*" in subscriptions:
        targets.update(subscriptions["*"])
    
    # Send to specific run_id subscribers
    if run_id in subscriptions:
        targets.update(subscriptions[run_id])
    
    if not targets:
        return 0
    
    data = json.dumps(message)
    dead = set()
    success_count = 0
    
    for subscriber in targets:
        try:
            await subscriber.send_text(data)
            success_count += 1
        except Exception as e:
            print(f"[ERROR] Failed to send to subscriber: {e}", flush=True)
            dead.add(subscriber)
    
    # Clean up dead connections
    for d in dead:
        for sub_run_id in list(subscriptions.keys()):
            subscriptions[sub_run_id].discard(d)
            if not subscriptions[sub_run_id]:
                del subscriptions[sub_run_id]
    
    return success_count


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8002,
        log_level="info",
        access_log=True
    )
