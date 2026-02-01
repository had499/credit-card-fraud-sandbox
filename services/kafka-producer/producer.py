import os
import json
import threading
import time
import uuid
from typing import Optional, Dict
from pathlib import Path
from collections import defaultdict, deque
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from kafka import KafkaProducer
import pandas as pd
from sklearn.utils import resample  


APP_DIR = Path(__file__).resolve().parent

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DEFAULT_TOPIC = os.environ.get("KAFKA_TOPIC", "transactions")


class ProduceRequest(BaseModel):
    count: int = Field(100, ge=1, le=100, description="Max 100 transactions per request")
    proportion_dist1: float = Field(0.5, ge=0.0, le=1.0, description="Probability of sampling from dist1")
    topic: Optional[str] = DEFAULT_TOPIC
    interval_seconds: Optional[float] = Field(0.0, ge=0.0, le=5.0, description="Sleep between sends; max 5 seconds")

def get_feature_names():
    return app.state.test_data[0].columns.tolist()

def load_test_data():

    X_data= pd.read_csv(APP_DIR / "data" / "X_test.csv")
    y_data= pd.read_csv(APP_DIR / "data" / "y_test.csv")
    
    return X_data, y_data
    
    
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP,
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                         api_version=(0, 10))

app = FastAPI(title="Kafka Producer API")

# Add CORS middleware - configured for local development
# Allows localhost and private network ranges (192.168.x.x, 10.x.x.x, 172.16-31.x.x)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For local dev - restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dictionary to track multiple concurrent runs: {run_id: {"thread": Thread, "stop_event": Event}}
active_runs: Dict[str, Dict] = {}

# Rate limiting: track request timestamps per IP (max 10 requests per minute)
rate_limit_data: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10))
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_MAX_REQUESTS = 10


@app.on_event("startup")
def startup_event():
    app.state.test_data = load_test_data()
    print(f"Producer API starting, Kafka={BOOTSTRAP} default_topic={DEFAULT_TOPIC}")


def send_messages(topic: str, messages):
    for msg in messages:
        producer.send(topic, msg)
    producer.flush()



def resample_to_target_fraction(X, y, target_fraction: float, random_state=None):
    """
    Return X_res, y_res such that fraction of 1's in y_res ~= target_fraction.
    Keeps total length the same as original dataset.
    """
    import pandas as pd

    if not (0.0 <= target_fraction <= 1.0):
        raise ValueError("target_fraction must be between 0 and 1")

    df = X.copy()
    df['y'] = y.values  # ensure alignment

    n_total = len(df)
    target_n_min = int(round(target_fraction * n_total))

    df_min = df[df['y'] == 1]
    df_maj = df[df['y'] == 0]

    n_min = len(df_min)

    if n_min == 0 and target_n_min > 0:
        raise ValueError("No minority class examples available to upsample from")

    # Upsample or downsample minority to reach the target minority count
    if target_n_min > n_min:
        df_min_res = resample(df_min,
                              replace=True,
                              n_samples=target_n_min,
                              random_state=random_state)
    else:
        df_min_res = resample(df_min,
                              replace=False,
                              n_samples=target_n_min,
                              random_state=random_state)

    # Keep majority as-is (could also downsample if you want different total size)
    df_res = pd.concat([df_maj, df_min_res], axis=0)

    # Shuffle and reset indices so X_res.sample(1) and y_res align by position
    df_res = df_res.sample(frac=1, random_state=random_state).reset_index(drop=True)

    X_res = df_res.drop(columns='y')
    y_res = df_res['y'].reset_index(drop=True)

    return X_res, y_res


def produce_background(count: int, proportion_dist1: float, topic: str, interval_seconds: float, stop_event: threading.Event, run_id: str):
    """Generate and send credit card transaction messages until count is reached or stop_event is set."""
    try:
        X_data, y_data = app.state.test_data
        
        # Separate fraud and non-fraud transactions
        fraud_mask = y_data.values.flatten() == 1
        X_fraud = X_data[fraud_mask]
        y_fraud = y_data[fraud_mask]
        X_non_fraud = X_data[~fraud_mask]
        y_non_fraud = y_data[~fraud_mask]
        
        print(f"Run {run_id}: {len(X_fraud)} fraud samples, {len(X_non_fraud)} non-fraud samples available", flush=True)
        
        # Wait 2 seconds to allow WebSocket clients to connect and subscribe
        print(f"Run {run_id}: waiting 2 seconds for clients to connect...", flush=True)
        time.sleep(2)

        for i in range(count):
            if stop_event.is_set():
                print(f"Run {run_id}: stop requested, exiting after {i} messages")
                break

            # Randomly decide if this transaction should be fraud based on proportion_dist1
            import random
            is_fraud_tx = random.random() < proportion_dist1
            
            # Sample from the appropriate dataset
            if is_fraud_tx and len(X_fraud) > 0:
                idx = random.randint(0, len(X_fraud) - 1)
                sample = X_fraud.iloc[idx]
                is_fraud = 1
            else:
                idx = random.randint(0, len(X_non_fraud) - 1)
                sample = X_non_fraud.iloc[idx]
                is_fraud = 0
            
            amount = float(sample['Amount'])
            
            payload = {
                "run_id": run_id,
                "transaction_index": i,
                "is_fraud": is_fraud,
                "amount": amount,
                "features": sample.tolist(),
                "feature_names": get_feature_names(),
                "timestamp": time.time(),
            }
            producer.send(topic, payload)
            producer.flush()  # Ensure message is sent immediately

            if interval_seconds and interval_seconds > 0:
                # allow responsive stop during sleeps
                slept = 0.0
                step = 0.1
                while slept < interval_seconds:
                    if stop_event.is_set():
                        break
                    time.sleep(min(step, interval_seconds - slept))
                    slept += step

        producer.flush()


        print(f"Run {run_id}: completed successfully, sent {count} transactions")
    except Exception as e:
        print(f"Run {run_id}: error occurred - {str(e)}")
    finally:
        # Clean up this run from active runs
        global active_runs
        if run_id in active_runs:
            del active_runs[run_id]
        print(f"Run {run_id}: cleaned up")



@app.post("/produce")
def produce(req: ProduceRequest, request: Request):
    """Produce credit card transaction messages to Kafka.

    proportion_dist1 represents the fraud rate (0.0 = no fraud, 1.0 = all fraud).
    The endpoint returns immediately and message generation happens in background.
    Multiple concurrent requests are supported.
    Rate limited to 10 requests per minute per IP.
    """
    global active_runs
    
    # Rate limiting check
    client_ip = request.client.host if request.client else "unknown"
    current_time = time.time()
    
    # Clean old requests outside the time window
    request_times = rate_limit_data[client_ip]
    while request_times and current_time - request_times[0] > RATE_LIMIT_WINDOW:
        request_times.popleft()
    
    # Check if rate limit exceeded
    if len(request_times) >= RATE_LIMIT_MAX_REQUESTS:
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit exceeded. Max {RATE_LIMIT_MAX_REQUESTS} requests per {RATE_LIMIT_WINDOW} seconds."
        )
    
    # Add current request timestamp
    request_times.append(current_time)
    
    topic = req.topic or DEFAULT_TOPIC
    
    # Generate unique run ID
    run_id = str(uuid.uuid4())
    
    # Create and start background thread
    stop_event = threading.Event()
    thread = threading.Thread(
        target=produce_background,
        args=(req.count, req.proportion_dist1, topic, req.interval_seconds, stop_event, run_id),
        daemon=True
    )
    
    active_runs[run_id] = {"thread": thread, "stop_event": stop_event, "topic": topic, "count": req.count}
    thread.start()
    
    return {
        "status": "started",
        "run_id": run_id,
        "count": req.count,
        "topic": topic,
        "fraud_rate": req.proportion_dist1,
        "interval_seconds": req.interval_seconds
    }



@app.post("/stop/{run_id}")
def stop(run_id: str):
    """Stop a specific in-progress run by its run_id."""
    global active_runs
    if run_id not in active_runs:
        return {"status": "not-found", "message": f"Run {run_id} not found or already completed"}
    
    run_info = active_runs[run_id]
    stop_event = run_info["stop_event"]
    thread = run_info["thread"]
    
    stop_event.set()
    thread.join(timeout=5.0)
    
    if run_id in active_runs:
        del active_runs[run_id]
    
    return {"status": "stopped", "run_id": run_id}


@app.post("/stop-all")
def stop_all():
    """Stop all active runs."""
    global active_runs
    if not active_runs:
        return {"status": "no-active-runs"}
    
    stopped_runs = []
    for run_id, run_info in list(active_runs.items()):
        run_info["stop_event"].set()
        run_info["thread"].join(timeout=5.0)
        stopped_runs.append(run_id)
    
    active_runs.clear()
    return {"status": "stopped-all", "stopped_runs": stopped_runs}


@app.get("/status")
def status():
    """Get status of all active runs."""
    runs = []
    for run_id, run_info in active_runs.items():
        runs.append({
            "run_id": run_id,
            "topic": run_info["topic"],
            "count": run_info["count"],
            "is_alive": run_info["thread"].is_alive()
        })
    return {"active_runs": len(runs), "runs": runs}


@app.get("/healthz")
def health():
    """Health check endpoint."""
    X_data, y_data = app.state.test_data if hasattr(app.state, 'test_data') else (None, None)
    return {
        "status": "healthy",
        "kafka_bootstrap": BOOTSTRAP,
        "default_topic": DEFAULT_TOPIC,
        "test_data_loaded": X_data is not None and y_data is not None,
        "active_runs": len(active_runs)
    }

