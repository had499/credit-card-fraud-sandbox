import os
import json
import time
import random
import threading
from typing import Optional, Dict
from pathlib import Path
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel, Field
from kafka import KafkaProducer
import joblib
import numpy as np


APP_DIR = Path(__file__).resolve().parent
DIST_DIR = APP_DIR / "distributions"

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DEFAULT_TOPIC = os.environ.get("KAFKA_TOPIC", "transactions")


class ProduceRequest(BaseModel):
    count: int = Field(100, ge=1, le=10000)
    proportion_dist1: float = Field(0.5, ge=0.0, le=1.0, description="Probability of sampling from dist1")
    topic: Optional[str] = DEFAULT_TOPIC
    interval_seconds: Optional[float] = Field(0.0, ge=0.0, description="Sleep between sends; 0 for no delay")


def load_distributions():
    from scipy.stats import multivariate_normal
    mean0_path = DIST_DIR / "mean0.joblib"
    cov0_path = DIST_DIR / "cov0.joblib"
    mean1_path = DIST_DIR / "mean1.joblib"
    cov1_path = DIST_DIR / "cov1.joblib"

    d0 = None
    d1 = None
    if mean0_path.exists() & cov0_path.exists():
        mean0 = joblib.load(mean0_path)
        cov0 = joblib.load(cov0_path)
        d0 = multivariate_normal(mean0, cov0)
    if mean1_path.exists() & cov1_path.exists():
        mean1 = joblib.load(mean1_path)
        cov1 = joblib.load(cov1_path)
        d1 = multivariate_normal(mean1, cov1)
    return d0, d1


producer = KafkaProducer(bootstrap_servers=BOOTSTRAP,
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                         api_version=(0, 10))

app = FastAPI(title="Kafka Producer API")

# single-slot tracker for background run: {"thread": Thread, "stop_event": Event}
current_run: Optional[Dict] = None


@app.on_event("startup")
def startup_event():
    app.state.distributions = load_distributions()
    print(f"Producer API starting, Kafka={BOOTSTRAP} default_topic={DEFAULT_TOPIC}")


def sample_from_distribution(dist, size=1):
    # frozen scipy distribution exposes rvs
    if dist is None:
        return None
    samples = dist.rvs(size=size)
    # Handle scalar or 1-D/2-D outputs
    return np.atleast_2d(samples)


def send_messages(topic: str, messages):
    for msg in messages:
        producer.send(topic, msg)
    producer.flush()


def produce_background(count: int, proportion_dist1: float, topic: str, interval_seconds: float, stop_event: threading.Event, run_id: str):
    """Generate and send messages until count is reached or stop_event is set.

    This runs in a separate thread so it can be cancelled via the `stop_event`.
    """
    d0, d1 = app.state.distributions
    for i in range(count):
        if stop_event.is_set():
            print(f"Run {run_id}: stop requested, exiting after {i} messages")
            break

        choose = random.random() < proportion_dist1
        dist_id = 1 if choose else 0
        dist = d1 if choose else d0
        sample = None
        if dist is not None:
            samp = sample_from_distribution(dist, size=1)
            # convert to native python types
            sample = samp.tolist()[0]
        payload = {
            "run_id": run_id,
            "transaction_index": i,
            "dist": dist_id,
            "features": sample,
            "timestamp": time.time(),
        }
        producer.send(topic, payload)
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
    print(f"Run {run_id}: finished or stopped; flushed producer")
    
    # Clear current_run after done
    global current_run
    current_run = None


@app.post("/produce")
def produce(req: ProduceRequest, background_tasks: BackgroundTasks):
    """Produce `count` messages to Kafka using the saved distributions.

    proportion_dist1 is probability of sampling from dist1; 1-proportion is from dist0.
    The endpoint returns immediately and the generation/sending happens in background.
    """
    global current_run
    topic = req.topic or DEFAULT_TOPIC
    if current_run is not None:
        return {"status": "busy", "message": "another produce run is already active"}
    stop_event = threading.Event()
    thread = threading.Thread(target=produce_background, args=(req.count, req.proportion_dist1, topic, req.interval_seconds, stop_event, "single"), daemon=True)
    # start immediately; keep reference so we can stop later
    current_run = {"thread": thread, "stop_event": stop_event}
    thread.start()
    return {"status": "scheduled", "count": req.count, "topic": topic, "proportion_dist1": req.proportion_dist1}



@app.post("/stop")
def stop():
    """Stop the current in-progress run started by POST /produce. Returns status."""
    global current_run
    if current_run is None:
        return {"status": "not-running"}
    stop_event = current_run.get("stop_event")
    thread = current_run.get("thread")
    stop_event.set()
    thread.join(timeout=5.0)
    current_run = None
    return {"status": "stopped"}


@app.get("/healthz")
def health():
    d0, d1 = app.state.distributions
    return {"kafka_bootstrap": BOOTSTRAP, "topic": DEFAULT_TOPIC, "dist0_loaded": d0 is not None, "dist1_loaded": d1 is not None}

