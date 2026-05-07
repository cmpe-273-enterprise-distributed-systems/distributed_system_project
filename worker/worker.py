"""
Worker node entry point.

Startup sequence:
  1. Read hardware info (RAM via psutil) and config from .env
  2. POST /register to the leader — retries until the leader is reachable
  3. Start a heartbeat thread (every 5 s)
  4. Enter the Kafka consume loop — pull tasks, call Ollama, publish results

Run:
  cd worker
  cp .env.example .env   # edit LEADER_URL, KAFKA_BROKER, MODEL, SKILLS
  pip install -r requirements.txt
  python worker.py
"""

import os
import signal
import sys
import threading
import time

import httpx
import psutil
from dotenv import load_dotenv

load_dotenv()

from kafka_client import WorkerKafka
from ollama_client import generate

# ── Config ────────────────────────────────────────────────────────────────────

LEADER_URL = os.getenv("LEADER_URL", "http://localhost:8000")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
MODEL = os.getenv("MODEL", "mistral")
SKILLS = [s.strip() for s in os.getenv("SKILLS", "general").split(",")]
NODE_ID = os.getenv("NODE_ID") or f"node_{os.urandom(3).hex()}"
RAM_GB = int(os.getenv("RAM_GB") or round(psutil.virtual_memory().total / (1024 ** 3)))
HEARTBEAT_INTERVAL = 5

# ── Shared state (only written from main thread or heartbeat thread) ───────────

_tasks_completed = 0
_current_status = "idle"
_shutdown = threading.Event()


# ── Registration ──────────────────────────────────────────────────────────────

def register():
    """Block until the leader accepts our registration."""
    while not _shutdown.is_set():
        try:
            r = httpx.post(
                f"{LEADER_URL}/register",
                json={"node_id": NODE_ID, "ram_gb": RAM_GB, "model": MODEL, "skills": SKILLS},
                timeout=5,
            )
            r.raise_for_status()
            print(f"[{NODE_ID}] Registered with leader at {LEADER_URL}")
            return
        except Exception as e:
            print(f"[{NODE_ID}] Registration failed ({e}). Retrying in 5 s…")
            time.sleep(5)


# ── Heartbeat loop ────────────────────────────────────────────────────────────

def heartbeat_loop():
    global _current_status
    while not _shutdown.is_set():
        try:
            r = httpx.post(
                f"{LEADER_URL}/heartbeat",
                json={"node_id": NODE_ID, "status": _current_status, "tasks_completed": _tasks_completed},
                timeout=5,
            )
            data = r.json()
            if data.get("reregister"):
                print(f"[{NODE_ID}] Leader asked us to re-register.")
                register()
        except Exception as e:
            print(f"[{NODE_ID}] Heartbeat failed: {e}")
        time.sleep(HEARTBEAT_INTERVAL)


# ── Task processing ───────────────────────────────────────────────────────────

def process_task(kafka: WorkerKafka, task: dict):
    global _tasks_completed, _current_status

    request_id = task.get("request_id", "unknown")
    prompt = task.get("prompt", "")
    print(f"[{NODE_ID}] Processing {request_id}: {prompt[:80]}…")

    _current_status = "busy"
    start = time.time()
    try:
        response = generate(MODEL, prompt)
        duration_ms = int((time.time() - start) * 1000)
        kafka.publish_result(request_id, response, duration_ms)
        _tasks_completed += 1
        print(f"[{NODE_ID}] Done {request_id} in {duration_ms} ms")
    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        print(f"[{NODE_ID}] Error on {request_id}: {e}")
        kafka.publish_result(request_id, "", duration_ms, error=str(e))
    finally:
        _current_status = "idle"


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"[{NODE_ID}] Starting — RAM: {RAM_GB} GB  Model: {MODEL}  Skills: {SKILLS}")

    register()

    hb = threading.Thread(target=heartbeat_loop, daemon=True, name="heartbeat")
    hb.start()

    kafka = WorkerKafka(KAFKA_BROKER, NODE_ID, RAM_GB)

    def on_signal(sig, frame):
        print(f"\n[{NODE_ID}] Shutting down…")
        _shutdown.set()
        kafka.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    print(f"[{NODE_ID}] Listening for tasks on Kafka broker {KAFKA_BROKER}…")
    while not _shutdown.is_set():
        try:
            for task in kafka.poll():
                if _shutdown.is_set():
                    break
                process_task(kafka, task)
        except Exception as e:
            print(f"[{NODE_ID}] Consumer error: {e}. Retrying in 2 s…")
            time.sleep(2)

    kafka.close()


if __name__ == "__main__":
    main()
