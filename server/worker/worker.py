"""
Worker node entry point.

Startup sequence:
  1. Read hardware info (RAM via psutil) and config from .env
  2. POST /register to the leader — retries until the leader is reachable
  3. Start a heartbeat thread (every 5 s)
  4. Enter the Kafka consume loop — pull tasks, call Ollama, publish results

Run:
  cd server/worker
  cp .env.example .env   # edit LEADER_URL, KAFKA_BROKER, MODEL, SKILLS
  pip install -r requirements.txt
  python worker.py
"""

import glob
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

# TODO(discovery): LEADER_URL is static — loaded once from .env and never updated.
# If the current leader goes down and a new leader is elected at a different IP,
# this worker will retry forever against a dead address. The fix requires a
# Discovery Service (an external KV store, DNS record, or hosted endpoint) that
# always holds the current leader's IP. On startup and after consecutive heartbeat
# failures, this worker should query that service instead of reading a hardcoded URL.
LEADER_URL = os.getenv("LEADER_URL", "http://localhost:8000")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
MODEL = os.getenv("MODEL", "mistral")
SKILLS = [s.strip() for s in os.getenv("SKILLS", "general").split(",")]
NODE_ID = os.getenv("NODE_ID") or f"node_{os.urandom(3).hex()}"
RAM_GB = int(os.getenv("RAM_GB") or round(psutil.virtual_memory().total / (1024 ** 3)))
HEARTBEAT_INTERVAL = 5


# ── Hardware profiling ────────────────────────────────────────────────────────

def get_ollama_models() -> list[str]:
    try:
        resp = httpx.get(f"{OLLAMA_URL}/api/tags", timeout=3)
        resp.raise_for_status()
        return [m["name"] for m in resp.json().get("models", [])]
    except Exception:
        return []


def get_skills() -> list[str]:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    skill_files = glob.glob(os.path.join(script_dir, "*.skill"))
    return [os.path.splitext(os.path.basename(f))[0] for f in skill_files]

# ── Shared state (only written from main thread or heartbeat thread) ───────────

_tasks_completed = 0
_current_status = "idle"
_shutdown = threading.Event()


# ── Registration ──────────────────────────────────────────────────────────────

def register() -> str:
    """Block until the leader accepts registration. Returns the Kafka broker address.

    TODO(discovery): This retries against the same LEADER_URL indefinitely. If the
    leader is permanently gone, the retry loop will never succeed. Before each retry,
    this function should query the Discovery Service for the current leader IP and
    update LEADER_URL accordingly so it targets the newly elected leader.
    """
    detected_models = get_ollama_models()
    effective_model = detected_models[0] if detected_models else MODEL
    effective_skills = get_skills() or SKILLS

    print(f"[{NODE_ID}] RAM: {RAM_GB} GB  Model: {effective_model}  Skills: {effective_skills}")

    while not _shutdown.is_set():
        try:
            r = httpx.post(
                f"{LEADER_URL}/register",
                json={"node_id": NODE_ID, "ram_gb": RAM_GB, "model": effective_model, "skills": effective_skills},
                timeout=5,
            )
            r.raise_for_status()
            data = r.json()
            broker = data.get("kafka_broker", KAFKA_BROKER)
            print(f"[{NODE_ID}] Registered with leader at {LEADER_URL}. Kafka broker: {broker}")
            return broker
        except Exception as e:
            print(f"[{NODE_ID}] Registration failed ({e}). Retrying in 5 s…")
            time.sleep(5)
    return KAFKA_BROKER


# ── Heartbeat loop ────────────────────────────────────────────────────────────

def heartbeat_loop(kafka: WorkerKafka):
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
                new_broker = register()
                kafka.request_reconnect(new_broker)
            elif (new_broker := data.get("kafka_broker")) and new_broker != kafka.bootstrap_servers:
                print(f"[{NODE_ID}] Kafka broker changed to {new_broker}.")
                kafka.request_reconnect(new_broker)
        except Exception as e:
            print(f"[{NODE_ID}] Heartbeat failed: {e}")
            # TODO(discovery): A heartbeat failure may mean the leader is down, not
            # just temporarily unreachable. After N consecutive failures, this worker
            # should query the Discovery Service for the new leader IP, update
            # LEADER_URL, call register() to join the new leader, and signal a Kafka
            # broker reconnect via kafka.request_reconnect(). Until the Discovery
            # Service exists, the worker will simply retry against the dead URL and
            # stall — tasks already in Kafka partitions will still be processed, but
            # no new tasks will be routable to this worker and heartbeats won't resume.
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

    broker = register()
    kafka = WorkerKafka(broker, NODE_ID, RAM_GB)

    # Heartbeat thread needs the kafka reference so it can signal broker changes.
    hb = threading.Thread(target=heartbeat_loop, args=(kafka,), daemon=True, name="heartbeat")
    hb.start()

    def on_signal(sig, frame):
        print(f"\n[{NODE_ID}] Shutting down…")
        _shutdown.set()
        kafka.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    print(f"[{NODE_ID}] Listening for tasks on Kafka broker {broker}…")
    while not _shutdown.is_set():
        try:
            for task in kafka.poll():
                if _shutdown.is_set():
                    break
                process_task(kafka, task)
            kafka.apply_reconnect_if_needed()
        except Exception as e:
            print(f"[{NODE_ID}] Consumer error: {e}. Retrying in 2 s…")
            time.sleep(2)

    kafka.close()


if __name__ == "__main__":
    main()
