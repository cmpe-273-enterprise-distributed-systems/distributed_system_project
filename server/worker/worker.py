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

import json
import os
import signal
import sys
import threading
import time
from pathlib import Path

import httpx
import psutil
from dotenv import load_dotenv

load_dotenv()

from kafka_client import WorkerKafka
from ollama_client import generate

# ── Config ────────────────────────────────────────────────────────────────────

# LEADER_URL from .env is used as the fallback bootstrap address. The actual
# leader URL is resolved at runtime — first from the Upstash discovery service
# (if configured), then from this fallback. The cached value is updated again
# after HEARTBEAT_FAILURE_THRESHOLD consecutive heartbeat failures so workers
# can follow the leader to its new home after a failover.
FALLBACK_LEADER_URL = os.getenv("LEADER_URL", "http://localhost:8000")
UPSTASH_URL = os.getenv("UPSTASH_REDIS_REST_URL", "").strip().rstrip("/")
UPSTASH_TOKEN = os.getenv("UPSTASH_REDIS_REST_READ_ONLY_TOKEN", "").strip()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
MODEL = os.getenv("MODEL", "mistral")
SKILLS = [s.strip() for s in os.getenv("SKILLS", "general").split(",")]
NODE_ID = os.getenv("NODE_ID") or f"node_{os.urandom(3).hex()}"
RAM_GB = int(os.getenv("RAM_GB") or round(psutil.virtual_memory().total / (1024 ** 3)))
HEARTBEAT_INTERVAL = 5
HEARTBEAT_FAILURE_THRESHOLD = 3

_leader_url = FALLBACK_LEADER_URL
_leader_lock = threading.Lock()


def _get_leader_url() -> str:
    with _leader_lock:
        return _leader_url


def _set_leader_url(url: str) -> None:
    global _leader_url
    with _leader_lock:
        _leader_url = url


def resolve_leader_from_discovery() -> str | None:
    """Return the current leader URL from Upstash, or None if unavailable."""
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return None
    try:
        r = httpx.get(
            f"{UPSTASH_URL}/get/leader",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            timeout=5,
        )
        r.raise_for_status()
        raw = r.json().get("result")
        if not raw:
            return None
        return (json.loads(raw).get("leader_url") or "").strip() or None
    except Exception as exc:
        print(f"[{NODE_ID}] Discovery resolve failed: {exc}")
        return None


def _not_leader_hint(exc: Exception) -> str | None:
    """
    If `exc` is a 503 from a leader that has self-demoted (or never was the
    leader), return the URL it advertised so we can switch immediately
    instead of waiting HEARTBEAT_FAILURE_THRESHOLD ticks for the discovery
    re-resolve fallback. Returns None for any other exception.
    """
    if not isinstance(exc, httpx.HTTPStatusError):
        return None
    if exc.response.status_code != 503:
        return None
    hint = exc.response.headers.get("X-Leader-URL", "").strip()
    if hint:
        return hint
    try:
        body = exc.response.json()
        detail = body.get("detail") if isinstance(body, dict) else None
        if isinstance(detail, dict) and detail.get("code") == "not_leader":
            url = (detail.get("leader_url") or "").strip()
            if url:
                return url
    except Exception:
        pass
    return None


# ── Hardware profiling ────────────────────────────────────────────────────────

def get_ollama_models() -> list[str]:
    try:
        resp = httpx.get(f"{OLLAMA_URL}/api/tags", timeout=3)
        resp.raise_for_status()
        return [m["name"] for m in resp.json().get("models", [])]
    except Exception:
        return []


SKILLS_DIR = Path(__file__).resolve().parent / "skills"

# Loaded once at startup by load_skills(); read by process_task to inject the
# matching SKILL.md content as Ollama's `system` prompt for skill-tagged tasks.
_skills: dict[str, str] = {}


def _strip_frontmatter(text: str) -> str:
    """Drop a YAML frontmatter block at the top of `text` if present.

    Upskill writes SKILL.md with `---\\nname: ...\\ndescription: ...\\n---\\n`
    at the top. The body below it is the actual instruction text we want
    Ollama to see as the system prompt; the frontmatter would just be visible
    markdown in the model's context.
    """
    if not text.startswith("---"):
        return text
    parts = text.split("---", 2)
    if len(parts) < 3:
        return text
    return parts[2].lstrip("\n")


def load_skills() -> list[str]:
    """Discover Upskill-format skills under SKILLS_DIR.

    A skill is any subdirectory containing a SKILL.md file; the directory
    name is the canonical skill key used for routing and registration.
    Contents are cached in _skills (keyed by directory name) so process_task
    can inject them at consume time without re-reading per request. Returns
    a sorted list of discovered skill names for the /register payload.
    """
    _skills.clear()
    if not SKILLS_DIR.is_dir():
        return []
    for entry in sorted(SKILLS_DIR.iterdir()):
        skill_md = entry / "SKILL.md"
        if entry.is_dir() and skill_md.is_file():
            _skills[entry.name] = _strip_frontmatter(skill_md.read_text(encoding="utf-8"))
    return sorted(_skills.keys())

# ── Shared state (only written from main thread or heartbeat thread) ───────────

_tasks_completed = 0
_current_status = "idle"
_shutdown = threading.Event()

# Resolved once at startup (first register() call) and reused for every task.
# Reading MODEL directly in process_task was a bug: registration reported the
# first detected Ollama tag while execution silently fell back to whatever
# MODEL happened to be, even when those didn't match.
_effective_model: str = MODEL


# ── Registration ──────────────────────────────────────────────────────────────

def register() -> str:
    """Block until the leader accepts registration. Returns the Kafka broker address.

    Re-resolves the leader URL from the discovery service on every retry attempt
    so a worker started before any leader is up will pick up the leader as soon
    as one publishes itself, and a worker rejoining after a failover will target
    the new leader.
    """
    global _effective_model
    detected_models = get_ollama_models()
    _effective_model = detected_models[0] if detected_models else MODEL
    effective_skills = load_skills() or SKILLS

    print(f"[{NODE_ID}] RAM: {RAM_GB} GB  Model: {_effective_model}  Skills: {effective_skills}")

    while not _shutdown.is_set():
        if discovered := resolve_leader_from_discovery():
            _set_leader_url(discovered)
        url = _get_leader_url()
        try:
            r = httpx.post(
                f"{url}/register",
                json={"node_id": NODE_ID, "ram_gb": RAM_GB, "model": _effective_model, "skills": effective_skills},
                timeout=5,
            )
            r.raise_for_status()
            data = r.json()
            broker = data.get("kafka_broker", KAFKA_BROKER)
            print(f"[{NODE_ID}] Registered with leader at {url}. Kafka broker: {broker}")
            return broker
        except Exception as e:
            if (hint := _not_leader_hint(e)) and hint != url:
                _set_leader_url(hint)
                print(f"[{NODE_ID}] {url} returned 503 not_leader; switching to {hint} and retrying.")
                continue
            print(f"[{NODE_ID}] Registration failed against {url} ({e}). Retrying in 5 s…")
            time.sleep(5)
    return KAFKA_BROKER


# ── Heartbeat loop ────────────────────────────────────────────────────────────

def heartbeat_loop(kafka: WorkerKafka):
    global _current_status
    failures = 0
    while not _shutdown.is_set():
        url = _get_leader_url()
        try:
            r = httpx.post(
                f"{url}/heartbeat",
                json={"node_id": NODE_ID, "status": _current_status, "tasks_completed": _tasks_completed},
                timeout=5,
            )
            r.raise_for_status()
            data = r.json()
            failures = 0
            if data.get("reregister"):
                print(f"[{NODE_ID}] Leader asked us to re-register.")
                new_broker = register()
                kafka.request_reconnect(new_broker)
            elif (new_broker := data.get("kafka_broker")) and new_broker != kafka.bootstrap_servers:
                print(f"[{NODE_ID}] Kafka broker changed to {new_broker}.")
                kafka.request_reconnect(new_broker)
        except Exception as e:
            if (hint := _not_leader_hint(e)) and hint != url:
                # Fast path: leader self-demoted (Scenario 2B Task 3 gate) or
                # we're talking to a node that never was the leader. Skip the
                # 3-failure threshold and switch immediately.
                print(f"[{NODE_ID}] Heartbeat to {url} returned 503 not_leader; switching to {hint}.")
                _set_leader_url(hint)
                new_broker = register()
                kafka.request_reconnect(new_broker)
                failures = 0
                time.sleep(HEARTBEAT_INTERVAL)
                continue
            failures += 1
            print(f"[{NODE_ID}] Heartbeat to {url} failed ({failures}/{HEARTBEAT_FAILURE_THRESHOLD}): {e}")
            if failures >= HEARTBEAT_FAILURE_THRESHOLD:
                discovered = resolve_leader_from_discovery()
                if discovered and discovered != url:
                    print(f"[{NODE_ID}] Discovery reports new leader: {discovered}. Re-registering.")
                    _set_leader_url(discovered)
                    new_broker = register()
                    kafka.request_reconnect(new_broker)
                    failures = 0
                else:
                    print(f"[{NODE_ID}] Discovery has no new leader yet; will keep trying.")
        time.sleep(HEARTBEAT_INTERVAL)


# ── Task processing ───────────────────────────────────────────────────────────

def process_task(kafka: WorkerKafka, task: dict):
    global _tasks_completed, _current_status

    request_id = task.get("request_id", "unknown")
    prompt = task.get("prompt", "")
    requested_skill = task.get("skill")

    # Look up the SKILL.md content for the task's skill, if any. If the
    # leader's preflight routed a skill-tagged task to a worker that doesn't
    # have it (Kafka can do this within a tier topic), degrade silently —
    # run the prompt without a system context and warn. Hard-rejecting would
    # leave the request hanging for the elected leader's TASK_TIMEOUT.
    system_prompt: str | None = None
    if requested_skill:
        system_prompt = _skills.get(requested_skill)
        if system_prompt is None:
            print(
                f"[{NODE_ID}] WARN: task {request_id} requests skill "
                f"{requested_skill!r} but this worker only has "
                f"{sorted(_skills.keys())}. Running without system prompt."
            )

    print(f"[{NODE_ID}] Processing {request_id} (skill={requested_skill}): {prompt[:80]}…")

    _current_status = "busy"
    start = time.time()
    try:
        response = generate(_effective_model, prompt, system=system_prompt)
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
    print(f"[{NODE_ID}] Subscribed to topics: {kafka.topics}")

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
                # Commit only after process_task returns. If the worker crashes
                # mid-task, the offset stays uncommitted and Kafka redelivers
                # to another worker on rebalance.
                kafka.commit()
            kafka.apply_reconnect_if_needed()
        except Exception as e:
            print(f"[{NODE_ID}] Consumer error: {e}. Retrying in 2 s…")
            time.sleep(2)

    kafka.close()


if __name__ == "__main__":
    main()
