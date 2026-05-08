"""
worker.py — Distributed AI Gateway: Worker Node

Runs on each worker laptop. On boot it:
  1. Profiles the host machine (RAM, Ollama models, .skill files)
  2. Registers with the Leader node
  3. Sends heartbeat pings every 5 seconds
  4. Polls the Leader for tasks and executes them via local Ollama

Usage:
  python worker.py --leader-ip 127.0.0.1
"""

import argparse
import glob
import os
import time
import uuid
import json

import psutil
import requests

from tailscale_utils import get_advertise_host

# ── Config ────────────────────────────────────────────

HEARTBEAT_INTERVAL = 5          # seconds between heartbeats
OLLAMA_URL = "http://localhost:11434"
LEADER_PORT = 8000

# Generate a persistent-ish node ID (stays the same for this session)
NODE_ID = f"node_{uuid.uuid4().hex[:6]}"


# ── 1. Hardware & Skill Profiler ──────────────────────

def get_ram_gb():
    """Return total system RAM in GB, rounded to 1 decimal."""
    return round(psutil.virtual_memory().total / (1024 ** 3), 1)


def get_ollama_models():
    """Query local Ollama for installed models. Returns [] if Ollama is not running."""
    try:
        resp = requests.get(f"{OLLAMA_URL}/api/tags", timeout=3)
        resp.raise_for_status()
        models = resp.json().get("models", [])
        return [m["name"] for m in models]
    except Exception:
        return []


def get_skills():
    """Scan the current directory for *.skill files and return skill names."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    skill_files = glob.glob(os.path.join(script_dir, "*.skill"))
    return [os.path.splitext(os.path.basename(f))[0] for f in skill_files]


def get_profile():
    """Build the full hardware profile for this worker."""
    ram = get_ram_gb()
    models = get_ollama_models()
    skills = get_skills()

    profile = {
        "node_id": NODE_ID,
        "ram_gb": ram,
        "models": models,
        "skills": skills,
    }

    print(f"\n{'='*50}")
    print(f"  Worker Node Profile")
    print(f"{'='*50}")
    print(f"  Node ID : {NODE_ID}")
    print(f"  RAM     : {ram} GB")
    print(f"  Models  : {models if models else '(none — is Ollama running?)'}")
    print(f"  Skills  : {skills if skills else '(none — no .skill files found)'}")
    print(f"{'='*50}\n")

    return profile


# ── 2. Registration & Heartbeat ───────────────────────

def register(leader_url, profile):
    """Send POST /register to the Leader with our profile."""
    try:
        resp = requests.post(f"{leader_url}/register", json=profile, timeout=5)
        resp.raise_for_status()
        print(f"[✓] Registered with Leader at {leader_url}")
        return True
    except requests.ConnectionError:
        print(f"[✗] Cannot reach Leader at {leader_url} — is main.py running?")
        return False
    except Exception as e:
        print(f"[✗] Registration failed: {e}")
        return False


# ── 3. Local Execution (Ollama Bridge) ────────────────

def execute_prompt(prompt, model=None):
    """Send a prompt to the local Ollama instance and return the response text."""
    if not model:
        # Pick the first available model
        models = get_ollama_models()
        if not models:
            return "[ERROR] No Ollama models available on this worker."
        model = models[0]

    print(f"[🤖] Executing prompt on model '{model}'...")
    print(f"[🤖] Prompt: \"{prompt[:80]}...\"" if len(prompt) > 80 else f"[🤖] Prompt: \"{prompt}\"")

    try:
        resp = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": model, "prompt": prompt, "stream": False},
            timeout=300,  # 5 min — small RAM machines need extra time
        )
        resp.raise_for_status()
        answer = resp.json().get("response", "")
        print(f"[🤖] Done. Response length: {len(answer)} chars")
        return answer
    except requests.ConnectionError:
        return "[ERROR] Ollama is not running on this machine."
    except Exception as e:
        return f"[ERROR] Ollama execution failed: {e}"


# ── 4. Main Loop (Heartbeat + Task Polling) ───────────

def heartbeat_loop(leader_url, profile):
    """Send heartbeats and poll the Leader for tasks."""
    tasks_completed = 0
    status = "idle"
    known_nodes = profile.get("_known_nodes", [])
    consecutive_leader_failures = 0

    print(f"[♥] Starting heartbeat + task polling loop (every {HEARTBEAT_INTERVAL}s)...\n")

    while True:
        # Send heartbeat
        payload = {
            "node_id": profile["node_id"],
            "status": status,
            "tasks_completed": tasks_completed,
        }
        try:
            resp = requests.post(f"{leader_url}/heartbeat", json=payload, timeout=5)
            resp.raise_for_status()
            print(f"[♥] Heartbeat sent — status: {status} | tasks done: {tasks_completed}")
            consecutive_leader_failures = 0
        except requests.ConnectionError:
            print(f"[!] Heartbeat FAILED — Leader unreachable at {leader_url}")
            consecutive_leader_failures += 1
        except Exception as e:
            print(f"[!] Heartbeat error: {e}")
            consecutive_leader_failures += 1

        # If leader is failing repeatedly, try to discover a new leader from known nodes.
        if consecutive_leader_failures >= 3 and known_nodes:
            new_leader = discover_leader_from_known_nodes(known_nodes)
            if new_leader and new_leader != leader_url:
                print(f"[↪] Switching leader: {leader_url} → {new_leader}")
                leader_url = new_leader
                consecutive_leader_failures = 0

        # Poll for a task
        try:
            task_resp = requests.get(f"{leader_url}/task/{profile['node_id']}", timeout=5)
            if task_resp.status_code == 200:
                task = task_resp.json()
                prompt = task.get("prompt", "")
                task_id = task.get("task_id", "unknown")
                model = task.get("model")  # None = use default

                print(f"\n[📥] Got task {task_id}!")
                status = "busy"

                # Execute on Ollama
                result = execute_prompt(prompt, model)

                # Report result back to Leader
                requests.post(f"{leader_url}/task/complete", json={
                    "task_id": task_id,
                    "node_id": profile["node_id"],
                    "response": result,
                }, timeout=10)

                tasks_completed += 1
                status = "idle"
                print(f"[✓] Task {task_id} completed and sent back to Leader\n")
        except Exception:
            pass  # No task available, that's fine

        time.sleep(HEARTBEAT_INTERVAL)


# ── Main ──────────────────────────────────────────────

def join_cluster(join_code: dict, profile: dict) -> tuple[str, list]:
    """
    Join the cluster using a join code (seed urls + join token).
    Returns (leader_url, known_nodes).
    """
    seed_nodes = join_code.get("seed_nodes") or []
    cluster_id = join_code.get("cluster_id")
    join_token = join_code.get("join_token")
    if not seed_nodes or not cluster_id or not join_token:
        raise ValueError("Invalid join code JSON (missing seed_nodes / cluster_id / join_token)")

    # Build NodeInfo-like payload
    host = get_advertise_host()
    node = {
        "node_id": profile["node_id"],
        "role": "worker",
        "priority": 10,
        "host": host,
        "port": LEADER_PORT,
        "url": f"http://{host}:{LEADER_PORT}",
        "ram_gb": profile.get("ram_gb"),
        "models": profile.get("models", []),
        "skills": profile.get("skills", []),
        "status": "alive",
        "last_heartbeat": int(time.time()),
        "tasks_completed": 0,
    }

    for seed in seed_nodes:
        try:
            resp = requests.post(
                f"{seed}/cluster/join",
                json={"cluster_id": cluster_id, "join_token": join_token, "node": node},
                timeout=5,
            )
            resp.raise_for_status()
            data = resp.json()
            leader = data.get("current_leader") or {}
            leader_url = leader.get("url") or seed
            known = data.get("known_nodes") or []
            return leader_url, known
        except Exception:
            continue
    raise RuntimeError("Could not join cluster: no reachable seed nodes")


def discover_leader_from_known_nodes(known_nodes: list) -> str | None:
    """
    Query /leader from known gateway/both nodes and return the leader url if found.
    """
    for n in known_nodes:
        url = None
        if isinstance(n, dict):
            url = n.get("url")
        if not url:
            continue
        try:
            r = requests.get(f"{url}/leader", timeout=3)
            if r.status_code == 200:
                leader = r.json()
                return leader.get("url") or url
        except Exception:
            pass
    return None


def main():
    parser = argparse.ArgumentParser(description="Worker node for the Distributed AI Gateway")
    parser.add_argument("--leader-ip", default="127.0.0.1", help="Leader node IP address (default: 127.0.0.1)")
    parser.add_argument("--join-code", default=None, help="Join code JSON (paste from node_cli.py create-cluster)")
    args = parser.parse_args()

    # Step 1: Profile this machine
    profile = get_profile()
    profile["role"] = "worker"
    profile["priority"] = 10
    profile["host"] = get_advertise_host()
    profile["port"] = LEADER_PORT

    if args.join_code:
        join_code = json.loads(args.join_code)
        leader_url, known_nodes = join_cluster(join_code, profile)
        profile["_known_nodes"] = known_nodes
        print(f"[✓] Joined cluster {join_code.get('cluster_id')} | leader={leader_url}")
    else:
        # Backward compatible path
        leader_url = f"http://{args.leader_ip}:{LEADER_PORT}"

    # Step 2: Register with the Leader (retry until successful)
    print(f"Connecting to Leader at {leader_url}...")
    while not register(leader_url, profile):
        print(f"  Retrying in 5 seconds...")
        time.sleep(5)

    # Step 3: Heartbeat forever
    heartbeat_loop(leader_url, profile)


if __name__ == "__main__":
    main()
