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

import psutil
import requests

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
        except requests.ConnectionError:
            print(f"[!] Heartbeat FAILED — Leader unreachable at {leader_url}")
        except Exception as e:
            print(f"[!] Heartbeat error: {e}")

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

def main():
    parser = argparse.ArgumentParser(description="Worker node for the Distributed AI Gateway")
    parser.add_argument("--leader-ip", default="127.0.0.1", help="Leader node IP address (default: 127.0.0.1)")
    args = parser.parse_args()

    leader_url = f"http://{args.leader_ip}:{LEADER_PORT}"

    # Step 1: Profile this machine
    profile = get_profile()

    # Step 2: Register with the Leader (retry until successful)
    print(f"Connecting to Leader at {leader_url}...")
    while not register(leader_url, profile):
        print(f"  Retrying in 5 seconds...")
        time.sleep(5)

    # Step 3: Heartbeat forever
    heartbeat_loop(leader_url, profile)


if __name__ == "__main__":
    main()
