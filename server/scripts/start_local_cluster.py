from __future__ import annotations

import argparse
import asyncio
import os
import platform
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class StrategyProcessConfig:
    creationflags: int = 0
    preexec_fn = None


class ProcessStrategy:
    def popen_config(self) -> StrategyProcessConfig:
        raise NotImplementedError


class WindowsProcessStrategy(ProcessStrategy):
    def popen_config(self) -> StrategyProcessConfig:
        # New process group makes Ctrl+C handling more reliable on Windows.
        return StrategyProcessConfig(creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)


class PosixProcessStrategy(ProcessStrategy):
    def popen_config(self) -> StrategyProcessConfig:
        # Start a new session so child processes don't get stuck.
        return StrategyProcessConfig(preexec_fn=os.setsid)


class StrategyFactory:
    @staticmethod
    def create() -> ProcessStrategy:
        sysname = platform.system().lower()
        if "windows" in sysname:
            return WindowsProcessStrategy()
        return PosixProcessStrategy()


def tcp_port_open(host: str, port: int, timeout_s: float = 1.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return True
    except Exception:
        return False


def wait_for_ollama(host: str = "127.0.0.1", port: int = 11434, timeout_s: int = 60) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if tcp_port_open(host, port, timeout_s=1.0):
            return True
        time.sleep(1.5)
    return False


def run_docker_compose(repo_root: Path, targets: List[str]) -> None:
    env = os.environ.copy()
    # Ensure Kafka OUTSIDE advertised listener works for single-host dev.
    env.setdefault("LEADER_IP", "localhost")
    cmd = ["docker", "compose", "up", "-d", *targets]
    print("Running:", " ".join(cmd))
    subprocess.run(cmd, cwd=str(repo_root), env=env, check=True)


def apply_cql_from_files(repo_root: Path, container: str = "web-app-cassandra") -> None:
    cql_dir = repo_root / "client" / "db"
    files = ["001_keyspace.cql", "002_tables.cql", "003_seed_data.cql"]
    for f in files:
        path = cql_dir / f
        if not path.exists():
            print(f"[WARN] Missing CQL file: {path}")
            continue
        sql = path.read_text(encoding="utf-8")
        print(f"Applying {f} to Cassandra ({container})…")
        subprocess.run(
            ["docker", "exec", "-i", container, "cqlsh"],
            input=sql,
            text=True,
            cwd=str(repo_root),
            check=True,
            capture_output=True,
        )


def start_process(
    cmd: List[str],
    cwd: Path,
    env: Dict[str, str],
    strategy: ProcessStrategy,
) -> subprocess.Popen:
    cfg = strategy.popen_config()
    return subprocess.Popen(
        cmd,
        cwd=str(cwd),
        env=env,
        stdout=sys.stdout,
        stderr=sys.stderr,
        creationflags=cfg.creationflags,
        preexec_fn=cfg.preexec_fn,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Start local Kafka+Cassandra + leader + worker.")
    parser.add_argument("--docker", action="store_true", help="Start docker compose kafka+cassandra")
    parser.add_argument("--ollama", action="store_true", help="Start ollama (ollama serve) if not reachable")
    parser.add_argument("--start-leader", action="store_true", default=True, help="Start leader FastAPI")
    parser.add_argument("--start-worker", action="store_true", default=True, help="Start worker consumer")
    parser.add_argument("--leader-host", default="0.0.0.0")
    parser.add_argument("--leader-port", type=int, default=8000)
    parser.add_argument("--kafka-broker", default="localhost:9092")
    parser.add_argument("--ollama-url", default="http://localhost:11434")
    parser.add_argument("--model", default=os.getenv("MODEL", "mistral"))
    parser.add_argument("--skills", default=os.getenv("SKILLS", "general"))
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[2]
    strategy = StrategyFactory.create()

    # Start docker stack first so leader/worker can connect.
    if args.docker:
        run_docker_compose(repo_root, ["kafka", "cassandra"])
        # Apply schema (safe if already applied)
        time.sleep(3)
        apply_cql_from_files(repo_root)

    # Start Ollama if requested and not reachable.
    if args.ollama:
        # Basic TCP check
        if not tcp_port_open("127.0.0.1", 11434, timeout_s=1.0):
            print("Starting Ollama (`ollama serve`)…")
            start_process(
                ["ollama", "serve"],
                cwd=repo_root,
                env=os.environ.copy(),
                strategy=strategy,
            )

        print("Waiting for Ollama to become reachable…")
        if not wait_for_ollama(timeout_s=90):
            print("[WARN] Ollama still not reachable after wait period.")

    processes: List[subprocess.Popen] = []

    # Leader
    if args.start_leader:
        leader_cwd = repo_root / "server" / "leader"
        leader_env = os.environ.copy()
        leader_env["KAFKA_BROKER"] = args.kafka_broker
        leader_env["TASK_TIMEOUT"] = leader_env.get("TASK_TIMEOUT", "60")

        print(f"Starting leader: uvicorn main:app --host {args.leader_host} --port {args.leader_port}")
        processes.append(
            start_process(
                ["python", "-m", "uvicorn", "main:app", "--host", args.leader_host, "--port", str(args.leader_port)],
                cwd=leader_cwd,
                env=leader_env,
                strategy=strategy,
            )
        )

        # Give it a moment to bind.
        time.sleep(2)

    # Worker
    if args.start_worker:
        worker_cwd = repo_root / "server" / "worker"
        worker_env = os.environ.copy()
        worker_env["KAFKA_BROKER"] = args.kafka_broker
        worker_env["OLLAMA_URL"] = args.ollama_url
        worker_env["MODEL"] = args.model
        worker_env["SKILLS"] = args.skills
        worker_env["LEADER_URL"] = f"http://127.0.0.1:{args.leader_port}"

        print("Starting worker: python worker.py")
        processes.append(
            start_process(
                ["python", "-u", "worker.py"],
                cwd=worker_cwd,
                env=worker_env,
                strategy=strategy,
            )
        )

    def _shutdown(*_args):
        print("\nShutting down processes…")
        for p in processes:
            try:
                p.terminate()
            except Exception:
                pass
        time.sleep(1)
        for p in processes:
            try:
                p.kill()
            except Exception:
                pass
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Wait forever (until Ctrl+C).
    try:
        while True:
            time.sleep(2)
            # If any process exits unexpectedly, stop everything.
            for p in processes:
                if p.poll() is not None:
                    print(f"[WARN] Process exited with code {p.returncode}. Stopping others…")
                    _shutdown()
    except KeyboardInterrupt:
        _shutdown()


if __name__ == "__main__":
    main()

