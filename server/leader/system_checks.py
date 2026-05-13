from __future__ import annotations

import asyncio
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx


@dataclass
class CheckItem:
    name: str
    status: str  # ok | warn | fail
    detail: str
    help: Optional[str] = None
    # Optional list for UI (e.g. every Ollama tag from /api/tags).
    models: Optional[List[str]] = None

    def as_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "name": self.name,
            "status": self.status,
            "detail": self.detail,
            "help": self.help,
        }
        if self.models is not None:
            out["models"] = self.models
        return out


class CommandRunner:
    async def run(
        self,
        cmd: List[str],
        timeout_s: float = 8.0,
        check: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        return await asyncio.to_thread(self._run_sync, cmd, timeout_s, check)

    def _run_sync(
        self,
        cmd: List[str],
        timeout_s: float,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            cmd,
            timeout=timeout_s,
            capture_output=True,
            text=True,
            check=check,
        )


class WindowsCommandRunner(CommandRunner):
    # Windows needs different process-group handling when we later add background process support.
    pass


class PosixCommandRunner(CommandRunner):
    pass


class CommandRunnerFactory:
    @staticmethod
    def create() -> CommandRunner:
        sysname = platform.system().lower()
        if "windows" in sysname:
            return WindowsCommandRunner()
        return PosixCommandRunner()


_IP_V4_PREFIX = re.compile(r"^100\\.64\\.")
_IP_V6_PREFIX = re.compile(r"^fd7a:")


async def check_tailscale(runner: CommandRunner) -> CheckItem:
    if not shutil.which("tailscale"):
        return CheckItem(
            name="Tailscale",
            status="fail",
            detail="`tailscale` command not found.",
            help="Install Tailscale and authenticate (tailscale up).",
        )

    try:
        # Most reliable JSON output for status checks.
        res = await runner.run(["tailscale", "status", "--json"], timeout_s=10.0)
        if res.returncode != 0:
            return CheckItem(
                name="Tailscale",
                status="fail",
                detail=res.stderr.strip() or f"Non-zero exit code: {res.returncode}",
                help="Run `tailscale status` and `tailscale up` to restore connectivity.",
            )

        data = json.loads(res.stdout or "{}")
        backend_state = data.get("BackendState") or data.get("backendState") or ""
        self_info = data.get("Self") or {}
        ips = (
            self_info.get("TailscaleIPs")
            or self_info.get("TailscaleIP")
            or []
        )

        ips = ips if isinstance(ips, list) else [ips] if ips else []
        has_v4 = any(str(ip).startswith("100.") for ip in ips)
        has_v6 = any(_IP_V6_PREFIX.match(str(ip)) for ip in ips)

        if (backend_state and backend_state != "Running") and not (has_v4 or has_v6):
            return CheckItem(
                name="Tailscale",
                status="warn",
                detail=f"BackendState={backend_state}; no expected Tailscale IP found.",
                help="Make sure Tailscale is logged in and assigned an IPv4 in 100.64.0.0/10 or IPv6 fd7a::/16.",
            )

        if (has_v4 or has_v6):
            return CheckItem(
                name="Tailscale",
                status="ok",
                detail=f"Connected (BackendState={backend_state or 'unknown'}). IPs={ips}",
            )

        return CheckItem(
            name="Tailscale",
            status="warn",
            detail=f"Detected Tailscale, but no 100.x.x.x/ fd7a:: IP in Self.TailscaleIPs. BackendState={backend_state or 'unknown'}",
            help="Run `tailscale status` and `tailscale up` to re-establish mesh connectivity.",
        )
    except Exception as e:
        return CheckItem(
            name="Tailscale",
            status="fail",
            detail=f"Failed to run tailscale status: {e}",
            help="Ensure Tailscale CLI is installed and your account is authenticated.",
        )


def _ollama_base_url() -> str:
    """Base URL for the local Ollama HTTP API (GET /api/tags, etc.)."""
    for key in ("OLLAMA_URL", "OLLAMA_BASE_URL"):
        raw = os.getenv(key, "").strip()
        if raw:
            return raw.rstrip("/")
    # 127.0.0.1 avoids some Windows setups where `localhost` resolves to ::1 first
    # while Ollama is only listening on IPv4.
    return "http://127.0.0.1:11434"


def _ollama_tag_names_from_models(models: List[Any]) -> List[str]:
    """Collect unique model tags; Ollama uses `name` and/or `model` on each entry."""
    seen: set[str] = set()
    out: List[str] = []
    for m in models:
        if not isinstance(m, dict):
            continue
        tag = (m.get("name") or m.get("model") or "").strip()
        if not tag or tag in seen:
            continue
        seen.add(tag)
        out.append(tag)
    return out


async def check_ollama() -> CheckItem:
    ollama_url = _ollama_base_url()
    tags_url = f"{ollama_url}/api/tags"
    try:
        async with httpx.AsyncClient(timeout=4.0) as client:
            res = await client.get(tags_url, headers={"Accept": "application/json"})
            res.raise_for_status()
            ct = (res.headers.get("content-type") or "").lower()
            if "json" not in ct and (res.text or "").lstrip().startswith("<"):
                return CheckItem(
                    name="Ollama + Models",
                    status="fail",
                    detail=f"Expected JSON from {tags_url}, got HTML (wrong port or HTTP proxy?).",
                    help="Set OLLAMA_URL to the full API base, e.g. http://127.0.0.1:11434",
                )
            payload = res.json() or {}
        if not isinstance(payload, dict):
            return CheckItem(
                name="Ollama + Models",
                status="fail",
                detail="Ollama /api/tags returned a non-object JSON body.",
                help="Confirm this host is the Ollama server (same API as `curl /api/tags`).",
            )
        if "models" not in payload:
            return CheckItem(
                name="Ollama + Models",
                status="fail",
                detail="JSON response has no `models` key (not Ollama, or incompatible API).",
                help="Verify OLLAMA_URL points at Ollama's API root; try `curl {0}`.".format(tags_url),
            )
        models = payload.get("models") or []
        if not isinstance(models, list):
            return CheckItem(
                name="Ollama + Models",
                status="fail",
                detail="`models` in /api/tags is not a list.",
                help="Upgrade Ollama or fix OLLAMA_URL if you are hitting a different service.",
            )
        model_names = _ollama_tag_names_from_models(models)
        if model_names:
            n = len(model_names)
            return CheckItem(
                name="Ollama + Models",
                status="ok",
                detail=f"Ollama reachable at {ollama_url}; {n} local model tag(s) listed below.",
                models=model_names,
            )
        return CheckItem(
            name="Ollama + Models",
            status="warn",
            detail=f"Ollama reachable at {ollama_url}, but no models are present.",
            help="Run: `ollama pull <model>` and ensure models appear under /api/tags.",
            models=[],
        )
    except Exception as e:
        return CheckItem(
            name="Ollama + Models",
            status="fail",
            detail=f"Cannot reach Ollama at {ollama_url}: {e}",
            help="Start Ollama, set OLLAMA_URL if it is not on 127.0.0.1:11434, then pull a model.",
        )


def _tcp_connect(host: str, port: int, timeout_s: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return True
    except Exception:
        return False


async def check_kafka_broker() -> CheckItem:
    broker = os.getenv("KAFKA_BROKER", "localhost:9092").strip()
    if not broker:
        return CheckItem(name="Kafka broker", status="fail", detail="KAFKA_BROKER not set.")

    # "host:port"
    if ":" not in broker:
        return CheckItem(name="Kafka broker", status="fail", detail=f"Unexpected KAFKA_BROKER value: {broker}")
    host, port_s = broker.rsplit(":", 1)
    try:
        port = int(port_s)
    except ValueError:
        return CheckItem(name="Kafka broker", status="fail", detail=f"Invalid port in KAFKA_BROKER: {broker}")

    ok = await asyncio.to_thread(_tcp_connect, host, port, 2.0)
    if ok:
        return CheckItem(name="Kafka broker", status="ok", detail=f"Reachable at {host}:{port}")
    return CheckItem(
        name="Kafka broker",
        status="fail",
        detail=f"Cannot connect to {host}:{port}.",
        help="Run docker compose Kafka (single-broker) or the multi-broker setup.",
    )


async def check_cassandra_schema() -> CheckItem:
    """
    Probes the database via the cassandra-driver session that database.py
    initializes. Works for both connection modes:
      USE_ASTRA=true  -> validates that Astra credentials + bundle work and
                         the keyspace contains the expected tables.
      USE_ASTRA=false -> validates that local docker Cassandra is up and the
                         schema has been applied.
    """
    from database import USE_ASTRA, _get_session, _keyspace

    mode_label = "Astra" if USE_ASTRA else "local docker"
    expected_tables = {"users", "requests", "cluster_requests_by_month"}

    try:
        session = await asyncio.to_thread(_get_session)
        rows = await asyncio.to_thread(
            session.execute,
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s",
            (_keyspace(),),
        )
        present = {r.table_name for r in rows}
        missing = expected_tables - present
        if missing:
            return CheckItem(
                name="Cassandra schema",
                status="warn",
                detail=f"Connected to {mode_label} keyspace `{_keyspace()}` but missing tables: {sorted(missing)}.",
                help="Apply client/db/002_tables.cql against this keyspace (Astra console CQL editor for cloud, or cqlsh for local).",
            )
        return CheckItem(
            name="Cassandra schema",
            status="ok",
            detail=f"Connected to {mode_label} keyspace `{_keyspace()}`; all tables present.",
        )
    except Exception as e:
        if USE_ASTRA:
            help_text = (
                "Set ASTRA_BUNDLE_PATH, ASTRA_CLIENT_ID, ASTRA_CLIENT_SECRET in "
                "server/leader/.env and confirm the secure-connect-bundle .zip exists at the configured path."
            )
        else:
            help_text = (
                "Run `docker compose up -d cassandra`, apply the CQL files, "
                "and confirm LOCAL_CASSANDRA_HOST/PORT (default 127.0.0.1:9042) is reachable."
            )
        return CheckItem(
            name="Cassandra schema",
            status="fail",
            detail=f"Cannot reach Cassandra ({mode_label}): {e}",
            help=help_text,
        )


async def run_all_requirements_checks() -> Dict[str, Any]:
    runner = CommandRunnerFactory.create()

    checks: List[CheckItem] = []
    checks.append(await check_tailscale(runner))
    checks.append(await check_ollama())
    checks.append(await check_kafka_broker())
    checks.append(await check_cassandra_schema())

    # quick roll-up
    statuses = [c.status for c in checks]
    overall = "ok" if all(s == "ok" for s in statuses) else "warn" if "fail" not in statuses else "fail"
    return {
        "overall": overall,
        "checkedAt": int(time.time()),
        "checks": [c.as_dict() for c in checks],
    }

