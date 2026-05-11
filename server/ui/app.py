"""
Server Node UI — self-contained Python web app.

Serves a browser UI at http://localhost:8001 that shows:
  /          → Setup Check  (system requirements: Tailscale, Ollama, Kafka, Cassandra)
  /servers   → Cluster Status  (leader + known nodes)
  /join      → Join cluster  (bootstrap URL + join code → POST /cluster/join)
  /api/checks       → JSON for Setup Check
  /api/servers      → JSON for Cluster Status
  /api/join         → Proxy join to bootstrap leader
  /api/mint-join-code → POST /cluster/create on configured leader (fresh join JSON)
  /api/bootstrap-hints → Tailscale / MagicDNS / leader URLs to paste as bootstrap

No React, no npm.  Pure Python + Jinja2 + minimal inline CSS/JS.

Run:
  cd server/ui
  pip install -r requirements.txt
  python app.py            # defaults: UI on 0.0.0.0:8001, leader at http://localhost:8000

  python app.py --port 8001 --leader http://100.64.0.5:8000

Environment (optional; a .env in cwd is loaded via python-dotenv):
  UI_HOST, UI_PORT              — bind address and port (default host 0.0.0.0 for LAN)
  LEADER_URL                    — leader base URL (overridden by --leader)
  UI_LEADER_HTTP_TIMEOUT        — seconds for leader HTTP client (default: 5)
  UI_LEADER_STATUS_PATH         — path under leader for cluster JSON (default: /server/status)
  KAFKA_BROKER                  — fallback when leader response omits kafka_broker

Leader (not UI): JOIN_TOKEN_EXPIRES_SECONDS — join token lifetime (default 86400 = 24h).

Strategy/Factory pattern is used for OS-level command runner differences
(Windows vs POSIX).  See system_checks.py for details.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

# Allow importing system_checks.py from ../leader without installing it as a package.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "leader"))

import json
import os
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from fastapi.templating import Jinja2Templates

from system_checks import run_all_requirements_checks
from tailscale_utils import get_advertise_host, get_tailscale_hostname, get_tailscale_ip

load_dotenv()

_HTML_CACHE_HEADERS = {"Cache-Control": "no-store, max-age=0"}


def _leader_scheme_host_port(leader_base: str) -> tuple[str, str, int]:
    raw = leader_base.strip()
    if "://" not in raw:
        raw = "http://" + raw
    u = urlparse(raw)
    scheme = (u.scheme or "http").lower()
    host = (u.hostname or "").strip() or "127.0.0.1"
    if u.port is not None:
        port = u.port
    else:
        port = 443 if scheme == "https" else 8000
    return scheme, host, port


def bootstrap_hints_for_leader(leader_base: str) -> Dict[str, Any]:
    """
    Build de-duplicated http(s)://host:port rows for the Join page (Tailscale IP,
    MagicDNS, advertise host, configured leader). Uses the same port as LEADER_URL.
    """
    scheme, _, port = _leader_scheme_host_port(leader_base)
    rows: List[Dict[str, str]] = []
    seen: set[str] = set()

    def add(label: str, url: str, hint: str) -> None:
        u = (url or "").strip().rstrip("/")
        if not u or u in seen:
            return
        seen.add(u)
        rows.append({"label": label, "url": u, "hint": hint})

    try:
        ts = get_tailscale_ip()
        if ts:
            add("Tailscale IPv4 (100.x…)", f"{scheme}://{ts}:{port}", "Paste this as bootstrap on other tailnet laptops.")
        magic = get_tailscale_hostname()
        if magic:
            h = magic.rstrip(".").strip()
            add("MagicDNS (Tailscale)", f"{scheme}://{h}:{port}", "Stable name; works only inside Tailscale.")
        adv = get_advertise_host()
        if adv:
            h = adv.rstrip(".").strip()
            add("Advertised host (this machine)", f"{scheme}://{h}:{port}", "Same logic the leader uses for seed URLs.")
    except Exception:
        pass

    add("Leader URL (this UI)", leader_base.rstrip("/"), "Matches --leader / LEADER_URL for Cluster Status.")
    return {"scheme": scheme, "port": port, "candidates": rows}


def _leader_status_url(leader_base: str, status_path: str) -> str:
    base = leader_base.rstrip("/")
    path = (status_path or "/server/status").strip()
    if not path.startswith("/"):
        path = "/" + path
    return f"{base}{path}"


def _leader_local_node_url(leader_base: str) -> str:
    return f"{leader_base.rstrip('/')}/server/local-node"


def _coerce_join_code_dict(raw: Any) -> Optional[Dict[str, Any]]:
    """
    Accept either the inner join bundle or the full POST /cluster/create response
    (where cluster_id + join_token live under join_code).
    """
    if isinstance(raw, str):
        try:
            raw = json.loads(raw.strip() or "{}")
        except json.JSONDecodeError:
            return None
    if not isinstance(raw, dict):
        return None
    inner = raw.get("join_code")
    if isinstance(inner, dict) and inner.get("join_token"):
        merged = {**inner}
        if raw.get("cluster_id") and not merged.get("cluster_id"):
            merged["cluster_id"] = raw["cluster_id"]
        return merged
    return raw


def _normalize_node_for_join(node: Dict[str, Any]) -> Dict[str, Any]:
    """Coerce types so FastAPI NodeInfo on the bootstrap host accepts the payload."""
    n = dict(node)

    def _i(key: str, default: int) -> int:
        try:
            return int(n.get(key, default))
        except (TypeError, ValueError):
            return default

    n["port"] = _i("port", 8000)
    n["priority"] = _i("priority", 10)
    n["last_heartbeat"] = _i("last_heartbeat", 0)
    n["tasks_completed"] = _i("tasks_completed", 0)
    if not isinstance(n.get("models"), list):
        n["models"] = []
    if not isinstance(n.get("skills"), list):
        n["skills"] = []
    if n.get("ram_gb") is not None:
        try:
            n["ram_gb"] = float(n["ram_gb"])
        except (TypeError, ValueError):
            n["ram_gb"] = None
    return n


def _http_error_detail(r: httpx.Response) -> Any:
    try:
        return r.json()
    except Exception:
        return (r.text or "")[:4000]


class JoinSubmitBody(BaseModel):
    """Bootstrap is one seed HTTP base URL; join_code is the JSON from create-cluster."""

    bootstrap_base: str = ""
    join_code: Any = Field(..., description="Join code object (or parse from string on client)")
    node: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class UiSettings:
    leader_url: str
    leader_http_timeout: float
    leader_status_path: str


def create_app(settings: UiSettings) -> FastAPI:
    leader_url = settings.leader_url.rstrip("/")
    status_url = _leader_status_url(leader_url, settings.leader_status_path)
    httpx_timeout = settings.leader_http_timeout
    app = FastAPI(title="Server Node UI", docs_url=None, redoc_url=None)
    templates_dir = Path(__file__).resolve().parent / "templates"
    templates = Jinja2Templates(directory=str(templates_dir))

    from datetime import datetime, timezone

    def _ts_filter(epoch: int) -> str:
        if not epoch:
            return "—"
        try:
            return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        except Exception:
            return str(epoch)

    templates.env.filters["timestamp"] = _ts_filter

    # ── API endpoints ──────────────────────────────────────────────────────────

    @app.get("/health")
    async def health():
        """Lightweight liveness probe (does not call the leader or run system checks)."""
        return {"status": "ok", "service": "server-node-ui"}

    @app.get("/api/checks")
    async def api_checks():
        result = await run_all_requirements_checks()
        return JSONResponse(result)

    @app.get("/api/bootstrap-hints")
    async def api_bootstrap_hints():
        """URLs other nodes can use as POST /cluster/join bootstrap (same port as configured leader)."""
        return JSONResponse(bootstrap_hints_for_leader(leader_url))

    @app.get("/api/servers")
    async def api_servers():
        try:
            async with httpx.AsyncClient(timeout=httpx_timeout) as client:
                r = await client.get(status_url)
                r.raise_for_status()
                return JSONResponse(r.json())
        except Exception as exc:
            return JSONResponse(
                {
                    "error": str(exc),
                    "cluster": None,
                    "nodes": [],
                    "kafka_broker": os.getenv("KAFKA_BROKER", "localhost:9092"),
                },
                status_code=200,
            )

    @app.post("/api/join")
    async def api_join(body: JoinSubmitBody):
        """
        POST /cluster/join on the bootstrap host. Node payload defaults to GET
        {UI_LEADER_URL}/server/local-node (same --leader as Cluster Status).
        """
        jc = _coerce_join_code_dict(body.join_code)
        if jc is None:
            return JSONResponse(
                {"success": False, "error": "join_code is not valid JSON or not an object."},
                status_code=400,
            )

        cluster_id = str(jc.get("cluster_id") or "").strip() or None
        join_token = str(jc.get("join_token") or "").strip() or None
        if not cluster_id or not join_token:
            return JSONResponse(
                {
                    "success": False,
                    "error": "join_code must include cluster_id and join_token (from the cluster admin's join bundle).",
                },
                status_code=400,
            )

        raw_bootstrap = (body.bootstrap_base or "").strip().rstrip("/")
        seeds = jc.get("seed_nodes") or []
        first_seed = ""
        if isinstance(seeds, list) and seeds:
            s0 = seeds[0]
            if isinstance(s0, str):
                first_seed = s0.strip().rstrip("/")
            elif s0 is not None:
                first_seed = str(s0).strip().rstrip("/")
                if first_seed.lower() == "none":
                    first_seed = ""
        target = raw_bootstrap or first_seed
        if not target:
            return JSONResponse(
                {
                    "success": False,
                    "error": "Set bootstrap base URL or include seed_nodes in the join code.",
                },
                status_code=400,
            )

        node = body.node
        if node is None:
            try:
                async with httpx.AsyncClient(timeout=httpx_timeout) as client:
                    lr = await client.get(_leader_local_node_url(leader_url))
                    lr.raise_for_status()
                    node = lr.json()
            except Exception as exc:
                return JSONResponse(
                    {
                        "success": False,
                        "error": (
                            f"Could not GET node identity from {_leader_local_node_url(leader_url)}: {exc}. "
                            "Start the leader on this machine, set UI --leader / LEADER_URL to that API, "
                            "or pass a full `node` object in the JSON body."
                        ),
                    },
                    status_code=400,
                )

        node = _normalize_node_for_join(node)
        payload = {"cluster_id": cluster_id, "join_token": join_token, "node": node}
        try:
            async with httpx.AsyncClient(timeout=httpx_timeout) as client:
                r = await client.post(f"{target}/cluster/join", json=payload)
                if r.is_client_error or r.is_server_error:
                    detail = _http_error_detail(r)
                    err_body: Dict[str, Any] = {
                        "success": False,
                        "error": f"Bootstrap returned HTTP {r.status_code}",
                        "detail": detail,
                        "posted_to": f"{target}/cluster/join",
                    }
                    inner = detail.get("detail") if isinstance(detail, dict) else None
                    if isinstance(inner, dict):
                        code = inner.get("code")
                        if code == "join_token_expired":
                            err_body["hint"] = (
                                "This join token is past its expiry. Click «Get fresh join code» below "
                                "(or POST /cluster/create on the bootstrap host), then try again."
                            )
                        elif code in ("join_token_mismatch", "no_active_join_token"):
                            err_body["hint"] = (
                                "Token does not match the bootstrap server, or the server never minted one. "
                                "Use «Get fresh join code» if this UI points at that leader."
                            )
                    return JSONResponse(err_body, status_code=400)
                data = r.json()
        except Exception as exc:
            return JSONResponse(
                {"success": False, "error": str(exc), "posted_to": f"{target}/cluster/join"},
                status_code=502,
            )

        hint = (
            "Membership updated on the bootstrap host. Cluster Status still lists workers from "
            "the leader registry until this node POSTs /register (e.g. run worker.py)."
        )
        return JSONResponse(
            {"success": True, "posted_to": f"{target}/cluster/join", **data, "hint": hint},
        )

    @app.post("/api/mint-join-code")
    async def api_mint_join_code():
        """
        Proxies POST /cluster/create to the UI-configured leader so you can paste
        a new join bundle without curl. Keeps existing cluster_id when cluster exists.
        """
        try:
            async with httpx.AsyncClient(timeout=httpx_timeout) as client:
                r = await client.post(
                    f"{leader_url}/cluster/create",
                    json={"role": "both", "priority": 100, "port": 8000},
                )
                if r.is_client_error or r.is_server_error:
                    return JSONResponse(
                        {
                            "success": False,
                            "error": f"Leader returned HTTP {r.status_code}",
                            "detail": _http_error_detail(r),
                            "posted_to": f"{leader_url}/cluster/create",
                        },
                        status_code=400,
                    )
                data = r.json()
        except Exception as exc:
            return JSONResponse(
                {
                    "success": False,
                    "error": str(exc),
                    "posted_to": f"{leader_url}/cluster/create",
                },
                status_code=502,
            )
        return JSONResponse({"success": True, **data})

    # ── Page endpoints ─────────────────────────────────────────────────────────

    @app.get("/", response_class=HTMLResponse)
    async def page_setup(request: Request):
        # Do not await run_all_requirements_checks() here — Tailscale/Docker/Cassandra
        # can take 30s+ and the browser would show a blank page until HTML is sent.
        # Checks load via GET /api/checks after paint (see layout.html).
        return templates.TemplateResponse(
            "layout.html",
            {
                "request": request,
                "page": "setup",
                "leader_url": leader_url,
            },
            headers=_HTML_CACHE_HEADERS,
        )

    @app.get("/servers", response_class=HTMLResponse)
    async def page_servers(request: Request):
        data: dict = {}
        error: str = ""
        try:
            async with httpx.AsyncClient(timeout=httpx_timeout) as client:
                r = await client.get(status_url)
                r.raise_for_status()
                data = r.json()
        except Exception as exc:
            error = str(exc)

        return templates.TemplateResponse(
            "layout.html",
            {
                "request": request,
                "page": "servers",
                "cluster": data.get("cluster") or {},
                "nodes": data.get("nodes") or [],
                "kafka_broker": data.get("kafka_broker", os.getenv("KAFKA_BROKER", "localhost:9092")),
                "leader_url": leader_url,
                "error": error,
            },
            headers=_HTML_CACHE_HEADERS,
        )

    @app.get("/join", response_class=HTMLResponse)
    @app.get("/join/", response_class=HTMLResponse)
    async def page_join(request: Request):
        return templates.TemplateResponse(
            "layout.html",
            {
                "request": request,
                "page": "join",
                "leader_url": leader_url,
            },
            headers=_HTML_CACHE_HEADERS,
        )

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Server Node UI")
    parser.add_argument(
        "--host",
        default=os.getenv("UI_HOST", "0.0.0.0"),
        help="Bind address (default: UI_HOST or 0.0.0.0 for LAN; use 127.0.0.1 for loopback only)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("UI_PORT", "8001")),
        help="Port (default: UI_PORT or 8001)",
    )
    parser.add_argument(
        "--leader",
        default=os.getenv("LEADER_URL", "http://localhost:8000"),
        help="Leader API base URL (default: LEADER_URL or http://localhost:8000)",
    )
    parser.add_argument(
        "--leader-timeout",
        type=float,
        default=float(os.getenv("UI_LEADER_HTTP_TIMEOUT", "5.0")),
        help="HTTP timeout in seconds when calling the leader (default: UI_LEADER_HTTP_TIMEOUT or 5)",
    )
    parser.add_argument(
        "--leader-status-path",
        default=os.getenv("UI_LEADER_STATUS_PATH", "/server/status"),
        help="Path on leader for cluster status JSON (default: UI_LEADER_STATUS_PATH or /server/status)",
    )
    args = parser.parse_args()

    import uvicorn

    settings = UiSettings(
        leader_url=args.leader.rstrip("/"),
        leader_http_timeout=args.leader_timeout,
        leader_status_path=args.leader_status_path,
    )
    app = create_app(settings)
    print(f"Server UI -> http://{args.host}:{args.port}")
    if args.host in ("0.0.0.0", "::", "[::]"):
        base = f"http://127.0.0.1:{args.port}"
        print(f"  Open in browser:  {base}/  (setup)   {base}/servers   {base}/join")
        print(f"  Health check:       {base}/health")
        print(f"  Or use this machine's LAN IP instead of 127.0.0.1 if needed.")
    print(f"Leader URL -> {args.leader}")
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
