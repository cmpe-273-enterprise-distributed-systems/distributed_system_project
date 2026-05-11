from __future__ import annotations

import base64
import copy
import os
import secrets
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import yaml

STATE_PATH = os.path.join(os.path.dirname(__file__), "config", "cluster_state.yaml")


def _now() -> int:
    return int(time.time())


def _ensure_dir_for(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def _safe_load_yaml(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        return {}
    return data


def _safe_dump_yaml(path: str, data: Dict[str, Any]) -> None:
    _ensure_dir_for(path)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False)


def _normalize_node(node: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure required keys exist with reasonable defaults.
    """
    n = dict(node)
    n.setdefault("role", "worker")
    n.setdefault("priority", 10)
    n.setdefault("port", 8000)
    n.setdefault("ram_gb", None)
    n.setdefault("models", [])
    n.setdefault("skills", [])
    n.setdefault("status", "alive")
    n.setdefault("last_heartbeat", 0)
    n.setdefault("tasks_completed", 0)
    host = n.get("host") or "unknown"
    port = n.get("port") or 8000
    n["url"] = n.get("url") or f"http://{host}:{port}"
    return n


@dataclass
class ClusterState:
    cluster_id: Optional[str] = None
    join_token: Optional[str] = None
    join_token_expires_at: Optional[int] = None
    current_leader: Optional[Dict[str, Any]] = None
    known_nodes: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    this_node_id: Optional[str] = None
    this_node_role: str = "both"
    this_node_priority: int = 100

    def to_dict(self) -> Dict[str, Any]:
        return {
            "cluster_id": self.cluster_id,
            "join_token": self.join_token,
            "join_token_expires_at": self.join_token_expires_at,
            "current_leader": self.current_leader,
            "known_nodes": list(self.known_nodes.values()),
            "this_node_id": self.this_node_id,
            "this_node_role": self.this_node_role,
            "this_node_priority": self.this_node_priority,
            "updated_at": _now(),
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "ClusterState":
        cs = ClusterState()
        cs.cluster_id = data.get("cluster_id")
        cs.join_token = data.get("join_token")
        cs.join_token_expires_at = data.get("join_token_expires_at")
        cs.current_leader = data.get("current_leader")
        cs.this_node_id = data.get("this_node_id")
        cs.this_node_role = data.get("this_node_role") or "both"
        cs.this_node_priority = int(data.get("this_node_priority") or 100)

        nodes = data.get("known_nodes") or []
        if isinstance(nodes, dict):
            # tolerate old formats
            nodes = list(nodes.values())
        if isinstance(nodes, list):
            for n in nodes:
                if isinstance(n, dict) and n.get("node_id"):
                    cs.known_nodes[n["node_id"]] = _normalize_node(n)

        # Normalize leader if present.
        if isinstance(cs.current_leader, dict) and cs.current_leader.get("node_id"):
            cs.current_leader = _normalize_node(cs.current_leader)
        else:
            cs.current_leader = None
        return cs


_STATE: ClusterState = ClusterState()


def load_cluster_state() -> ClusterState:
    global _STATE
    data = _safe_load_yaml(STATE_PATH)
    _STATE = ClusterState.from_dict(data)
    return _STATE


def save_cluster_state() -> None:
    global _STATE
    _safe_dump_yaml(STATE_PATH, _STATE.to_dict())


def create_cluster(this_node: Dict[str, Any]) -> ClusterState:
    """
    Create a new cluster if none exists. Adds this node into known_nodes.
    """
    global _STATE
    if not _STATE.cluster_id:
        suffix = base64.b32encode(secrets.token_bytes(4)).decode("utf-8").lower().strip("=").replace("=", "")
        _STATE.cluster_id = f"cmpe273-ai-{suffix}"
    _STATE.this_node_id = this_node.get("node_id") or _STATE.this_node_id
    _STATE.this_node_role = this_node.get("role") or _STATE.this_node_role
    _STATE.this_node_priority = int(this_node.get("priority") or _STATE.this_node_priority)

    add_or_update_node(this_node)
    elect_leader()
    save_cluster_state()
    return _STATE


def generate_join_code(expires_in_seconds: Optional[int] = None) -> Dict[str, Any]:
    """
    Join code is a *first contact* token. It's not DNS; it just tells a new node where to call.

    TTL: env JOIN_TOKEN_EXPIRES_SECONDS (default 86400 = 24h). Older bundles used 600s and
    expired quickly during UI testing.
    """
    global _STATE
    if expires_in_seconds is None:
        try:
            expires_in_seconds = int(os.getenv("JOIN_TOKEN_EXPIRES_SECONDS", "86400"))
        except ValueError:
            expires_in_seconds = 86400
    token = secrets.token_urlsafe(24)
    _STATE.join_token = token
    _STATE.join_token_expires_at = _now() + int(expires_in_seconds)
    save_cluster_state()
    return {
        "cluster_id": _STATE.cluster_id,
        "seed_nodes": [get_leader().get("url")] if get_leader() else [],
        "join_token": token,
        "expires_at": _STATE.join_token_expires_at,
        "message": "Paste this JSON into another node to join the cluster",
    }


def join_token_issue(provided: str) -> Optional[str]:
    """
    None if the token is valid. Otherwise a short machine-readable reason
    (for HTTP 401 detail — do not echo the expected secret).
    """
    global _STATE
    got = (provided or "").strip()
    if not _STATE.join_token or not _STATE.join_token_expires_at:
        return "no_active_join_token"
    if got != _STATE.join_token:
        return "join_token_mismatch"
    if _now() > int(_STATE.join_token_expires_at):
        return "join_token_expired"
    return None


def validate_join_token(token: str) -> bool:
    return join_token_issue(token) is None


def add_or_update_node(node: Dict[str, Any]) -> Dict[str, Any]:
    global _STATE
    node_id = node.get("node_id")
    if not node_id:
        raise ValueError("node_id is required")
    merged = _normalize_node(node)
    existing = _STATE.known_nodes.get(node_id)
    if existing:
        # Keep newer heartbeat, and keep tasks_completed max.
        if int(existing.get("last_heartbeat") or 0) > int(merged.get("last_heartbeat") or 0):
            merged["last_heartbeat"] = existing.get("last_heartbeat")
        merged["tasks_completed"] = max(int(existing.get("tasks_completed") or 0), int(merged.get("tasks_completed") or 0))
        # Preserve status if explicitly dead.
        if existing.get("status") == "dead" and merged.get("status") != "alive":
            merged["status"] = "dead"
    _STATE.known_nodes[node_id] = merged
    return merged


def update_heartbeat(node_id: str, status: str, tasks_completed: int) -> None:
    global _STATE
    n = _STATE.known_nodes.get(node_id) or {"node_id": node_id}
    n = _normalize_node(n)
    n["status"] = status or "alive"
    n["last_heartbeat"] = _now()
    try:
        n["tasks_completed"] = int(tasks_completed)
    except Exception:
        pass
    _STATE.known_nodes[node_id] = n


def mark_dead_nodes(timeout_seconds: int = 15) -> List[str]:
    global _STATE
    now = _now()
    dead: List[str] = []
    for node_id, n in list(_STATE.known_nodes.items()):
        last = int(n.get("last_heartbeat") or 0)
        if last and (now - last) > int(timeout_seconds):
            if n.get("status") != "dead":
                n["status"] = "dead"
                dead.append(node_id)
                _STATE.known_nodes[node_id] = n
    return dead


def _eligible_gateway_nodes(timeout_seconds: int = 15) -> List[Dict[str, Any]]:
    now = _now()
    out = []
    for n in _STATE.known_nodes.values():
        role = (n.get("role") or "").lower()
        if role not in ("gateway", "both"):
            continue
        if n.get("status") == "dead":
            continue
        last = int(n.get("last_heartbeat") or 0)
        if last and (now - last) > int(timeout_seconds):
            continue
        out.append(n)
    return out


def elect_leader(timeout_seconds: int = 15) -> Optional[Dict[str, Any]]:
    """
    Highest priority among alive gateway/both nodes wins.
    Tie-breaker: lexicographically smaller node_id.
    """
    global _STATE
    eligible = _eligible_gateway_nodes(timeout_seconds=timeout_seconds)
    if not eligible:
        _STATE.current_leader = None
        return None
    # sort: priority desc, node_id asc
    eligible.sort(key=lambda n: (-int(n.get("priority") or 0), str(n.get("node_id") or "")))
    leader = _normalize_node(eligible[0])
    _STATE.current_leader = leader
    return leader


def get_leader() -> Optional[Dict[str, Any]]:
    global _STATE
    if _STATE.current_leader and _STATE.current_leader.get("node_id"):
        return _STATE.current_leader
    return elect_leader()


def get_cluster_status() -> Dict[str, Any]:
    global _STATE
    elect_leader()
    nodes = list(_STATE.known_nodes.values())
    nodes.sort(key=lambda n: (n.get("status") != "alive", -(int(n.get("priority") or 0)), str(n.get("node_id") or "")))
    return {
        "cluster_id": _STATE.cluster_id,
        "current_leader": _STATE.current_leader,
        "known_nodes": nodes,
        "timestamp": _now(),
    }


def merge_cluster_state(remote_state: Dict[str, Any]) -> ClusterState:
    """
    Merge known_nodes by node_id. Keep newer last_heartbeat.
    """
    global _STATE
    rid = remote_state.get("cluster_id")
    if rid and not _STATE.cluster_id:
        _STATE.cluster_id = rid
    if rid and _STATE.cluster_id and rid != _STATE.cluster_id:
        # Different cluster; ignore merge.
        return _STATE

    remote_nodes = remote_state.get("known_nodes") or []
    if isinstance(remote_nodes, dict):
        remote_nodes = list(remote_nodes.values())
    if isinstance(remote_nodes, list):
        for n in remote_nodes:
            if not isinstance(n, dict) or not n.get("node_id"):
                continue
            add_or_update_node(n)

    elect_leader()
    save_cluster_state()
    return _STATE

