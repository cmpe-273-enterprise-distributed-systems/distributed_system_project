from __future__ import annotations

import os
import uuid
from typing import Any, Dict

import yaml

from tailscale_utils import get_advertise_host

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config", "node.yaml")


def _ensure_dir_for(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def save_node_config(config: Dict[str, Any]) -> None:
    _ensure_dir_for(CONFIG_PATH)
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        yaml.safe_dump(config, f, sort_keys=False)


def load_or_create_node_config(role: str = "both", priority: int = 100, port: int = 8000) -> Dict[str, Any]:
    """
    Node ID must be stable across restarts once created.
    """
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        if isinstance(data, dict) and data.get("node_id"):
            # Refresh host/port if needed, keep node_id stable.
            data.setdefault("role", role)
            data.setdefault("priority", int(priority))
            data.setdefault("port", int(port))
            data["host"] = data.get("host") or get_advertise_host()
            save_node_config(data)
            return data

    node_id = f"node_{uuid.uuid4().hex[:6]}"
    cfg = {
        "node_id": node_id,
        "role": role,
        "priority": int(priority),
        "host": get_advertise_host(),
        "port": int(port),
    }
    save_node_config(cfg)
    return cfg

