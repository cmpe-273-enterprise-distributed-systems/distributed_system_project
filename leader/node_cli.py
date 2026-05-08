from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict

import requests


DEFAULT_BASE = "http://127.0.0.1:8000"


def _post(path: str, body: Dict[str, Any] | None = None, base: str = DEFAULT_BASE) -> Any:
    r = requests.post(f"{base}{path}", json=body or {}, timeout=10)
    r.raise_for_status()
    return r.json()


def _get(path: str, base: str = DEFAULT_BASE) -> Any:
    r = requests.get(f"{base}{path}", timeout=10)
    r.raise_for_status()
    return r.json()


def cmd_create_cluster(args) -> int:
    data = _post("/cluster/create", {"role": args.role, "priority": args.priority, "port": args.port}, base=args.base)
    join_code = data.get("join_code") or {}
    print("\n=== JOIN CODE (paste into another laptop) ===\n")
    print(json.dumps(join_code, indent=2))
    print("\n=== Worker join command ===\n")
    print(f"python worker.py --join-code '{json.dumps(join_code)}'")
    return 0


def cmd_status(args) -> int:
    data = _get("/cluster/status", base=args.base)
    print(json.dumps(data, indent=2))
    return 0


def cmd_leader(args) -> int:
    data = _get("/leader", base=args.base)
    print(json.dumps(data, indent=2))
    return 0


def cmd_join_cluster(args) -> int:
    # For the demo CLI, we just print the recommended worker command.
    join_code = json.loads(args.join_code)
    print("Run this on the joining laptop:")
    print(f"python worker.py --join-code '{json.dumps(join_code)}'")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Node CLI for Distributed AI Gateway")
    p.add_argument("--base", default=DEFAULT_BASE, help="Base URL for local node (default: http://127.0.0.1:8000)")
    sub = p.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("create-cluster", help="Create a cluster and print a join code")
    c.add_argument("--role", default="both", help="Role for this node (gateway|both)")
    c.add_argument("--priority", type=int, default=100, help="Leader election priority (higher wins)")
    c.add_argument("--port", type=int, default=8000, help="Port this node is serving on")
    c.set_defaults(fn=cmd_create_cluster)

    j = sub.add_parser("join-cluster", help="Print join command for worker")
    j.add_argument("--join-code", required=True, help="Join code JSON (paste from create-cluster output)")
    j.set_defaults(fn=cmd_join_cluster)

    s = sub.add_parser("status", help="Show cluster status")
    s.set_defaults(fn=cmd_status)

    l = sub.add_parser("leader", help="Show current leader")
    l.set_defaults(fn=cmd_leader)

    args = p.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())

