from __future__ import annotations

import json
import os
import socket
import subprocess
from typing import Optional


def _run(cmd: list[str], timeout_seconds: int = 3) -> Optional[str]:
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_seconds, check=True)
        out = (res.stdout or "").strip()
        return out or None
    except Exception:
        return None


def get_tailscale_ip() -> Optional[str]:
    """
    Best-effort: returns the Tailscale IPv4 if available.
    """
    out = _run(["tailscale", "ip", "-4"])
    if out:
        # can return multiple lines; take first
        return out.splitlines()[0].strip() or None

    out = _run(["tailscale", "status", "--json"])
    if out:
        try:
            data = json.loads(out)
            self_data = data.get("Self") or {}
            addrs = self_data.get("TailscaleIPs") or []
            for a in addrs:
                if "." in a:
                    return a
        except Exception:
            pass
    return None


def get_tailscale_hostname() -> Optional[str]:
    """
    Best-effort: returns the MagicDNS name or a stable name from `tailscale status --json`.
    """
    out = _run(["tailscale", "status", "--json"])
    if out:
        try:
            data = json.loads(out)
            self_data = data.get("Self") or {}
            # Prefer DNSName if present (often ends with tailnet.ts.net).
            dns = self_data.get("DNSName")
            if dns:
                return str(dns).strip() or None
            # Fall back to HostName
            hn = self_data.get("HostName")
            if hn:
                return str(hn).strip() or None
        except Exception:
            pass
    return None


def get_advertise_host() -> str:
    """
    Returns a host string that other nodes can reach.
    Explicit LEADER_ADVERTISE_HOST env var wins; otherwise prefer MagicDNS,
    then Tailscale IP, then the local hostname.

    The env override matters on single-host dev (especially WSL2), where
    socket.gethostname() returns the Windows host name — resolvable but not
    reachable from the worker process.
    """
    override = os.getenv("LEADER_ADVERTISE_HOST", "").strip()
    if override:
        return override
    hn = get_tailscale_hostname()
    if hn:
        return hn
    ip = get_tailscale_ip()
    if ip:
        return ip
    return socket.gethostname()

