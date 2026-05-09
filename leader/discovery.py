"""
DiscoveryClient — publish and resolve the active leader URL.

Backends (tried in order):
  1. HTTP  — set DISCOVERY_URL env var to any PUT-able endpoint
             (Cloudflare Worker KV, a simple hosted text file, etc.).
             PUT writes the leader URL; GET reads it back.
  2. File  — config/leader_url.txt — always written locally; used as
             fallback when DISCOVERY_URL is unset or unreachable.

The leader node exposes GET /discovery/leader so clients and workers can
ask any known seed node for the current leader without external infra.
"""

import logging
import os
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

_FILE_PATH = Path(__file__).parent / "config" / "leader_url.txt"


class DiscoveryClient:
    def __init__(self):
        self._url = os.getenv("DISCOVERY_URL", "").strip()
        _FILE_PATH.parent.mkdir(parents=True, exist_ok=True)

    async def publish(self, leader_url: str) -> bool:
        """Announce that `leader_url` is the active leader. Returns True on success."""
        if not leader_url:
            return False

        try:
            _FILE_PATH.write_text(leader_url.strip())
        except OSError as exc:
            logger.warning("Discovery: could not write local file: %s", exc)

        if not self._url:
            return True

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                r = await client.put(
                    self._url,
                    content=leader_url.encode(),
                    headers={"Content-Type": "text/plain"},
                )
                r.raise_for_status()
                logger.info("Discovery updated: %s -> %s", self._url, leader_url)
                return True
        except Exception as exc:
            logger.warning("Discovery publish to %s failed: %s", self._url, exc)
            return False

    async def resolve(self) -> str | None:
        """Return the current leader URL, or None if unknown."""
        if self._url:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    r = await client.get(self._url)
                    if r.status_code == 200:
                        return r.text.strip() or None
            except Exception as exc:
                logger.debug("Discovery remote resolve failed: %s", exc)

        if _FILE_PATH.exists():
            url = _FILE_PATH.read_text().strip()
            return url or None

        return None
