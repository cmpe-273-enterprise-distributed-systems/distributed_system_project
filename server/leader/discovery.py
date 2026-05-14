"""
DiscoveryClient — publish and resolve the active leader URL via Upstash Redis.

Storage:
  Key   = "leader"
  Value = JSON-encoded record stored as a Redis string:
            {"leader_url": "...", "node_id": "...", "cluster_id": "...", "updated_at": int}
  TTL   = LEADER_KEY_TTL_S (default 900 s). The leader monitor publishes
          every POLL_INTERVAL_S=5 s, so a 15-minute TTL gives 180x headroom
          before a stale entry goes live. Without TTL, a crashed leader's
          URL would stay in Redis forever and misdirect workers after a
          cluster wipe.

REST API (https://upstash.com/docs/redis/features/restapi):
  POST {UPSTASH_REDIS_REST_URL}/set/leader   body = value         → {"result":"OK"}
  GET  {UPSTASH_REDIS_REST_URL}/get/leader                         → {"result":"<value>"|null}
  Both require:  Authorization: Bearer {UPSTASH_REDIS_REST_TOKEN}

The local file (config/leader_url.txt) is preserved as an offline fallback so
the cluster can still recover its bearings if Upstash is briefly unreachable.
"""

import json
import logging
import os
import time
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

_FILE_PATH = Path(__file__).parent / "config" / "leader_url.txt"
LEADER_KEY_TTL_S = 900  # 15 minutes; leader publishes every 5 s so there is 180x headroom


class DiscoveryClient:
    def __init__(self):
        self._url = os.getenv("UPSTASH_REDIS_REST_URL", "").strip().rstrip("/")
        self._token = os.getenv("UPSTASH_REDIS_REST_TOKEN", "").strip()
        _FILE_PATH.parent.mkdir(parents=True, exist_ok=True)

    def _auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}

    async def publish(self, leader_url: str, node_id: str = "", cluster_id: str = "") -> bool:
        """Announce `leader_url` as the active leader. Returns True on success."""
        if not leader_url:
            return False

        try:
            _FILE_PATH.write_text(leader_url.strip())
        except OSError as exc:
            logger.warning("Discovery: could not write local file: %s", exc)

        if not self._url or not self._token:
            return True  # local-only mode

        record = json.dumps({
            "leader_url": leader_url.strip(),
            "node_id": node_id,
            "cluster_id": cluster_id,
            "updated_at": int(time.time()),
        })

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                # Use the Redis pipeline endpoint to SET and EXPIRE atomically.
                # This prevents a stale leader URL persisting in Redis forever
                # if the leader crashes before the key is manually cleared.
                pipeline = [
                    ["SET", "leader", record],
                    ["EXPIRE", "leader", LEADER_KEY_TTL_S],
                ]
                r = await client.post(
                    f"{self._url}/pipeline",
                    content=json.dumps(pipeline),
                    headers={**self._auth_headers(), "Content-Type": "application/json"},
                )
                r.raise_for_status()
                logger.info("Discovery updated -> %s (TTL=%ds)", leader_url, LEADER_KEY_TTL_S)
                return True
        except Exception as exc:
            logger.warning("Discovery publish failed: %s", exc)
            return False

    async def resolve(self) -> str | None:
        """Return the current leader URL, or None if unknown."""
        if self._url and self._token:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    r = await client.get(
                        f"{self._url}/get/leader",
                        headers=self._auth_headers(),
                    )
                    if r.status_code == 200:
                        raw = r.json().get("result")
                        if raw:
                            url = (json.loads(raw).get("leader_url") or "").strip()
                            if url:
                                return url
            except Exception as exc:
                logger.debug("Discovery remote resolve failed: %s", exc)

        if _FILE_PATH.exists():
            url = _FILE_PATH.read_text().strip()
            return url or None

        return None
