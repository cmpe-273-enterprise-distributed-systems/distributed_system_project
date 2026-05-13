"""
LeaderMonitor — distributed leader failure detection and re-election.

Algorithm (simplified Bully):
  Every POLL_INTERVAL seconds each node:
  1. Self-heartbeats into local cluster_state so peers' mark_dead_nodes()
     won't evict this node.
  2. Reaps nodes that stopped heartbeating (marks them dead).
  3. If this node IS the leader, runs a quorum check by pinging each
     leader-eligible peer's /health. If a majority of leader-eligible
     nodes (including self) is unreachable, self-demote: stop publishing
     to discovery and flip the is_local_leader flag so the FastAPI gate
     starts returning 503. Without this check a partitioned leader would
     keep publishing itself to discovery while peers elect their own
     leader on the other side of the split.
  4. Otherwise, GET {leader}/health.  After FAILURE_THRESHOLD consecutive
     failures the leader is declared dead.
  5. elect_leader() is deterministic — every node with consistent known_nodes
     picks the same winner (highest priority among alive gateway/both nodes).
  6. The winner broadcasts the new cluster state to all alive peers via
     POST /cluster/sync, then publishes itself to the discovery service.
"""

import asyncio
import logging

import httpx

from cluster_state import (
    elect_leader,
    get_cluster_status,
    get_leader,
    load_cluster_state,
    mark_dead_nodes,
    save_cluster_state,
    update_heartbeat,
)
from discovery import DiscoveryClient

logger = logging.getLogger(__name__)

POLL_INTERVAL_S = 5
FAILURE_THRESHOLD = 3
HEALTH_TIMEOUT_S = 3.0
SYNC_TIMEOUT_S = 4.0


class LeaderMonitor:
    def __init__(self, node_id: str, node_url: str, discovery: DiscoveryClient):
        self._node_id = node_id
        self._node_url = node_url
        self._discovery = discovery
        self._failures = 0
        # True when this node holds the leader role per local cluster_state but
        # has lost contact with a majority of leader-eligible peers. While
        # demoted, do NOT publish to discovery (let peers elect a real leader
        # on their side of the partition) and let the FastAPI gate refuse
        # user-facing requests via is_local_leader.
        self._demoted = False

    @property
    def is_local_leader(self) -> bool:
        """
        True iff cluster_state says this node is the elected leader AND we
        haven't self-demoted due to quorum loss. The /ask, /register, etc.
        endpoints check this to decide whether to serve or 503.
        """
        leader = get_leader()
        return bool(
            leader
            and leader.get("node_id") == self._node_id
            and not self._demoted
        )

    async def run(self) -> None:
        while True:
            await asyncio.sleep(POLL_INTERVAL_S)
            try:
                await self._tick()
            except Exception:
                logger.exception("LeaderMonitor tick error")

    async def _tick(self) -> None:
        # Keep this node alive in cluster_state so peers don't evict it.
        update_heartbeat(self._node_id, "alive", 0)
        save_cluster_state()

        mark_dead_nodes(timeout_seconds=POLL_INTERVAL_S * FAILURE_THRESHOLD)

        leader = get_leader()
        if not leader:
            await self._trigger_election()
            return

        if leader.get("node_id") == self._node_id:
            await self._leader_tick()
            return

        alive = await _ping(leader.get("url", ""))
        if alive:
            self._failures = 0
            return

        self._failures += 1
        logger.warning(
            "Leader %s unreachable (%d/%d)",
            leader.get("url"), self._failures, FAILURE_THRESHOLD,
        )
        if self._failures >= FAILURE_THRESHOLD:
            self._failures = 0
            await self._trigger_election()

    async def _leader_tick(self) -> None:
        """
        Tick body when this node holds the leader role. Verifies quorum of
        leader-eligible peers before publishing to discovery; if the partition
        has cut us off, we self-demote and stay quiet so the rest of the
        cluster can elect a real leader.
        """
        self._failures = 0

        peers = _leader_eligible_peers(self._node_id)
        if peers:
            reachable = await _count_reachable(peers)
            total_eligible = len(peers) + 1  # include self
            quorum = total_eligible // 2 + 1
            if (reachable + 1) < quorum:
                if not self._demoted:
                    logger.warning(
                        "Leader lost quorum: only %d/%d leader-eligible peers reachable "
                        "(quorum=%d incl. self). Self-demoting; will not publish to discovery.",
                        reachable, len(peers), quorum,
                    )
                self._demoted = True
                return
            if self._demoted:
                logger.info(
                    "Leader regained quorum: %d/%d peers reachable. Resuming.",
                    reachable, len(peers),
                )
                self._demoted = False

        await self._discovery.publish(self._node_url, node_id=self._node_id)

    async def _trigger_election(self) -> None:
        new_leader = elect_leader()
        save_cluster_state()

        if not new_leader:
            logger.warning("Election yielded no leader — cluster may be empty")
            return

        logger.info(
            "Election result: %s @ %s",
            new_leader.get("node_id"), new_leader.get("url"),
        )

        if new_leader.get("node_id") != self._node_id:
            return  # another node won; let it broadcast

        logger.info("This node is now the leader — broadcasting and updating discovery")
        await asyncio.gather(
            self._broadcast_state(),
            self._discovery.publish(self._node_url, node_id=self._node_id),
        )

    async def _broadcast_state(self) -> None:
        st = load_cluster_state()
        status = get_cluster_status()
        payload = {
            "cluster_id": status.get("cluster_id"),
            "known_nodes": status.get("known_nodes", []),
            "current_leader": status.get("current_leader"),
        }
        peers = [
            n for n in st.known_nodes.values()
            if n.get("node_id") != self._node_id
            and n.get("status") != "dead"
            and n.get("url")
        ]
        if not peers:
            return
        async with httpx.AsyncClient(timeout=SYNC_TIMEOUT_S) as client:
            await asyncio.gather(*[
                _sync_peer(client, peer["url"], payload)
                for peer in peers
            ])


def _leader_eligible_peers(self_node_id: str) -> list[dict]:
    """
    Snapshot of leader-eligible peers (role gateway/both, has a url, not us).
    Caller decides whether to filter by status — for the quorum check we
    intentionally include peers that local cluster_state has marked dead,
    because a healthy peer on the other side of a partition will look dead
    here even when it's actually serving the cluster.
    """
    out: list[dict] = []
    for n in load_cluster_state().known_nodes.values():
        if n.get("node_id") == self_node_id:
            continue
        role = (n.get("role") or "").lower()
        if role not in ("gateway", "both"):
            continue
        if not n.get("url"):
            continue
        out.append(n)
    return out


async def _count_reachable(peers: list[dict]) -> int:
    """Count peers whose /health responds 200 inside HEALTH_TIMEOUT_S."""
    async with httpx.AsyncClient(timeout=HEALTH_TIMEOUT_S) as client:
        results = await asyncio.gather(
            *[_health_ok(client, p["url"]) for p in peers],
            return_exceptions=True,
        )
    return sum(1 for r in results if r is True)


async def _health_ok(client: httpx.AsyncClient, base_url: str) -> bool:
    if not base_url:
        return False
    try:
        r = await client.get(base_url.rstrip("/") + "/health")
        return r.status_code == 200
    except Exception:
        return False


async def _ping(base_url: str) -> bool:
    if not base_url:
        return False
    try:
        async with httpx.AsyncClient(timeout=HEALTH_TIMEOUT_S) as client:
            r = await client.get(base_url.rstrip("/") + "/health")
            return r.status_code == 200
    except Exception:
        return False


async def _sync_peer(client: httpx.AsyncClient, peer_url: str, payload: dict) -> None:
    try:
        await client.post(peer_url.rstrip("/") + "/cluster/sync", json=payload)
        logger.debug("Synced state to %s", peer_url)
    except Exception as exc:
        logger.debug("Could not sync to %s: %s", peer_url, exc)
