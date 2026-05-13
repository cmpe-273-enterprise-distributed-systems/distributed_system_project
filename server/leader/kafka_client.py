"""
Kafka integration for the leader node.

TaskProducer  — publishes tasks to a RAM tier topic chosen by a token-count
                heuristic (or an explicit `tier` override on /ask). Walks
                TIER_ORDER downward when the requested tier has no eligible
                workers; raises NoEligibleWorker if no tier has any.
                Topics: tasks-high-ram, tasks-low-ram, tasks-general.
ResultConsumer — runs a KafkaConsumer in a background thread, resolves
                 asyncio.Events when completed-task results arrive.

Thread-safety note:
  _pending and _results are accessed from both the FastAPI async event loop
  (register / pop_result) and the consumer thread (_consume). Python's GIL
  makes dict get/pop/set atomic, so no explicit lock is needed here.
"""

import asyncio
import json
import threading
import time
from typing import Any

from kafka import KafkaConsumer, KafkaProducer


def _parse_brokers(s: str) -> list[str]:
    """
    Parse a comma-separated bootstrap-servers string into a list. kafka-python
    treats a single string as one host, so passing "a:9092,b:9092" verbatim
    would fail to discover the second broker — we have to split it ourselves
    so the client can fail over to peer brokers in the same KRaft quorum.
    """
    return [b.strip() for b in s.split(",") if b.strip()]


# RAM tier thresholds (GB). Must stay in sync with server/worker/kafka_client.py.
TIER_RAM_GB = {"high-ram": 16, "low-ram": 8, "general": 0}
# Ordered most-capable → least-capable. Used as the downgrade walk order.
TIER_ORDER = ["high-ram", "low-ram", "general"]

# Heuristic tier classifier — len(prompt) // 4 ≈ token count.
_HIGH_RAM_TOKEN_FLOOR = 2000
_LOW_RAM_TOKEN_FLOOR = 500


class NoEligibleWorker(Exception):
    """Raised by TaskProducer.publish when no tier has any online worker."""


def _classify_tier(prompt: str) -> str:
    estimated_tokens = len(prompt) // 4
    if estimated_tokens > _HIGH_RAM_TOKEN_FLOOR:
        return "high-ram"
    if estimated_tokens > _LOW_RAM_TOKEN_FLOOR:
        return "low-ram"
    return "general"


class TaskProducer:
    def __init__(self, bootstrap_servers: str):
        self._producer = KafkaProducer(
            bootstrap_servers=_parse_brokers(bootstrap_servers),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            # acks="all" is required for the topic-level min.insync.replicas
            # setting to take effect. Without it, the producer is satisfied as
            # soon as the partition leader broker acks, and a single broker
            # death (case B) could lose acknowledged writes.
            acks="all",
        )

    async def publish(
        self,
        request_id: str,
        prompt: str,
        user_id: str,
        user_name: str,
        *,
        tier_override: str | None,
        registry,
    ) -> str:
        """
        Publish to the chosen tier topic. Returns the chosen tier.

        1. Resolve tier (override if valid, else token heuristic).
        2. Walk TIER_ORDER starting at the resolved tier. First tier with at
           least one eligible (RAM-meeting, non-offline) worker wins.
        3. If no tier has any eligible worker, raise NoEligibleWorker.
        """
        requested_tier = tier_override if tier_override in TIER_RAM_GB else _classify_tier(prompt)
        start_idx = TIER_ORDER.index(requested_tier)

        chosen_tier: str | None = None
        for tier in TIER_ORDER[start_idx:]:
            if await registry.eligible(TIER_RAM_GB[tier]):
                chosen_tier = tier
                break

        if chosen_tier is None:
            raise NoEligibleWorker("No worker is online to handle this task.")

        topic = f"tasks-{chosen_tier}"
        self._producer.send(topic, {
            "request_id": request_id,
            "prompt": prompt,
            "user_id": user_id,
            "user_name": user_name,
            "timestamp": int(time.time() * 1000),
            "topic": topic,
            "tier": chosen_tier,
            "requested_tier": requested_tier,
        })
        self._producer.flush()
        return chosen_tier


class ResultConsumer:
    """
    Listens to the 'completed-tasks' topic on a background thread.
    When a result arrives for a tracked request_id, it stores the payload
    and signals the corresponding asyncio.Event so the waiting /ask handler
    can return the response to the client.

    Each leader process uses a UNIQUE consumer group (`leader-{node_id}`)
    rather than sharing `group_id="leader"`. Reason: in the multi-laptop
    demo every leader-eligible process runs ResultConsumer (because the
    election machinery lives in the FastAPI process), but the waiting
    asyncio.Event for a given request_id only exists on the elected
    leader. With a shared group, Kafka would distribute partitions across
    all leader processes — most results would land on a non-elected
    leader, find no matching _pending entry, and silently drop. With
    per-instance groups, every leader receives every result; only the
    elected leader's pop succeeds; the others harmlessly no-op.

    The cost is N-fold fanout on completed-tasks (one delivery per leader
    process). At demo scale this is negligible.
    """

    def __init__(self, bootstrap_servers: str, node_id: str):
        self._bootstrap = bootstrap_servers
        self._group_id = f"leader-{node_id}"
        self._pending: dict[str, asyncio.Event] = {}
        self._results: dict[str, Any] = {}
        self._loop: asyncio.AbstractEventLoop | None = None

    def start(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop
        t = threading.Thread(target=self._consume, daemon=True, name="kafka-result-consumer")
        t.start()

    def register(self, request_id: str) -> asyncio.Event:
        """Call from the async context before publishing the task."""
        event = asyncio.Event()
        self._pending[request_id] = event
        return event

    def pop_result(self, request_id: str) -> dict | None:
        """Call after the event fires to retrieve and remove the payload."""
        return self._results.pop(request_id, None)

    def cancel(self, request_id: str):
        """Remove a timed-out request so the thread doesn't touch a stale event."""
        self._pending.pop(request_id, None)
        self._results.pop(request_id, None)

    def _consume(self):
        consumer = KafkaConsumer(
            "completed-tasks",
            bootstrap_servers=_parse_brokers(self._bootstrap),
            group_id=self._group_id,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
        )
        for msg in consumer:
            data: dict = msg.value
            rid = data.get("request_id")
            # Atomic pop — if the request timed out, _pending[rid] won't exist
            event = self._pending.pop(rid, None)
            if event:
                self._results[rid] = data
                self._loop.call_soon_threadsafe(event.set)
