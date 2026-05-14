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

import logging

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable, NodeNotReadyError
from metrics import tasks_by_tier_total, tasks_dispatched_total, prompt_tokens_estimated

logger = logging.getLogger(__name__)


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

# Keyword sets for the auto-skill classifier. Order matters: first match wins.
# Used only when the /ask body did not pass an explicit `skill`. Explicit
# skill from the client always bypasses this map (see main.py /ask handler).
# Operator updates this alongside skills under server/worker/skills/ — adding
# a new SKILL.md without a keyword entry just means that skill is never
# auto-routed, but the React picker (or curl) can still target it.
_SKILL_KEYWORDS: dict[str, tuple[str, ...]] = {
    "coding": (
        "code", "function", "class ", "compile", "syntax", "bug",
        "python", "javascript", "typescript", "java ", "c++", "rust ",
        "def ", "import ", "refactor", "lint", "stack trace",
        "method", "variable", "regex", "exception", "snippet", "script",
    ),
    "summarization": (
        "summarize", "summarise", "summary", "tl;dr", "tldr",
        "key points", "bullet points", "brief overview", "condense",
        "shorten", "main ideas", "main points",
    ),
    "creative-writing": (
        "story", "fiction", "poem", "narrative", "screenplay",
        "character", "plot", "setting", "scene", "dialogue",
        "creative writing", "short story", "verse",
    ),
}


class NoEligibleWorker(Exception):
    """Raised by TaskProducer.publish when no tier has any online worker."""


def _classify_tier(prompt: str) -> str:
    estimated_tokens = len(prompt) // 4
    if estimated_tokens > _HIGH_RAM_TOKEN_FLOOR:
        return "high-ram"
    if estimated_tokens > _LOW_RAM_TOKEN_FLOOR:
        return "low-ram"
    return "general"


def _classify_skill(prompt: str) -> str | None:
    """Pick a skill name from _SKILL_KEYWORDS whose keywords appear in `prompt`.

    Returns None if no keywords match — caller should not constrain routing
    by skill in that case. First-match-wins iteration order, so order in
    _SKILL_KEYWORDS reflects priority for ambiguous prompts.

    Used as a soft default by TaskProducer.publish — explicit `skill` on the
    /ask body bypasses this entirely.
    """
    lowered = prompt.lower()
    for skill_name, keywords in _SKILL_KEYWORDS.items():
        if any(kw in lowered for kw in keywords):
            return skill_name
    return None


class TaskProducer:
    def __init__(self, bootstrap_servers: str, *, retries: int = 6):
        brokers = _parse_brokers(bootstrap_servers)
        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                self._producer = KafkaProducer(
                    bootstrap_servers=brokers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    acks="all",
                )
                return
            except (NoBrokersAvailable, NodeNotReadyError) as exc:
                last_exc = exc
                logger.info(
                    "Kafka producer: broker not ready yet (attempt %d/%d): %s",
                    attempt, retries, exc,
                )
                time.sleep(2)
        raise RuntimeError(
            f"TaskProducer: could not reach any broker in {brokers!r} after {retries} attempts"
        ) from last_exc

    async def publish(
        self,
        request_id: str,
        prompt: str,
        user_id: str,
        user_name: str,
        *,
        tier_override: str | None,
        skill: str | None,
        skill_strict: bool,
        registry,
    ) -> tuple[str, str | None]:
        """
        Publish to the chosen tier topic. Returns (chosen_tier, used_skill).

        Inputs:
          skill         — explicit skill from /ask body, or None to let the
                          heuristic auto-classify from the prompt.
          skill_strict  — True iff the caller passed `skill` explicitly. When
                          True and no eligible worker has that skill, raise
                          (HTTP 503). When False (auto-classified or None),
                          fall back to publishing without a skill filter.

        Algorithm:
          1. If `skill` is None, call _classify_skill(prompt) — may still be None.
          2. Resolve tier (override if valid, else token heuristic).
          3. Walk TIER_ORDER from the resolved tier. First tier with ≥1
             eligible worker (RAM-meeting, non-offline, advertising `skill`
             if set) wins.
          4. Soft fallback: if step 3 came up empty AND skill is set AND
             not strict, drop the skill filter and walk again.
          5. If still no eligible worker, raise NoEligibleWorker.

          The `used_skill` returned reflects what was actually attached to
          the published message — may differ from the requested `skill` if
          the soft fallback dropped it.
        """
        if skill is None:
            skill = _classify_skill(prompt)
        used_skill = skill

        requested_tier = tier_override if tier_override in TIER_RAM_GB else _classify_tier(prompt)
        start_idx = TIER_ORDER.index(requested_tier)

        async def _walk(filter_skill: str | None) -> str | None:
            for tier in TIER_ORDER[start_idx:]:
                if await registry.eligible(TIER_RAM_GB[tier], skill=filter_skill):
                    return tier
            return None

        chosen_tier = await _walk(used_skill)

        # Soft fallback: auto-classified (or otherwise non-strict) skill found
        # nothing — drop the skill filter and try again. A strict (explicit)
        # skill mismatch falls through to the raise below.
        if chosen_tier is None and used_skill and not skill_strict:
            used_skill = None
            chosen_tier = await _walk(None)

        if chosen_tier is None:
            if skill_strict and skill:
                raise NoEligibleWorker(
                    f"No worker advertising skill {skill!r} is online to handle this task."
                )
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
            "skill": used_skill,
        })
        self._producer.flush()
        tasks_dispatched_total.labels(topic=topic).inc()
        tasks_by_tier_total.labels(tier=chosen_tier).inc()
        prompt_tokens_estimated.observe(len(prompt) // 4)
        return chosen_tier, used_skill


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
