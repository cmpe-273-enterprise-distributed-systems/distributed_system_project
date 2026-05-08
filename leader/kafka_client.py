"""
Kafka integration for the leader node.

TaskProducer  — publishes tasks to the appropriate task topic based on prompt
                analysis. Topics: tasks-high-ram, tasks-low-ram, tasks-general.
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

TOPIC_HIGH_RAM = "tasks-high-ram"
TOPIC_LOW_RAM = "tasks-low-ram"
TOPIC_GENERAL = "tasks-general"

# TODO(mock): These keyword sets are a placeholder for real dispatch logic.
# A proper implementation should estimate token count, inspect required model
# size/context window, or use request metadata set by the client. The current
# keyword approach over-routes to tasks-high-ram (e.g. any prompt containing
# "write" or "code") and has no fallback when no high-RAM worker is available.
_HIGH_RAM_KEYWORDS = {
    "analyze", "analyse", "summarize", "summarise", "generate", "write",
    "code", "program", "refactor", "implement", "essay", "compare",
    "explain in detail", "step by step", "in depth",
}

# TODO(mock): Likewise, these low-RAM keywords are illustrative only.
_LOW_RAM_KEYWORDS = {
    "what is", "what are", "define", "who is", "when was", "how many",
    "yes or no", "is it", "true or false", "list",
}


def _select_topic(prompt: str) -> str:
    # TODO(mock): Replace with real routing logic — token-count estimate, model
    # metadata, or an explicit tier field on the /ask request body.
    lowered = prompt.lower()
    if any(kw in lowered for kw in _HIGH_RAM_KEYWORDS):
        return TOPIC_HIGH_RAM
    if any(kw in lowered for kw in _LOW_RAM_KEYWORDS):
        return TOPIC_LOW_RAM
    return TOPIC_GENERAL


class TaskProducer:
    def __init__(self, bootstrap_servers: str):
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def publish(self, request_id: str, prompt: str, user_id: str, user_name: str) -> str:
        """Publishes the task to the appropriate topic. Returns the chosen topic name."""
        topic = _select_topic(prompt)
        self._producer.send(topic, {
            "request_id": request_id,
            "prompt": prompt,
            "user_id": user_id,
            "user_name": user_name,
            "timestamp": int(time.time() * 1000),
            "topic": topic,
        })
        self._producer.flush()
        return topic


class ResultConsumer:
    """
    Listens to the 'completed-tasks' topic on a background thread.
    When a result arrives for a tracked request_id, it stores the payload
    and signals the corresponding asyncio.Event so the waiting /ask handler
    can return the response to the client.
    """

    def __init__(self, bootstrap_servers: str):
        self._bootstrap = bootstrap_servers
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
            bootstrap_servers=self._bootstrap,
            group_id="leader",
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
