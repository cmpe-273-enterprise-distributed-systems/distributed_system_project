import json

from kafka import KafkaConsumer, KafkaProducer

# TODO(mock): This RAM threshold is a placeholder. A proper implementation
# should derive topic eligibility from the worker's registered skill set and
# actual available memory at task-acceptance time, not a fixed boot-time value.
HIGH_RAM_THRESHOLD_GB = 12

ALL_TASK_TOPICS = ("tasks-high-ram", "tasks-low-ram", "tasks-general")
LOW_RAM_TOPICS = ("tasks-low-ram", "tasks-general")


class WorkerKafka:
    """
    Consumer + producer pair for a single worker node.

    All workers share the consumer group 'workers', so Kafka distributes
    each task to exactly one worker — no duplicate processing.

    TODO(mock): Topic subscription is currently determined by a fixed RAM
    threshold at startup. A proper implementation would subscribe based on
    registered skills and dynamically adjust if RAM availability changes.

    consumer_timeout_ms=1000 makes the consumer iterator exit after 1 s of
    silence, allowing the outer loop in worker.py to check the shutdown flag
    and any pending broker reconnect without blocking forever.

    Broker reconnection is intentionally single-threaded: the heartbeat thread
    calls request_reconnect() to signal a broker change, and the main poll loop
    calls apply_reconnect_if_needed() between iterations to perform the actual
    swap. Python's GIL makes the str assignment in request_reconnect atomic.
    """

    def __init__(self, bootstrap_servers: str, node_id: str, ram_gb: float):
        self._node_id = node_id
        self._ram_gb = ram_gb
        self._bootstrap = bootstrap_servers
        # TODO(mock): topic selection by RAM threshold — see module comment above
        self._topics = ALL_TASK_TOPICS if ram_gb >= HIGH_RAM_THRESHOLD_GB else LOW_RAM_TOPICS
        self._pending_broker: str | None = None
        self._consumer = self._make_consumer(bootstrap_servers)
        self._producer = self._make_producer(bootstrap_servers)

    # ── Internal factories ────────────────────────────────────────────────────

    def _make_consumer(self, bootstrap_servers: str) -> KafkaConsumer:
        return KafkaConsumer(
            *self._topics,
            bootstrap_servers=bootstrap_servers,
            group_id="workers",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
            consumer_timeout_ms=1000,
        )

    def _make_producer(self, bootstrap_servers: str) -> KafkaProducer:
        return KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    # ── Public interface ──────────────────────────────────────────────────────

    @property
    def bootstrap_servers(self) -> str:
        return self._bootstrap

    def request_reconnect(self, new_bootstrap_servers: str):
        """Thread-safe: signal the main thread to reconnect to a new broker."""
        self._pending_broker = new_bootstrap_servers

    def apply_reconnect_if_needed(self):
        """
        Call from the main thread only (between poll iterations).
        Swaps the consumer and producer if a new broker was signaled.
        """
        broker = self._pending_broker
        if broker and broker != self._bootstrap:
            self._pending_broker = None
            print(f"[WorkerKafka] Reconnecting to new broker: {broker}")
            self._consumer.close()
            self._producer.close()
            self._bootstrap = broker
            self._consumer = self._make_consumer(broker)
            self._producer = self._make_producer(broker)

    def poll(self):
        """Yield task dicts. Exits after ~1 s of no messages (consumer_timeout_ms)."""
        for msg in self._consumer:
            yield msg.value

    def publish_result(
        self,
        request_id: str,
        response: str,
        duration_ms: int,
        error: str | None = None,
    ):
        payload = {
            "request_id": request_id,
            "worker_id": self._node_id,
            "response": response,
            "duration_ms": duration_ms,
        }
        if error:
            payload["error"] = error
        self._producer.send("completed-tasks", payload)
        self._producer.flush()

    def close(self):
        self._consumer.close()
        self._producer.close()
