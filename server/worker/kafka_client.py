import json

from kafka import KafkaConsumer, KafkaProducer


def _parse_brokers(s: str) -> list[str]:
    """
    Parse a comma-separated bootstrap-servers string into a list. kafka-python
    treats a single string as one host, so passing "a:9092,b:9092" verbatim
    would fail to discover the second broker — we have to split it ourselves
    so the worker can fail over to peer brokers in the same KRaft quorum.
    """
    return [b.strip() for b in s.split(",") if b.strip()]


# RAM tier thresholds (GB). Must stay in sync with server/leader/kafka_client.py.
TIER_RAM_GB = {"high-ram": 16, "low-ram": 8, "general": 0}
TIER_ORDER = ["high-ram", "low-ram", "general"]


def _topics_for_ram(ram_gb: float) -> list[str]:
    """Every tier topic this worker meets by RAM. 'general' is always included."""
    return [f"tasks-{t}" for t in TIER_ORDER if ram_gb >= TIER_RAM_GB[t]]


class WorkerKafka:
    """
    Consumer + producer pair for a single worker node.

    All workers share the consumer group 'workers', so Kafka distributes
    each task to exactly one worker — no duplicate processing. A worker
    subscribes to every tasks-{tier} whose RAM threshold it meets, so a big
    worker can drain low-tier topics when no smaller worker is around.

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
        self._topics = _topics_for_ram(ram_gb)
        self._pending_broker: str | None = None
        self._consumer = self._make_consumer(bootstrap_servers)
        self._producer = self._make_producer(bootstrap_servers)

    # ── Internal factories ────────────────────────────────────────────────────

    def _make_consumer(self, bootstrap_servers: str) -> KafkaConsumer:
        return KafkaConsumer(
            *self._topics,
            bootstrap_servers=_parse_brokers(bootstrap_servers),
            group_id="workers",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            # earliest: if a partition gets reassigned after a worker death,
            # start from the last committed offset so the in-flight task is
            # redelivered instead of skipped.
            auto_offset_reset="earliest",
            # Manual commit only after publish_result returns. If the worker
            # dies mid-task, the offset stays uncommitted and Kafka rebalances
            # the partition to another worker, which then receives the task.
            enable_auto_commit=False,
            # Ollama generations can be slow. Without this, Kafka would assume
            # the worker is hung and trigger a rebalance, causing duplicate
            # processing of the in-flight task.
            max_poll_interval_ms=1_800_000,
            consumer_timeout_ms=1000,
        )

    def _make_producer(self, bootstrap_servers: str) -> KafkaProducer:
        return KafkaProducer(
            bootstrap_servers=_parse_brokers(bootstrap_servers),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            # acks="all" honors topic min.insync.replicas: with RF=3/ISR=2,
            # the producer fails fast if fewer than 2 brokers are alive
            # rather than acknowledging a write only the dying leader saw.
            acks="all",
        )

    # ── Public interface ──────────────────────────────────────────────────────

    @property
    def bootstrap_servers(self) -> str:
        return self._bootstrap

    @property
    def topics(self) -> list[str]:
        return list(self._topics)

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

    def commit(self):
        """
        Commit the offset of the most recently yielded message. Call only after
        publish_result has flushed successfully — committing earlier means a
        crash before the result is published would lose the task.
        """
        self._consumer.commit()

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
