import json

from kafka import KafkaConsumer, KafkaProducer


class WorkerKafka:
    """
    Consumer + producer pair for a single worker node.

    All workers share the consumer group 'workers', so Kafka distributes
    each task to exactly one worker — no duplicate processing.

    consumer_timeout_ms=1000 makes the consumer iterator exit after 1 s of
    silence, allowing the outer loop in worker.py to check the shutdown flag
    without blocking forever.
    """

    def __init__(self, bootstrap_servers: str, node_id: str):
        self._node_id = node_id
        self._consumer = KafkaConsumer(
            "tasks",
            bootstrap_servers=bootstrap_servers,
            group_id="workers",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
            consumer_timeout_ms=1000,
        )
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

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
