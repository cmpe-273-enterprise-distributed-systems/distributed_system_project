"""
Topic provisioning for the cluster.

Auto-created Kafka topics default to replication.factor=1 and
min.insync.replicas=1. With RF=1, a partition lives on a single broker —
when that broker dies (Scenario 2B case B: full machine kill), the
partition becomes unreadable until the broker comes back, and case-B
failover hangs.

ensure_topics() creates the four cluster topics with config that scales
to the actual broker count:
  - 1 broker  -> RF=1, ISR=1 (local single-broker dev; works with the
                 docker-compose.yml at the repo root).
  - >=3 brokers -> RF=3, ISR=2 (multi-laptop demo; lose any one broker
                 and writes still succeed, reads still resolve).

Idempotent: TopicAlreadyExistsError is swallowed. If a topic exists
with a different RF than we'd create, log a WARN — fixing it requires
partition reassignment, which the operator should do manually.

Called from the leader's lifespan on every boot, AND exposed via
server/scripts/ensure_kafka_topics.py for one-off ops.
"""

from __future__ import annotations

import logging
import time
from typing import Iterable

from kafka.admin import ConfigResource, ConfigResourceType, KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, NoBrokersAvailable, TopicAlreadyExistsError

logger = logging.getLogger(__name__)

CLUSTER_TOPICS: tuple[str, ...] = (
    "tasks-high-ram",
    "tasks-low-ram",
    "tasks-general",
    "completed-tasks",
)

DEFAULT_PARTITIONS = 3


def _parse_brokers(value: str | Iterable[str]) -> list[str]:
    if isinstance(value, str):
        return [b.strip() for b in value.split(",") if b.strip()]
    return [b.strip() for b in value if b and b.strip()]


def _connect_admin(brokers: list[str], retries: int) -> KafkaAdminClient:
    """
    KafkaAdminClient is happy to fail fast if no broker is reachable yet —
    leader startup races the broker process, so retry with backoff.
    """
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return KafkaAdminClient(
                bootstrap_servers=brokers,
                client_id="leader-topic-provisioner",
            )
        except NoBrokersAvailable as exc:
            last_exc = exc
            logger.info(
                "Kafka admin: no brokers reachable yet (attempt %d/%d): %s",
                attempt, retries, exc,
            )
            time.sleep(2)
    raise RuntimeError(
        f"ensure_topics: could not reach any broker in {brokers!r} after {retries} attempts"
    ) from last_exc


def ensure_topics(
    brokers: str | Iterable[str],
    *,
    retries: int = 6,
    partitions: int = DEFAULT_PARTITIONS,
) -> dict[str, str]:
    """
    Create CLUSTER_TOPICS if absent. Returns {topic_name: status}
    where status is one of:
      'created'       — topic did not exist; created with target RF/ISR
      'exists'        — topic exists with matching RF
      'rf_mismatch'   — topic exists but RF differs from target (logged WARN;
                        operator must fix via partition reassignment)
    Raises RuntimeError if no broker is reachable after `retries` attempts.
    """
    bootstrap = _parse_brokers(brokers)
    if not bootstrap:
        raise ValueError("ensure_topics: empty brokers list")

    admin = _connect_admin(bootstrap, retries)
    try:
        cluster_meta = admin.describe_cluster()
        broker_count = len(cluster_meta.get("brokers", []))
        if broker_count == 0:
            broker_count = len(bootstrap)
        target_rf = min(3, max(1, broker_count))
        target_isr = max(1, target_rf - 1)
        logger.info(
            "Kafka admin: cluster has %d broker(s); target RF=%d, min.insync.replicas=%d",
            broker_count, target_rf, target_isr,
        )

        result: dict[str, str] = {}
        new_topics = [
            NewTopic(
                name=name,
                num_partitions=partitions,
                replication_factor=target_rf,
                topic_configs={"min.insync.replicas": str(target_isr)},
            )
            for name in CLUSTER_TOPICS
        ]
        try:
            admin.create_topics(new_topics, validate_only=False)
            for name in CLUSTER_TOPICS:
                result[name] = "created"
        except TopicAlreadyExistsError:
            # At least one already exists. Fall through to per-topic resolution.
            pass
        except KafkaError as exc:
            # Some topics may have been created before the error fired; per-topic
            # resolution below will sort out the actual state.
            logger.warning("Kafka admin: bulk create_topics returned %s", exc)

        # For any topic not marked 'created' above, resolve its actual state
        # against target_rf so we can report 'exists' vs 'rf_mismatch'.
        unresolved = [t for t in CLUSTER_TOPICS if t not in result]
        if unresolved:
            existing_meta = _describe_topic_rfs(admin, unresolved)
            for name in unresolved:
                actual_rf = existing_meta.get(name)
                if actual_rf is None:
                    # Couldn't read it — treat as a soft failure but don't crash startup.
                    result[name] = "rf_unknown"
                    logger.warning(
                        "Kafka admin: topic %r exists but RF could not be read; assuming OK",
                        name,
                    )
                elif actual_rf == target_rf:
                    result[name] = "exists"
                else:
                    result[name] = "rf_mismatch"
                    logger.warning(
                        "Kafka admin: topic %r already exists with RF=%d (expected %d). "
                        "Fix requires partition reassignment — see kafka-reassign-partitions. "
                        "Case-B (full machine kill) failover may hang on this topic until corrected.",
                        name, actual_rf, target_rf,
                    )
        return result
    finally:
        try:
            admin.close()
        except Exception:
            pass


def _describe_topic_rfs(admin: KafkaAdminClient, topic_names: list[str]) -> dict[str, int]:
    """
    Return {topic_name: replication_factor} by inspecting the cluster's
    topic metadata. Skips topics that aren't found.
    """
    out: dict[str, int] = {}
    try:
        # describe_topics returns a list of dicts with 'topic' + 'partitions',
        # where each partition has a 'replicas' list.
        meta = admin.describe_topics(topic_names)
    except KafkaError as exc:
        logger.warning("Kafka admin: describe_topics failed: %s", exc)
        return out

    for entry in meta or []:
        name = entry.get("topic") if isinstance(entry, dict) else None
        if not name:
            continue
        partitions = entry.get("partitions") or []
        if not partitions:
            continue
        replicas = partitions[0].get("replicas") or []
        out[name] = len(replicas)
    return out
