"""
Integration test for the multi-broker Kafka cluster topology.

Brings up kafka/docker-compose.test.yml, validates the cluster forms with
3 brokers and replication-factor 3 topics, kills one broker mid-flight,
produces + consumes through the survivors, then restarts the killed broker
and verifies ISR recovery. Tears the cluster down at the end.

Prerequisites:
  - Docker + docker compose
  - Python env with kafka-python  (the repo-root .venv has it)

Run from anywhere:
  /path/to/repo/.venv/bin/python kafka/test_multi_broker.py
"""

import json
import os
import re
import subprocess
import sys
import time
import uuid

from kafka import KafkaConsumer, KafkaProducer

# Resolve the compose file next to this script so the test runs regardless of cwd.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
COMPOSE_FILE = os.path.join(SCRIPT_DIR, "docker-compose.test.yml")
BROKERS = "localhost:9092,localhost:9192,localhost:9292"
TOPIC = "tasks-high-ram"
ALL_TOPICS = ("tasks-high-ram", "tasks-low-ram", "tasks-general", "completed-tasks")
INIT_CONTAINER = "kafka-init-test"


def _run(cmd, check=True):
    print(f"$ {' '.join(cmd)}")
    return subprocess.run(cmd, check=check, capture_output=True, text=True)


def _compose(*args, check=True):
    return _run(["docker", "compose", "-f", COMPOSE_FILE, *args], check=check)


def wait_for_kafka_init(timeout=180):
    """Poll the kafka-init container until it exits with code 0."""
    print(f"\nWaiting up to {timeout}s for kafka-init to create topics...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        res = subprocess.run(
            ["docker", "inspect", INIT_CONTAINER,
             "--format", "{{.State.Status}}:{{.State.ExitCode}}"],
            capture_output=True, text=True,
        )
        out = (res.stdout or "").strip()
        if out == "exited:0":
            print("kafka-init exited cleanly.")
            return
        if out.startswith("exited:") and out != "exited:0":
            sys.exit(f"FAIL: kafka-init exited non-zero: {out}")
        time.sleep(2)
    sys.exit("FAIL: kafka-init did not finish within timeout.")


def describe_topic(topic):
    """Parse Replicas + Isr per partition from kafka-topics --describe."""
    res = subprocess.run(
        ["docker", "exec", "kafka-1", "kafka-topics",
         "--bootstrap-server", "localhost:9092",
         "--describe", "--topic", topic],
        capture_output=True, text=True, check=True,
    )
    partitions = []
    for line in res.stdout.splitlines():
        m = re.search(
            r"Partition:\s*(\d+)\s+Leader:\s*(\d+)\s+Replicas:\s*([\d,]+)\s+Isr:\s*([\d,]+)",
            line,
        )
        if m:
            partitions.append({
                "partition": int(m.group(1)),
                "leader": int(m.group(2)),
                "replicas": [int(x) for x in m.group(3).split(",")],
                "isr": [int(x) for x in m.group(4).split(",")],
            })
    return partitions


def assert_topic_rf3(topic):
    parts = describe_topic(topic)
    assert parts, f"FAIL: topic {topic!r} has no partitions"
    for p in parts:
        assert set(p["replicas"]) == {1, 2, 3}, (
            f"FAIL: {topic} partition {p['partition']} replicas {p['replicas']} != [1,2,3]"
        )
        assert set(p["isr"]) == {1, 2, 3}, (
            f"FAIL: {topic} partition {p['partition']} ISR {p['isr']} != [1,2,3]"
        )
    print(f"OK: {topic} — {len(parts)} partitions, RF=3, full ISR")


def produce_and_consume(label):
    """Round-trip a single uniquely-tagged message; assert it comes back."""
    test_id = str(uuid.uuid4())
    payload = {"_test_id": test_id, "label": label}
    print(f"\n--- {label}: round-tripping test_id {test_id} ---")

    producer = KafkaProducer(
        bootstrap_servers=BROKERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",               # requires min.insync.replicas=2 acks
        retries=3,
        request_timeout_ms=15000,
    )
    producer.send(TOPIC, value=payload).get(timeout=15)
    producer.close()
    print("  Producer: send acknowledged.")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKERS,
        group_id=f"multi-broker-test-{uuid.uuid4().hex[:6]}",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=20000,
    )
    seen = False
    for msg in consumer:
        if msg.value.get("_test_id") == test_id:
            seen = True
            print(f"  Consumer: matched on partition {msg.partition} offset {msg.offset}")
            break
    consumer.close()
    assert seen, f"FAIL ({label}): test message {test_id} not consumed within 20s"


def main():
    print("=" * 60)
    print("Multi-broker Kafka cluster integration test")
    print("=" * 60)

    try:
        print("\n[1/6] Bringing up test cluster (clean slate)...")
        _compose("down", "-v", "--remove-orphans", check=False)
        _compose("up", "-d")
        wait_for_kafka_init()

        print("\n[2/6] Verifying every topic has RF=3 with full ISR...")
        for topic in ALL_TOPICS:
            assert_topic_rf3(topic)

        print("\n[3/6] Producing + consuming through full cluster...")
        produce_and_consume("full cluster")

        print("\n[4/6] Killing kafka-2 and waiting for the controller to shrink ISR...")
        _run(["docker", "kill", "kafka-2"])
        # KRaft fences a dead broker after broker.session.timeout.ms (default 9s);
        # ISR can lag a few seconds beyond that as replica.lag.time.max.ms ticks.
        deadline = time.time() + 45
        affected = []
        parts = []
        while time.time() < deadline:
            parts = describe_topic(TOPIC)
            affected = [p for p in parts if 2 not in p["isr"]]
            if affected:
                break
            time.sleep(2)
        assert affected, "FAIL: ISR still contained broker 2 after 45s"
        print(f"OK: {len(affected)}/{len(parts)} partitions show ISR without broker 2")

        print("\n[5/6] Producing + consuming with kafka-2 dead...")
        produce_and_consume("broker-2-down")

        print("\n[6/6] Restarting kafka-2 and waiting for ISR recovery...")
        _run(["docker", "start", "kafka-2"])
        deadline = time.time() + 60
        recovered = False
        while time.time() < deadline:
            parts = describe_topic(TOPIC)
            if all(set(p["isr"]) == {1, 2, 3} for p in parts):
                recovered = True
                break
            time.sleep(2)
        assert recovered, "FAIL: ISR did not recover to {1,2,3} within 60s"
        print("OK: ISR recovered to [1,2,3] for all partitions")

        print("\n" + "=" * 60)
        print("ALL TESTS PASSED")
        print("=" * 60)

    except (AssertionError, subprocess.CalledProcessError) as exc:
        print(f"\n*** FAILED *** {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        print("\nTearing down test cluster...")
        _compose("down", "-v", check=False)


if __name__ == "__main__":
    main()
