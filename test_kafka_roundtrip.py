"""
Integration test: validates the full Kafka round-trip without a real worker or Ollama.

Prerequisites:
  1. docker-compose up -d           (starts Kafka, creates topics)
  2. cd leader && uvicorn main:app  (starts the leader on port 8000)

Then run:
  python test_kafka_roundtrip.py
"""

import json
import sys
import threading
import time
import uuid

import requests
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKER = "localhost:9092"
LEADER_URL = "http://localhost:8000"
MOCK_WORKER_ID = "mock-worker-test"
TIMEOUT = 20  # seconds

TASK_TOPICS = ("tasks-high-ram", "tasks-low-ram", "tasks-general")


def wait_for_leader(retries=5):
    for i in range(retries):
        try:
            r = requests.get(f"{LEADER_URL}/health", timeout=3)
            if r.status_code == 200:
                return
        except requests.ConnectionError:
            pass
        print(f"  Waiting for leader... ({i+1}/{retries})")
        time.sleep(2)
    print("ERROR: Leader not reachable at", LEADER_URL)
    sys.exit(1)


def run_mock_worker(stop_event, received_tasks, ready_event):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    consumer = KafkaConsumer(
        *TASK_TOPICS,
        bootstrap_servers=KAFKA_BROKER,
        group_id=f"mock-worker-{uuid.uuid4().hex[:6]}",
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    # Block until Kafka has assigned partitions to this consumer.
    # Without this, messages published before assignment are silently missed
    # because auto_offset_reset="latest" only takes effect after assignment.
    while not consumer.assignment():
        consumer.poll(timeout_ms=200)
    ready_event.set()

    try:
        while not stop_event.is_set():
            records = consumer.poll(timeout_ms=200)
            for tp, msgs in records.items():
                for msg in msgs:
                    if stop_event.is_set():
                        return
                    task = msg.value
                    received_tasks.append({**task, "_received_on": tp.topic})
                    result = {
                        "request_id": task["request_id"],
                        "worker_id": MOCK_WORKER_ID,
                        "response": f"[mock] Echo: {task['prompt']}",
                        "duration_ms": 42,
                    }
                    producer.send("completed-tasks", value=result)
                    producer.flush()
    finally:
        consumer.close()
        producer.close()


def test_round_trip(prompt: str, expected_topic: str):
    print(f"\n=== Round-trip: {expected_topic!r} ===")
    print(f"    Prompt: {prompt!r}\n")

    received_tasks = []
    stop_event = threading.Event()
    ready_event = threading.Event()

    worker_thread = threading.Thread(
        target=run_mock_worker,
        args=(stop_event, received_tasks, ready_event),
        daemon=True,
    )
    worker_thread.start()

    if not ready_event.wait(timeout=15):
        print("FAIL: Mock worker consumer never received partition assignments.")
        sys.exit(1)

    try:
        resp = requests.post(
            f"{LEADER_URL}/ask",
            json={"prompt": prompt, "user_id": 1, "user_name": "Test User"},
            timeout=TIMEOUT,
        )
    except requests.Timeout:
        print(f"FAIL [{expected_topic}]: Leader timed out — mock worker may not have consumed the task.")
        sys.exit(1)
    finally:
        stop_event.set()

    # Wait for the worker thread to exit before the next test starts.
    # Without this, a slow-exiting worker would still be subscribed to all task
    # topics and could steal the next test's message, then crash on a closed producer.
    worker_thread.join(timeout=3)

    print(f"   HTTP status: {resp.status_code}")

    if resp.status_code != 200:
        print(f"FAIL [{expected_topic}]: Expected 200, got {resp.status_code}")
        print(f"     Response body: {resp.text}")
        sys.exit(1)

    data = resp.json()
    print(f"   Response body: {json.dumps(data, indent=2)}\n")

    assert "response" in data, "Missing 'response' field in leader reply"
    assert "[mock] Echo:" in data["response"], "Response doesn't match what mock worker sent"

    assert len(received_tasks) >= 1, f"Mock worker never received a task from {TASK_TOPICS}"
    task = received_tasks[0]
    assert task["prompt"] == prompt, f"Prompt mismatch: {task['prompt']!r}"

    actual_topic = task.get("topic") or task.get("_received_on")
    if actual_topic != expected_topic:
        print(f"WARN: Expected topic {expected_topic!r}, task landed on {actual_topic!r}")
    else:
        print(f"   Correct topic: {actual_topic!r}")

    print(f"PASS [{expected_topic}]")


def test_topic_connectivity():
    """Quick check: can we produce and consume on all topics?"""
    print("=== Topic connectivity check ===\n")

    all_topics = (*TASK_TOPICS, "completed-tasks")
    for topic in all_topics:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            producer.send(topic, value={"_test": True})
            producer.flush()
            producer.close()
            print(f"   OK: can produce to '{topic}'")
        except Exception as e:
            print(f"   FAIL: cannot produce to '{topic}': {e}")
            sys.exit(1)

    print()


if __name__ == "__main__":
    print("1. Checking leader health...")
    wait_for_leader()
    print("   Leader is up.\n")

    test_topic_connectivity()

    # Each prompt is chosen to trigger a specific dispatcher route
    test_round_trip(
        prompt="Write a Python program that implements a binary search tree.",
        expected_topic="tasks-high-ram",
    )
    test_round_trip(
        prompt="What is the capital of France?",
        expected_topic="tasks-low-ram",
    )
    test_round_trip(
        prompt="Tell me something interesting.",
        expected_topic="tasks-general",
    )

    print("\nAll round-trip tests passed.")
