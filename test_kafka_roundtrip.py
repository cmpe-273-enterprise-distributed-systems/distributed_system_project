"""
Integration test: validates the full Kafka round-trip without a real worker or Ollama.

Prerequisites:
  1. docker-compose up -d           (starts Kafka + Zookeeper, creates topics)
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


def run_mock_worker(producer, stop_event, received_tasks):
    consumer = KafkaConsumer(
        "tasks",
        bootstrap_servers=KAFKA_BROKER,
        group_id=f"mock-worker-{uuid.uuid4().hex[:6]}",
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    try:
        while not stop_event.is_set():
            for msg in consumer:
                task = msg.value
                received_tasks.append(task)
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


def test_round_trip():
    print("=== Kafka round-trip test ===\n")

    print("1. Checking leader health...")
    wait_for_leader()
    print("   Leader is up.\n")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    received_tasks = []
    stop_event = threading.Event()

    print("2. Starting mock worker (consuming 'tasks', publishing to 'completed-tasks')...")
    worker_thread = threading.Thread(
        target=run_mock_worker, args=(producer, stop_event, received_tasks), daemon=True
    )
    worker_thread.start()
    time.sleep(2)  # give consumer time to join the group

    print("3. Sending POST /ask to leader...\n")
    test_prompt = "What is 2 + 2?"
    try:
        resp = requests.post(
            f"{LEADER_URL}/ask",
            json={"prompt": test_prompt, "user_id": "test-user-001", "user_name": "Test User"},
            timeout=TIMEOUT,
        )
    except requests.Timeout:
        print("FAIL: Leader timed out — mock worker may not have consumed the task.")
        sys.exit(1)
    finally:
        stop_event.set()

    print(f"   HTTP status: {resp.status_code}")

    if resp.status_code != 200:
        print(f"FAIL: Expected 200, got {resp.status_code}")
        print(f"      Response body: {resp.text}")
        sys.exit(1)

    data = resp.json()
    print(f"   Response body: {json.dumps(data, indent=2)}\n")

    # Assertions
    assert "response" in data, "Missing 'response' field in leader reply"
    assert "[mock] Echo:" in data["response"], "Response doesn't match what mock worker sent"
    assert data.get("worker") == MOCK_WORKER_ID or "worker_id" in str(data), \
        "Worker ID not propagated correctly"

    print("4. Verifying task received by mock worker...")
    assert len(received_tasks) >= 1, "Mock worker never received a task from 'tasks' topic"
    task = received_tasks[0]
    assert task["prompt"] == test_prompt, f"Prompt mismatch: {task['prompt']!r}"
    print(f"   Task received: request_id={task['request_id']}, prompt={task['prompt']!r}\n")

    print("PASS: Full Kafka round-trip working correctly.")
    producer.close()


def test_topic_connectivity():
    """Quick check: can we produce and consume on both topics?"""
    print("=== Topic connectivity check ===\n")

    for topic in ("tasks", "completed-tasks"):
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
    test_topic_connectivity()
    test_round_trip()
