# Production Testing Guide

End-to-end test plan for the full production stack: single Kafka broker on a designated laptop, multiple worker nodes, and Astra DataStax as the shared database. All tests assume the cluster is fully bootstrapped per the [Multi-laptop Production Demo](README.md#multi-laptop-production-demo) in the README.

---

## Prerequisites

Before running any test:

- Three laptops connected via Tailscale (call them **A**, **B**, **C**).
- One designated laptop (**Laptop K**, can be A or a separate machine) running the Kafka broker (`docker compose -f server/kafka/docker-compose.yml up -d`).
- Each laptop running a leader process (`python server/leader/main.py`).
- At least two laptops running a worker process (`python server/worker/worker.py`).
- Astra DataStax provisioned with the schema from `client/db/002_tables.cql` + `003_seed_data.cql`.
- Upstash Redis configured and referenced in all `.env` files.

Set a shell variable for convenience throughout these tests:
```bash
LEADER=http://<A_IP>:8000    # current leader's Tailscale IP
```

---

## 1. Pre-test Cluster Verification

Run these before any scenario test to confirm the cluster is healthy.

```bash
# All 3 leaders + all workers listed; current_leader is Laptop A
curl -s $LEADER/cluster/status | python3 -m json.tool

# Discovery resolves to Laptop A
curl -s $LEADER/discovery/leader

# Skills available in the cluster
curl -s $LEADER/cluster/skills

# Kafka topic list (run on Laptop K — the designated broker host)
docker exec kafka-broker kafka-topics --bootstrap-server localhost:29092 --list
```

**Expected:**
- `cluster/status` → `known_nodes` contains 3 leader-role nodes and all worker nodes; `current_leader.node_id` is A's node.
- `discovery/leader` → returns A's URL.
- Topic list includes: `tasks-high-ram`, `tasks-low-ram`, `tasks-general`, `completed-tasks`.
- All four topics have RF=1 (single broker).

---

## 2. Basic Prompt Flow

Verifies end-to-end: React → leader → Kafka → worker → Ollama → response.

```bash
curl -s -X POST $LEADER/ask \
  -H 'Content-Type: application/json' \
  -d '{"prompt": "Reply with the single word: hello"}' \
  | python3 -m json.tool
```

**Expected:** JSON response with a `response` field containing model output and a `worker` field identifying which worker handled it. Request appears in Astra (check via `curl $LEADER/cluster/requests` or the admin dashboard).

**What to watch:**
- Worker terminal logs: `Processing <request_id>` then `Done <request_id> in <ms> ms`.
- Leader logs: JSON entries showing task dispatched and result received.
- Prometheus: `gateway_tasks_completed_total` increments by 1.

---

## 3. RAM Tier Routing

Verifies that the leader routes tasks to the correct Kafka topic based on estimated token count.

### 3a. General tier (short prompt)

```bash
# ~10 tokens → tasks-general
curl -s -X POST $LEADER/ask \
  -H 'Content-Type: application/json' \
  -d '{"prompt": "What is 2 + 2?"}' \
  | python3 -m json.tool
```

**Expected:** Leader logs show `tier: general`; `gateway_tasks_by_tier_total{tier="general"}` increments.

### 3b. Low-RAM tier (medium prompt)

```bash
# Pad the prompt to ~600 estimated tokens (2400+ chars)
MEDIUM=$(python3 -c "print('Explain the following concept in detail: ' + 'context ' * 500)")
curl -s -X POST $LEADER/ask \
  -H 'Content-Type: application/json' \
  -d "{\"prompt\": \"$MEDIUM\"}" \
  | python3 -m json.tool
```

**Expected:** `tier: low-ram` in leader logs; `gateway_tasks_by_tier_total{tier="low-ram"}` increments.

### 3c. High-RAM tier (large prompt)

```bash
# >2000 estimated tokens (8000+ chars)
LARGE=$(python3 -c "print('Summarize the following document: ' + 'word ' * 2200)")
curl -s -X POST $LEADER/ask \
  -H 'Content-Type: application/json' \
  -d "{\"prompt\": \"$LARGE\"}" \
  | python3 -m json.tool
```

**Expected:** `tier: high-ram` in leader logs (or downgraded to `low-ram`/`general` if no worker meets the 16 GB threshold); `gateway_tasks_by_tier_total` increments for the chosen tier.

### 3d. Explicit tier override

```bash
curl -s -X POST $LEADER/ask \
  -H 'Content-Type: application/json' \
  -d '{"prompt": "Short prompt.", "tier": "high-ram"}' \
  | python3 -m json.tool
```

**Expected:** Leader attempts `high-ram` first regardless of token count; downgrades if no eligible worker.

---

## 4. Skills Routing

Verifies that the leader auto-classifies prompts by keyword and routes to a worker advertising the matching skill, and that the SKILL.md system prompt influences model output.

### 4a. Coding skill (auto-classified)

```bash
curl -s -X POST $LEADER/ask \
  -H 'Content-Type: application/json' \
  -d '{"prompt": "Write a Python function to reverse a string with type hints"}' \
  | python3 -m json.tool
```

**Expected:** Leader logs show `skill: coding`; response uses type hints and clean code style matching the coding SKILL.md.

### 4b. Summarization skill (auto-classified)

```bash
curl -s -X POST $LEADER/ask \
  -H 'Content-Type: application/json' \
  -d '{"prompt": "Summarize the key points of distributed systems in bullet points"}' \
  | python3 -m json.tool
```

**Expected:** Leader logs show `skill: summarization`; response is concise and bullet-pointed.

### 4c. Creative writing skill (explicit)

```bash
curl -s -X POST $LEADER/ask \
  -H 'Content-Type: application/json' \
  -d '{"prompt": "Write a two-sentence story about a robot.", "skill": "creative-writing"}' \
  | python3 -m json.tool
```

**Expected:** `skill_strict: true` path used (explicit skill); 503 if no worker advertises `creative-writing`, otherwise narrative response.

### 4d. Unknown skill (strict — expect 503)

```bash
curl -s -X POST $LEADER/ask \
  -H 'Content-Type: application/json' \
  -d '{"prompt": "Help me.", "skill": "nonexistent-skill"}' \
  | python3 -m json.tool
```

**Expected:** HTTP 503 with `"No worker advertising skill 'nonexistent-skill' is online"`.

---

## 5. Multi-worker Load Distribution

Verifies that concurrent tasks spread across available workers.

```bash
# Fire 6 tasks simultaneously
for i in $(seq 1 6); do
  curl -s -X POST $LEADER/ask \
    -H 'Content-Type: application/json' \
    -d "{\"prompt\": \"Task $i: Count to 5\"}" &
done
wait
```

**Expected:**
- Worker terminals on multiple laptops each show `Processing` entries.
- `gateway_tasks_completed_by_worker_total` has non-zero counts for at least 2 distinct `worker_id` labels.
- Grafana "Tasks completed by worker" panel shows distribution across workers.

---

## 6. Worker Failover

Verifies the cluster continues serving requests when a worker goes offline.

1. Note the current worker count:
   ```bash
   curl -s $LEADER/cluster/status | python3 -c "import sys,json; s=json.load(sys.stdin); print(len([n for n in s['known_nodes'] if n.get('role')=='worker']))"
   ```
2. Kill the worker on Laptop C (`Ctrl+C` on that terminal).
3. Wait ~15 seconds for the heartbeat timeout to mark it offline.
4. Send a prompt:
   ```bash
   curl -s -X POST $LEADER/ask \
     -H 'Content-Type: application/json' \
     -d '{"prompt": "Are you still working?"}' \
     | python3 -m json.tool
   ```
5. Restart the worker on Laptop C (`python worker.py`).

**Expected:**
- After step 3: `cluster/status` shows one fewer active worker.
- After step 4: Request completes on one of the remaining workers. No 503 (as long as ≥ 1 worker remains online).
- After step 5: Worker re-registers; `cluster/status` shows it alive again.

---

## 7. Leader Failover

Verifies that a new leader is elected when the current leader dies and that workers follow automatically via Upstash discovery.

1. Confirm current leader is Laptop A:
   ```bash
   curl -s $LEADER/discovery/leader
   ```
2. Kill the leader process on Laptop A (`Ctrl+C`).
3. Watch Laptops B and C logs for election messages (within ~15 seconds).
4. Query the new leader (should be B):
   ```bash
   NEW_LEADER=http://<B_IP>:8000
   curl -s $NEW_LEADER/cluster/status | python3 -m json.tool
   curl -s $NEW_LEADER/discovery/leader
   ```
5. Send a prompt through the new leader:
   ```bash
   curl -s -X POST $NEW_LEADER/ask \
     -H 'Content-Type: application/json' \
     -d '{"prompt": "Who is the leader now?"}' \
     | python3 -m json.tool
   ```
6. Restart Laptop A's leader process.

**Expected:**
- Steps 3-4: B's logs show `Election result: <B_node_id>`; Upstash discovery updates to B's URL; `current_leader` in status is B.
- Step 5: Workers switched to B automatically (via discovery or 503 hint); request completes successfully.
- After step 6: A rejoins as a peer (not leader, since B already holds it with valid quorum).

---

## 8. Kafka Broker Outage (Single Point of Failure)

The cluster runs a single Kafka broker on Laptop K. This test confirms the expected behavior when the broker goes down and verifies clean recovery when it comes back up.

1. Stop the Kafka broker on Laptop K:
   ```bash
   # On Laptop K:
   docker compose -f server/kafka/docker-compose.yml stop kafka
   ```
2. Send a prompt through the leader:
   ```bash
   curl -s -X POST $LEADER/ask \
     -H 'Content-Type: application/json' \
     -d '{"prompt": "Broker down test"}' \
     | python3 -m json.tool
   ```
3. Restart the broker on Laptop K:
   ```bash
   # On Laptop K:
   docker compose -f server/kafka/docker-compose.yml start kafka
   ```
4. After ~10 seconds for reconnect, send another prompt:
   ```bash
   curl -s -X POST $LEADER/ask \
     -H 'Content-Type: application/json' \
     -d '{"prompt": "Broker restored test"}' \
     | python3 -m json.tool
   ```

**Expected:**
- Step 2: Request times out after `TASK_TIMEOUT` seconds; leader logs show producer/consumer connection errors. `gateway_tasks_failed_total` increments.
- Step 4: Request completes normally after broker recovers. Leader and workers reconnect automatically. `gateway_tasks_completed_total` increments.

> **Note:** A single Kafka broker is a single point of failure for task dispatch. Leader election and worker heartbeats continue unaffected while the broker is down — only `/ask` is impacted.

---

## 9. Astra DB Persistence

Verifies that user data and request history survive leader failover because all leaders share the same Astra instance.

1. Send a prompt and note the `request_id`:
   ```bash
   curl -s -X POST $LEADER/ask \
     -H 'Content-Type: application/json' \
     -d '{"prompt": "Persistence test prompt"}' \
     | python3 -m json.tool
   ```
2. Kill Laptop A's leader (see [Worker Failover](#7-leader-failover) steps 1-4 for election steps).
3. Query request history from the new leader (B):
   ```bash
   curl -s http://<B_IP>:8000/cluster/requests | python3 -m json.tool
   ```

**Expected:** The request from step 1 appears in the history returned by B. Login credentials remain valid (test via `POST /auth/login` against B).

---

## 10. Observability Verification

Verifies that Prometheus is scraping metrics and Grafana displays them.

### Prometheus

```bash
# Confirm leader is a healthy scrape target
curl -s http://localhost:9090/api/v1/targets | python3 -m json.tool | grep health

# Query completed task count
curl -s "http://localhost:9090/api/v1/query?query=gateway_tasks_completed_total" \
  | python3 -m json.tool

# Query active worker count
curl -s "http://localhost:9090/api/v1/query?query=gateway_nodes_active" \
  | python3 -m json.tool

# Query P95 ask latency over the last 10 minutes
curl -s "http://localhost:9090/api/v1/query?query=histogram_quantile(0.95,rate(gateway_ask_duration_seconds_bucket[10m]))" \
  | python3 -m json.tool
```

**Expected:** Target health is `"up"`; metrics return non-zero values after tests above.

### Grafana

1. Open http://localhost:3001 (or the leader laptop's Tailscale IP on port 3001).
2. Log in with `admin` / `admin`.
3. Open the **AI Gateway** dashboard (auto-provisioned from `grafana/provisioning/dashboards/ai_gateway.json`).
4. Verify panels show data for:
   - HTTP request rate and latency
   - Active / total worker nodes
   - Tasks dispatched by tier
   - Tasks completed by worker

---

## 11. Auth Flow

Verifies that login and signup work against the real backend with Astra as the user store.

```bash
# Login with seeded admin
curl -s -X POST $LEADER/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email": "admin@cluster.local", "password": "admin"}' \
  | python3 -m json.tool

# Create a new user
curl -s -X POST $LEADER/auth/signup \
  -H 'Content-Type: application/json' \
  -d '{"email": "testuser@example.com", "password": "testpass", "name": "Test User"}' \
  | python3 -m json.tool

# Login as new user
curl -s -X POST $LEADER/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email": "testuser@example.com", "password": "testpass"}' \
  | python3 -m json.tool
```

**Expected:** Login returns a user object with `role`. Signup returns the new user; subsequent login succeeds. After a leader failover, the same credentials work against the new leader (data in Astra).

---

## Quick Reference: Expected Metric Changes Per Test

| Test | Metric | Change |
|---|---|---|
| Any `/ask` completion | `gateway_tasks_completed_total` | +1 |
| Short prompt | `gateway_tasks_by_tier_total{tier="general"}` | +1 |
| Medium prompt | `gateway_tasks_by_tier_total{tier="low-ram"}` | +1 |
| Large prompt | `gateway_tasks_by_tier_total{tier="high-ram"}` | +1 (if eligible worker) |
| Task timeout | `gateway_tasks_failed_total` | +1 |
| Worker registered | `gateway_nodes_total` | +1 |
| Worker online | `gateway_nodes_active` | +1 |
| Worker offline | `gateway_nodes_active` | -1 |
