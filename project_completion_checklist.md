# 🚀 Distributed AI Gateway: Completion Checklist (V2)

Here is the master checklist required to take this project from its current mock-data state to a fully functional, self-healing distributed system.

## 1. 🌐 Infrastructure & Networking
- [x] **Tailscale Mesh Setup:** Install and configure Tailscale on all participating laptops to establish the private `100.x.x.x` network.
- [x] **Discovery Service:** Deploy a lightweight, highly-available service (e.g., a simple cloud function, KV store, or Dynamic DNS) that holds the single source of truth: `Current_Leader_IP`.
- [x] **Ollama Provisioning:** Install Ollama on all worker laptops and pull the required local models (e.g., `mistral`, `phi-2`).
- [x] **Worker Skill Files:** Create the local `.skill` files on each worker laptop to define what specialized prompts they are allowed to handle (e.g., coding, creative writing).
- [x] Observability 

## 2. 💽 Distributed State & Security (Crucial Missing Pieces)
- [x] **Shared Database Setup:** Migrated from per-laptop docker Cassandra (each leader had its own empty DB after failover) to managed Astra DataStax. All leaders now connect to the same shared cloud Cassandra so Users, Passwords, and Request History survive leader failover. Code path uses cassandra-driver with prepared statements; selectable via `USE_ASTRA=true|false` env var. See `server/leader/.env.example` for credentials and `.venv/astra_migration_plan.md` for the full migration writeup.
- [Skip] **JWT Authentication:** Implement JSON Web Tokens (JWT) on the Leader to secure the `/ask` endpoints so only logged-in users can send prompts.

## 3. 🧠 Backend: The Worker Node (`worker.py`) (shivansh)
- [x] **Hardware Profiling:** Script logic to read the host machine's specs (Total RAM, available models) and `.skill` files on boot.
- [x] **Registration & Heartbeats:** Build the API client to `POST /register` to the Leader on startup, and send periodic `POST /heartbeat` pings.
- [x] **Local Execution:** Build the bridge to send pulled prompts to the local Ollama instance (`http://localhost:11434/api/generate`).

## 4. 👑 Backend: The Leader Node (FastAPI) (Divya)
- [x] **API Endpoints:** Build the real FastAPI routes (`/register`, `/heartbeat`, `/ask`, cluster stats, `/auth/login`, `/auth/signup`).
- [x] **Registry Management:** Build the internal state manager that tracks which nodes are active, busy, or dead based on heartbeats.
- [x] **Response Router:** Build the logic that listens to completed tasks from workers and routes them back to the specific user's open connection.
- [x] **Failover / Leader Election:** Implement the consensus protocol (e.g., Raft). If the current Leader process dies, remaining nodes detect the failure, vote a new Leader, and automatically update the **Discovery Service** with the new IP.

## 5. 💻 Frontend: Network Wiring (React) (Abhinand)
- [x] **Swap Mocks:** Replace all simulated data functions in `src/api/index.js` with real `axios` HTTP requests.
- [x] **Dynamic IP Resolution:** Remove the hardcoded `.env` Leader IP. Write an initialization function that queries the **Discovery Service** on boot to find the active Leader.
- [x] **Self-Healing Interceptors:** Add an `axios` interceptor. If a request to the Leader fails (Network Error), automatically pause, re-query the Discovery Service for the *new* Leader IP, and seamlessly retry the request.
- [x] **Real-time Responses:** Implement Server-Sent Events (SSE) or WebSockets so the client chat screen streams the AI's response in real-time as it arrives from the worker.

## 6. 📨 Communication: Kafka Pipeline (Conlyn)
- [x] **Kafka Cluster / Broker Setup:** Install and configure Apache Kafka on laptops capable of being a Leader. *If a new Leader is elected, it must have a message broker ready to go.* (Each of the 3 leader-eligible laptops must be on the Tailscale mesh, then run `server/kafka/docker-compose.yml` with its own `server/kafka/.env` — unique `KAFKA_NODE_ID` and that laptop's Tailscale IP as `KAFKA_ADVERTISED_HOST`; shared `KAFKA_CLUSTER_ID` and `KAFKA_QUORUM_VOTERS` listing all 3 peers.)
- [x] **Smart Dispatcher (Kafka Publisher):** Write the logic that receives a prompt, analyzes required specs, and places it in the correct Kafka topic (e.g., `tasks-high-ram`).
- [x] **Kafka Consumer:** Implement the listener on workers that constantly polls the Leader's Kafka broker for new tasks matching the worker's skills/RAM.
- [x] **Result Returner:** Script the logic to package the local Ollama output and push it back to the Leader's Kafka `completed-tasks` topic.
