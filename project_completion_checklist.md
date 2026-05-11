# 🚀 Distributed AI Gateway: Completion Checklist (V2)

Here is the master checklist required to take this project from its current mock-data state to a fully functional, self-healing distributed system.

## 1. 🌐 Infrastructure & Networking
- [ ] **Tailscale Mesh Setup:** Install and configure Tailscale on all participating laptops to establish the private `100.x.x.x` network.
- [x] **Discovery Service:** Deploy a lightweight, highly-available service (e.g., a simple cloud function, KV store, or Dynamic DNS) that holds the single source of truth: `Current_Leader_IP`.
- [ ] **Ollama Provisioning:** Install Ollama on all worker laptops and pull the required local models (e.g., `mistral`, `phi-2`).
- [ ] **Worker Skill Files:** Create the local `.skill` files on each worker laptop to define what specialized prompts they are allowed to handle (e.g., coding, creative writing).
- [ ] Observability 

## 2. 💽 Distributed State & Security (Crucial Missing Pieces)
- [ ] **Shared Database Setup:** Set up a database (Cassandra) to store Users, Passwords, and Request History. *If the Leader laptop dies, the new Leader must be able to read this database so users don't lose their chat history.*
- [Skip] **JWT Authentication:** Implement JSON Web Tokens (JWT) on the Leader to secure the `/ask` endpoints so only logged-in users can send prompts.

## 3. 🧠 Backend: The Worker Node (`worker.py`) (shivansh)
- [ ] **Hardware Profiling:** Script logic to read the host machine's specs (Total RAM, available models) and `.skill` files on boot.
- [ ] **Registration & Heartbeats:** Build the API client to `POST /register` to the Leader on startup, and send periodic `POST /heartbeat` pings.
- [ ] **Local Execution:** Build the bridge to send pulled prompts to the local Ollama instance (`http://localhost:11434/api/generate`).

## 4. 👑 Backend: The Leader Node (FastAPI) (Divya)
- [ ] **API Endpoints:** Build the real FastAPI routes (`/register`, `/heartbeat`, `/ask`, cluster stats, `/auth/login`, `/auth/signup`).
- [ ] **Registry Management:** Build the internal state manager that tracks which nodes are active, busy, or dead based on heartbeats.
- [ ] **Response Router:** Build the logic that listens to completed tasks from workers and routes them back to the specific user's open connection.
- [ ] **Failover / Leader Election:** Implement the consensus protocol (e.g., Raft). If the current Leader process dies, remaining nodes detect the failure, vote a new Leader, and automatically update the **Discovery Service** with the new IP.

## 5. 💻 Frontend: Network Wiring (React) (Abhinand)
- [ ] **Swap Mocks:** Replace all simulated data functions in `src/api/index.js` with real `axios` HTTP requests.
- [ ] **Dynamic IP Resolution:** Remove the hardcoded `.env` Leader IP. Write an initialization function that queries the **Discovery Service** on boot to find the active Leader.
- [ ] **Self-Healing Interceptors:** Add an `axios` interceptor. If a request to the Leader fails (Network Error), automatically pause, re-query the Discovery Service for the *new* Leader IP, and seamlessly retry the request.
- [ ] **Real-time Responses:** Implement Server-Sent Events (SSE) or WebSockets so the client chat screen streams the AI's response in real-time as it arrives from the worker.

## 6. 📨 Communication: Kafka Pipeline (Conlyn)
- [Blocked] **Kafka Cluster / Broker Setup:** Install and configure Apache Kafka on laptops capable of being a Leader. *If a new Leader is elected, it must have a message broker ready to go.* (waiting for service discovery & group concensus on replication direction...will discuss)
- [x] **Smart Dispatcher (Kafka Publisher):** Write the logic that receives a prompt, analyzes required specs, and places it in the correct Kafka topic (e.g., `tasks-high-ram`).
- [x] **Kafka Consumer:** Implement the listener on workers that constantly polls the Leader's Kafka broker for new tasks matching the worker's skills/RAM.
- [x] **Result Returner:** Script the logic to package the local Ollama output and push it back to the Leader's Kafka `completed-tasks` topic.

## Observability 
## Cloud end-to-end 
