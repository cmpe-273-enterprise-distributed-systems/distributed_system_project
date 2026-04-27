# Distributed System Scenarios

Let's set the stage with 4 laptops on the VPN:

*   **Laptop A (Current Leader):** Acts as the dispatcher.
*   **Laptop B (Worker 1):** 8GB RAM (Good for simple chat models).
*   **Laptop C (Worker 2):** 16GB RAM (Good for coding models).
*   **Laptop D (Worker 3):** 64GB RAM (A beast, good for massive data models).

## Scenario 1: Routing Based on RAM

*   **Registration:** When B, C, and D boot up, they register with the Leader (A). They don't just say "I'm alive"; they report their hardware: "I am Laptop C, I have 16GB RAM." Leader A keeps this in a registry.
*   **The Request:** Alice logs into the web app and asks for a highly complex task that requires a massive AI model (e.g., Llama 3 70B).
*   **Smart Dispatching:** The Leader receives the request. It looks at its registry and realizes Laptop B and C would crash trying to run this. It specifically routes the task to Laptop D's queue because it's the only one with 64GB RAM. Laptop D pulls it, processes it, and returns the result.

## Scenario 2: Handling Failures (The "Self-Healing" App)

Distributed systems are designed to assume hardware will fail. Here is how the app stays up.

### Case A: A Worker Dies Mid-Task
*   **The Crash:** Laptop C pulls a coding task. Halfway through generating the code, the owner slams the laptop shut. It goes offline.
*   **The Detection:** Leader A expects a "heartbeat" ping from Laptop C every 5 seconds. The heartbeat stops.
*   **The Recovery:** Leader A officially declares Laptop C "dead". Because Laptop C never sent a "Task Completed" message, the task is automatically thrown back into the Kafka queue.
*   **The Hand-off:** Laptop B or D pulls the abandoned task and finishes it. Alice's browser simply spins for a few extra seconds, but she eventually gets her answer. The app never crashes.

### Case B: The Leader Dies (The Single Point of Failure)
If Leader A has a power failure, the whole brain of the operation goes down. Here is how the cluster survives:
*   **The Crash:** Leader A shuts down. The Kafka queue and FastAPI server vanish.
*   **The Election:** Laptops B, C, and D immediately notice the Leader isn't responding. They instantly talk to each other over the VPN and hold a "Leader Election." They vote, and Laptop C is promoted to be the new Leader.
*   **The Promotion:** Laptop C stops being a worker. It spins up the FastAPI server and takes over the orchestration duties.
*   **The App Re-Routes:** But wait, Alice's web app was pointing at Laptop A's IP address! How does the frontend know where to go?
    *   The system uses a tiny, hyper-reliable Discovery Server (or a dynamic DNS record).
    *   When Alice's app tries to talk to Laptop A and fails, it pings the Discovery Server: "Hey, who is the Leader right now?"
    *   The Discovery Server replies: "Laptop A died. Laptop C is the new Leader. Here is Laptop C's IP."
    *   The React app seamlessly updates and sends Alice's prompt to Laptop C. Alice might not even notice the transition occurred.

---

## Scenario 3: Monitoring the Demo (Admin Access)

**The Setup:** Your 5-device experiment is running perfectly. Laptops B, C, and D are processing background tasks as workers, and the iPhone is sending prompts. You want to show off the live Admin Dashboard (RAM usage, node status) to a friend.

**How to do it:**
1.  **Use Any Device:** You do not need a special "Admin Laptop." You can pick up *any* device that is on the Tailscale VPN. You can even use Laptop C, which is currently in the middle of processing a heavy coding task in its background terminal.
2.  **Log Out & Log In:** Open Google Chrome. If the browser is currently logged into a "Worker" or "Client" account, you must click **Log Out** first. Then, log back in using the `admin@cluster.local` email and the admin password.
3.  **Independent Systems:** The React web app will verify your admin credentials and display the Admin Dashboard. Laptop C will continue to quietly process its coding task in the background. The background worker script and the web browser you are looking at are completely separate—the worker script does not have your web login credentials, and your web login does not interfere with the worker script.
