# Upstash Serverless Redis Leader IP Setup

## Recommended Option: Upstash Serverless Redis

The easiest and fastest option is to use a **free Serverless Redis database from Upstash**.

### Why Upstash?

1. **No extra service to deploy**  
   You do not need to write, deploy, or maintain a separate microservice.

2. **Built-in REST API**  
   Upstash provides a standard HTTPS REST API, so the app can interact with Redis using normal HTTP requests. This means you do not need a Redis client or driver in your code.

---

## Example Usage

### Get the Current Leader IP

This can be done from either the React app or `worker.py`.

```python
import requests

response = requests.get(
    "https://cute-cat-1234.upstash.io/get/leader_ip",
    headers={"Authorization": "Bearer YOUR_TOKEN"}
)

leader_ip = response.json()["result"]
print(leader_ip)  # Example output: "100.64.0.5"
```

### Claim Leadership

When a worker becomes the new leader, it can update the leader IP in Upstash.

```python
import requests

requests.post(
    "https://cute-cat-1234.upstash.io/set/leader_ip/100.64.1.20",
    headers={"Authorization": "Bearer YOUR_TOKEN"}
)
```

---

## Setup Steps

1. Go to [upstash.com](https://upstash.com).
2. Sign in with GitHub.
3. Click **Create Database**.
4. Open the database details page.
5. Scroll to the **REST API** section.
6. Copy the REST URL and token.
7. Add the credentials to the project configuration, or use placeholders during development.

Once the REST URL and token are available, `worker.py` can be updated to automatically fetch the current leader IP on startup.