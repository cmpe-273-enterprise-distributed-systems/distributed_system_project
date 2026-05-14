from prometheus_client import Counter, Gauge, Histogram

http_requests_total = Counter(
    "gateway_http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status_code"],
)

http_request_duration_seconds = Histogram(
    "gateway_http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["endpoint"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

ask_duration_seconds = Histogram(
    "gateway_ask_duration_seconds",
    "End-to-end /ask latency including worker processing time",
    buckets=(1, 5, 10, 20, 30, 60, 120),
)

nodes_active = Gauge("gateway_nodes_active", "Number of active (non-offline) worker nodes")
nodes_total = Gauge("gateway_nodes_total", "Total registered worker nodes")

tasks_dispatched_total = Counter(
    "gateway_tasks_dispatched_total",
    "Total tasks dispatched to Kafka by topic",
    ["topic"],
)

tasks_completed_total = Counter("gateway_tasks_completed_total", "Total tasks successfully completed")
tasks_failed_total = Counter("gateway_tasks_failed_total", "Total tasks that timed out or errored")

# Worker assignment by RAM tier
tasks_by_tier_total = Counter(
    "gateway_tasks_by_tier_total",
    "Tasks dispatched by RAM tier (high-ram / low-ram / general)",
    ["tier"],
)

# Per-worker completion tracking
tasks_completed_by_worker_total = Counter(
    "gateway_tasks_completed_by_worker_total",
    "Tasks completed per worker node",
    ["worker_id"],
)

# RAM available per registered worker
worker_ram_gb = Gauge(
    "gateway_worker_ram_gb",
    "RAM (GB) of each registered worker",
    ["worker_id"],
)

# Token usage estimate (len(prompt) // 4)
prompt_tokens_estimated = Histogram(
    "gateway_prompt_tokens_estimated",
    "Estimated token count per prompt",
    buckets=(50, 100, 250, 500, 1000, 2000, 5000, 10000),
)
