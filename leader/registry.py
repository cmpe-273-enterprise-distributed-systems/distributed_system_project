import asyncio
import time
from dataclasses import dataclass, field

# A node is declared offline if no heartbeat is received within this window
HEARTBEAT_TIMEOUT_S = 15


@dataclass
class NodeInfo:
    node_id: str
    ip: str
    status: str          # idle | busy | offline
    model: str
    skills: list[str]
    ram_gb: int
    tasks_completed: int
    last_seen: float = field(default_factory=time.time)


class Registry:
    def __init__(self):
        self._nodes: dict[str, NodeInfo] = {}
        self._lock = asyncio.Lock()

    async def register(self, node_id: str, ip: str, ram_gb: int, model: str, skills: list[str]):
        async with self._lock:
            self._nodes[node_id] = NodeInfo(
                node_id=node_id,
                ip=ip,
                status="idle",
                model=model,
                skills=skills,
                ram_gb=ram_gb,
                tasks_completed=0,
                last_seen=time.time(),
            )

    async def heartbeat(self, node_id: str, status: str, tasks_completed: int) -> bool:
        """Update a node's last-seen timestamp. Returns False if node is unknown."""
        async with self._lock:
            if node_id not in self._nodes:
                return False
            node = self._nodes[node_id]
            node.status = status
            node.tasks_completed = tasks_completed
            node.last_seen = time.time()
            return True

    async def get_all(self) -> list[NodeInfo]:
        async with self._lock:
            return list(self._nodes.values())

    async def check_timeouts(self):
        """Background loop: marks nodes offline when heartbeats stop arriving."""
        while True:
            await asyncio.sleep(5)
            now = time.time()
            async with self._lock:
                for node in self._nodes.values():
                    if node.status != "offline" and (now - node.last_seen) > HEARTBEAT_TIMEOUT_S:
                        node.status = "offline"
