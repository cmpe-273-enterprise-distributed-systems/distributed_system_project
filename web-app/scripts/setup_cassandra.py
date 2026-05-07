from cassandra.cluster import Cluster
from pathlib import Path
import time

def connect():
    for attempt in range(40):
        try:
            cluster = Cluster(["127.0.0.1"], port=9042)
            session = cluster.connect()
            return cluster, session
        except Exception:
            print(f"Waiting for Cassandra to start... (attempt {attempt + 1}/40)")
            time.sleep(3)
    raise RuntimeError("Cassandra is not ready")

def split_cql(content):
    statements = []
    current = []

    for line in content.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue

        current.append(line)

        if stripped.endswith(";"):
            statement = "\n".join(current).strip().rstrip(";")
            statements.append(statement)
            current = []

    return statements

cluster, session = connect()

for file in sorted(Path("db").glob("*.cql")):
    print(f"Running {file}")
    content = file.read_text()
    for statement in split_cql(content):
        session.execute(statement)

cluster.shutdown()
print("Cassandra setup complete")
