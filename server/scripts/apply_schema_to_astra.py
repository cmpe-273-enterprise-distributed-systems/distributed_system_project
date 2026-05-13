#!/usr/bin/env python3
"""
Apply Cassandra schema to Astra DB.

Setup:
  1. Create an Astra database at https://astra.datastax.com
  2. Generate credentials: Settings → Access Control → Service Accounts (create one)
  3. Download the secure-connect bundle: Settings → Administration → Certificates
  4. Set environment variables:
       export ASTRA_CLIENT_ID="your_client_id"
       export ASTRA_CLIENT_SECRET="your_client_secret"  
       export ASTRA_SECURE_BUNDLE="/path/to/secure-connect-yourdb.zip"

Run:
  python server/scripts/apply_schema_to_astra.py
"""

import os
import sys
from pathlib import Path

try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
except ImportError:
    print("ERROR: cassandra-driver not installed. Install with:")
    print("  pip install cassandra-driver")
    sys.exit(1)


def main():
    # Get Astra credentials from environment
    client_id = os.getenv("ASTRA_CLIENT_ID")
    client_secret = os.getenv("ASTRA_CLIENT_SECRET")
    secure_bundle = os.getenv("ASTRA_SECURE_BUNDLE")

    if not all([client_id, client_secret, secure_bundle]):
        print("ERROR: Missing Astra credentials. Set these environment variables:")
        print("  ASTRA_CLIENT_ID")
        print("  ASTRA_CLIENT_SECRET")
        print("  ASTRA_SECURE_BUNDLE")
        print("\nSee script docstring for setup instructions.")
        sys.exit(1)

    if not Path(secure_bundle).exists():
        print(f"ERROR: Secure bundle not found: {secure_bundle}")
        sys.exit(1)

    print(f"Connecting to Astra DB...")
    try:
        auth_provider = PlainTextAuthProvider(
            username=client_id,
            password=client_secret
        )
        cluster = Cluster(
            cloud={"secure_connect_bundle": secure_bundle},
            auth_provider=auth_provider,
            connect_timeout=30,
            idle_heartbeat_interval=60,
        )
        session = cluster.connect()
        session.default_timeout = 120
        print("✓ Connected to Astra DB\n")
    except Exception as e:
        print(f"ERROR: Failed to connect to Astra DB: {e}")
        sys.exit(1)

    # Find CQL files in client/db relative to project root
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent
    cql_dir = project_root / "client" / "db"

    if not cql_dir.exists():
        print(f"ERROR: Schema directory not found: {cql_dir}")
        sys.exit(1)

    cql_files = sorted(cql_dir.glob("*.cql"))

    if not cql_files:
        print(f"ERROR: No .cql files found in {cql_dir}")
        sys.exit(1)

    print(f"Found {len(cql_files)} schema files:\n")

    def clean_statement(stmt: str) -> str:
        """Remove comments and extra whitespace from a CQL statement."""
        lines = []
        for line in stmt.split('\n'):
            # Remove inline comments
            if '--' in line:
                line = line[:line.index('--')]
            line = line.strip()
            if line:
                lines.append(line)
        return ' '.join(lines)

    failed = False
    for cql_file in cql_files:
        print(f"Processing {cql_file.name}...")
        try:
            content = cql_file.read_text()
            statements = [s.strip() for s in content.split(";")]

            file_failed = False
            for raw_stmt in statements:
                stmt = clean_statement(raw_stmt)
                if not stmt:
                    continue

                try:
                    session.execute(stmt, timeout=120)
                except Exception as e:
                    print(f"  ✗ Statement failed: {stmt[:60]}...")
                    print(f"    Error: {e}")
                    file_failed = True
                    failed = True

            if file_failed:
                print(f"  ⚠ {cql_file.name} (some statements failed)")
            else:
                print(f"  ✓ {cql_file.name}")

        except Exception as e:
            print(f"  ✗ Failed to read/execute {cql_file.name}: {e}")
            failed = True

    cluster.shutdown()
    print()

    if failed:
        print("⚠ Schema application completed with errors.")
        sys.exit(1)
    else:
        print("✓ Schema applied successfully!")
        sys.exit(0)


if __name__ == "__main__":
    main()
