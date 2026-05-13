"""
Standalone CLI wrapper around server/leader/kafka_admin.ensure_topics.

Usage:
    python server/scripts/ensure_kafka_topics.py --brokers a:9092,b:9092,c:9092
    python server/scripts/ensure_kafka_topics.py --brokers $KAFKA_BROKER

The leader's lifespan already calls ensure_topics on every boot, so this
script is for one-off ops:
  - Pre-seeding before the leader starts.
  - Repairing after a manual delete (e.g., when an existing topic has the
    wrong RF and you've recreated it via kafka-topics --delete).
  - CI sanity checks against an arbitrary broker.

Exits 0 if every topic ends up 'created' or 'exists'.
Exits 1 on connection failure or rf_mismatch (operator action needed).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

# Allow running from anywhere: add server/leader to sys.path so the
# kafka_admin module imports cleanly without a package install.
LEADER_DIR = Path(__file__).resolve().parent.parent / "leader"
sys.path.insert(0, str(LEADER_DIR))

from kafka_admin import DEFAULT_PARTITIONS, ensure_topics  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Ensure cluster Kafka topics exist with correct RF.")
    parser.add_argument(
        "--brokers",
        default=os.getenv("KAFKA_BROKER", "localhost:9092"),
        help="Comma-separated bootstrap servers (default: $KAFKA_BROKER or localhost:9092).",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=DEFAULT_PARTITIONS,
        help=f"Partitions per topic (default: {DEFAULT_PARTITIONS}).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=6,
        help="Retry connection this many times before giving up (default: 6).",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Show DEBUG logs.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    try:
        result = ensure_topics(args.brokers, retries=args.retries, partitions=args.partitions)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print()
    print(f"{'Topic':<24} Status")
    print(f"{'-' * 24} {'-' * 12}")
    bad = False
    for name, status in result.items():
        print(f"{name:<24} {status}")
        if status == "rf_mismatch":
            bad = True
    print()

    if bad:
        print(
            "WARN: at least one topic exists with the wrong replication factor. "
            "Case-B failover may hang on those topics. To fix, delete + recreate:\n"
            "  docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 "
            "--delete --topic <name>\n"
            "then re-run this script.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
