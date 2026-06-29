#!/usr/bin/env python3
"""Read-side verification helpers for the offset-migration harness.

Importable helpers (used by the test runner, e.g. T7, to assert resume points)
plus a tiny ``read`` CLI. Every call is per (topic, partition); nothing here
mutates the cluster.

  read_at(bootstrap, topic, partition, offset) -> dict | None
  end_offset(bootstrap, topic, partition)      -> int
  begin_offset(bootstrap, topic, partition)    -> int
  count(bootstrap, topic, partition)           -> int   (end - begin)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Optional

# Make the in-repo ``offset_migration`` package importable when run by path.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from kafka import KafkaConsumer, TopicPartition  # noqa: E402

from offset_migration.config import SECURITY_PROTOCOL  # noqa: E402

_DEFAULT_BOOTSTRAP_FILE = os.path.join(_REPO_ROOT, ".cluster", "bootstrap.txt")


def read_bootstrap(explicit: Optional[str]) -> str:
    """Return the bootstrap string from --bootstrap or .cluster/bootstrap.txt."""
    if explicit:
        return explicit.strip()
    with open(_DEFAULT_BOOTSTRAP_FILE, "r", encoding="utf-8") as fh:
        value = fh.read().strip()
    if not value:
        raise SystemExit(f"empty bootstrap file: {_DEFAULT_BOOTSTRAP_FILE}")
    return value


def _make_consumer(bootstrap: str) -> KafkaConsumer:
    return KafkaConsumer(
        bootstrap_servers=bootstrap.split(","),
        security_protocol=SECURITY_PROTOCOL,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )


def _decode(raw) -> Optional[str]:
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        return raw.decode("utf-8", errors="replace")
    return str(raw)


def read_at(bootstrap: str, topic: str, partition: int, offset: int) -> "Optional[dict[str, object]]":
    """Return the record AT ``offset`` as a dict, or None if nothing is there.

    dict shape: {"offset", "key", "value", "timestamp"} with key/value decoded
    to utf-8 strings.
    """
    consumer = _make_consumer(bootstrap)
    tp = TopicPartition(topic, partition)
    try:
        consumer.assign([tp])
        consumer.seek(tp, offset)
        batch = consumer.poll(timeout_ms=5000, max_records=1)
        records = batch.get(tp, [])
        if not records:
            return None
        rec = records[0]
        return {
            "offset": rec.offset,
            "key": _decode(rec.key),
            "value": _decode(rec.value),
            "timestamp": rec.timestamp,
        }
    finally:
        consumer.close()


def end_offset(bootstrap: str, topic: str, partition: int) -> int:
    """Log-end offset (the offset the next produced record will get)."""
    consumer = _make_consumer(bootstrap)
    tp = TopicPartition(topic, partition)
    try:
        consumer.assign([tp])
        return consumer.end_offsets([tp])[tp]
    finally:
        consumer.close()


def begin_offset(bootstrap: str, topic: str, partition: int) -> int:
    """Earliest still-available offset."""
    consumer = _make_consumer(bootstrap)
    tp = TopicPartition(topic, partition)
    try:
        consumer.assign([tp])
        return consumer.beginning_offsets([tp])[tp]
    finally:
        consumer.close()


def count(bootstrap: str, topic: str, partition: int) -> int:
    """Number of currently-retained records (end - begin)."""
    consumer = _make_consumer(bootstrap)
    tp = TopicPartition(topic, partition)
    try:
        consumer.assign([tp])
        begin = consumer.beginning_offsets([tp])[tp]
        end = consumer.end_offsets([tp])[tp]
        return end - begin
    finally:
        consumer.close()


def main(argv: "Optional[list[str]]" = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_read = sub.add_parser("read", help="print the record JSON at an offset")
    p_read.add_argument("--bootstrap", default=None)
    p_read.add_argument("--topic", required=True)
    p_read.add_argument("--partition", type=int, required=True)
    p_read.add_argument("--offset", type=int, required=True)

    p_end = sub.add_parser("end", help="print the end (log-end) offset")
    p_end.add_argument("--bootstrap", default=None)
    p_end.add_argument("--topic", required=True)
    p_end.add_argument("--partition", type=int, required=True)

    p_begin = sub.add_parser("begin", help="print the beginning offset")
    p_begin.add_argument("--bootstrap", default=None)
    p_begin.add_argument("--topic", required=True)
    p_begin.add_argument("--partition", type=int, required=True)

    p_count = sub.add_parser("count", help="print end-begin record count")
    p_count.add_argument("--bootstrap", default=None)
    p_count.add_argument("--topic", required=True)
    p_count.add_argument("--partition", type=int, required=True)

    args = parser.parse_args(argv)
    bootstrap = read_bootstrap(args.bootstrap)

    if args.cmd == "read":
        rec = read_at(bootstrap, args.topic, args.partition, args.offset)
        print(json.dumps(rec))
    elif args.cmd == "end":
        print(end_offset(bootstrap, args.topic, args.partition))
    elif args.cmd == "begin":
        print(begin_offset(bootstrap, args.topic, args.partition))
    elif args.cmd == "count":
        print(count(bootstrap, args.topic, args.partition))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
