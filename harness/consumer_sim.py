#!/usr/bin/env python3
"""Simulate a consumer's committed position and persist it to the external OffsetDB.

The customer's real consumers store, per partition, the *next* offset they would
read (``committed_offset``, standard Kafka commit semantics) plus the timestamp of
the LAST message they actually processed (offset ``committed_offset - 1``). This
script fabricates that state on the SOURCE topic so the migration logic has
something to translate.

Flags map to the migration corner cases:

  --upto N        committed_offset = N  (i.e. records 0..N-1 are "processed";
                  the last processed record is at offset N-1).
  --caughtup      committed_offset = end_offset(partition)  (drained / lag 0).
  --no-timestamp  store committed_offset but last_msg_ts = NULL, exercising
                  Scheme A's reverse-lookup path (it re-reads the ts from the OLD
                  cluster at offset committed_offset-1).

last_msg_ts is the timestamp of the record at offset ``committed_offset - 1``;
when committed_offset == 0 (never consumed) it is None.
"""
from __future__ import annotations

import argparse
import os
import sys

# Make the in-repo ``offset_migration`` package importable when run by path.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from kafka import KafkaConsumer, TopicPartition  # noqa: E402

from offset_migration.config import SECURITY_PROTOCOL, SOURCE_TOPIC  # noqa: E402
from offset_migration.offsetdb import OffsetDB  # noqa: E402

_DEFAULT_BOOTSTRAP_FILE = os.path.join(_REPO_ROOT, ".cluster", "bootstrap.txt")


def read_bootstrap(explicit: "str | None") -> str:
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


def end_offset(consumer: KafkaConsumer, tp: TopicPartition) -> int:
    return consumer.end_offsets([tp])[tp]


def timestamp_at(consumer: KafkaConsumer, tp: TopicPartition, target_offset: int) -> "int | None":
    """Return the record timestamp (ms) at ``target_offset`` via beginning+poll scan.

    Assigns the partition, seeks to the beginning, and polls forward accumulating
    offset->timestamp until the target offset is observed or the log end is
    reached. Returns None if the target offset has no record.
    """
    if target_offset < 0:
        return None
    end = end_offset(consumer, tp)
    if target_offset >= end:
        return None

    consumer.seek_to_beginning(tp)
    seen: "dict[int, int]" = {}
    empty_polls = 0
    while True:
        if target_offset in seen:
            break
        pos = consumer.position(tp)
        if pos is not None and pos >= end:
            break
        batch = consumer.poll(timeout_ms=5000, max_records=500)
        records = batch.get(tp, [])
        if not records:
            empty_polls += 1
            if empty_polls >= 3:
                break
            continue
        empty_polls = 0
        for rec in records:
            seen[rec.offset] = rec.timestamp
    return seen.get(target_offset)


def main(argv: "list[str] | None" = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default=None,
                        help="bootstrap servers; default reads .cluster/bootstrap.txt")
    parser.add_argument("--db", required=True, help="path to the OffsetDB sqlite file")
    parser.add_argument("--topic", default=SOURCE_TOPIC,
                        help=f"topic the consumer reads (default {SOURCE_TOPIC})")
    parser.add_argument("--partition", type=int, required=True, help="partition to record")
    parser.add_argument("--upto", type=int, default=None,
                        help="committed_offset; records 0..upto-1 are 'processed'")
    parser.add_argument("--caughtup", action="store_true",
                        help="set committed_offset = end_offset (consumer drained)")
    parser.add_argument("--no-timestamp", action="store_true",
                        help="store committed_offset but last_msg_ts=None (Scheme A reverse-lookup)")
    args = parser.parse_args(argv)

    if args.upto is None and not args.caughtup:
        parser.error("provide --upto N or --caughtup")

    bootstrap = read_bootstrap(args.bootstrap)
    tp = TopicPartition(args.topic, args.partition)
    consumer = _make_consumer(bootstrap)
    consumer.assign([tp])

    try:
        if args.caughtup:
            committed_offset = end_offset(consumer, tp)
            print(f"[consumer_sim] --caughtup: end_offset({args.topic} p{args.partition})="
                  f"{committed_offset}")
        else:
            committed_offset = args.upto

        if committed_offset == 0:
            last_msg_ts = None
            print("[consumer_sim] committed_offset=0 (never consumed) -> last_msg_ts=None")
        elif args.no_timestamp:
            last_msg_ts = None
            print("[consumer_sim] --no-timestamp: storing offset only, last_msg_ts=None")
        else:
            last_processed = committed_offset - 1
            last_msg_ts = timestamp_at(consumer, tp, last_processed)
            print(f"[consumer_sim] last processed offset={last_processed} "
                  f"-> last_msg_ts={last_msg_ts}")
    finally:
        consumer.close()

    db = OffsetDB(args.db)
    try:
        db.upsert(args.topic, args.partition, committed_offset, last_msg_ts)
        stored = db.get(args.topic, args.partition)
    finally:
        db.close()

    print(f"[consumer_sim] wrote to {args.db}: topic={args.topic} "
          f"partition={args.partition} committed_offset={committed_offset} "
          f"last_msg_ts={last_msg_ts}")
    print(f"[consumer_sim] readback: {stored}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
