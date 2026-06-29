#!/usr/bin/env python3
"""Seed the live cluster to force per-partition offset divergence, then verify.

Order of operations matters:

  1. PRE-SEED the TARGET topic ``src.orders`` with junk records FIRST. Their
     timestamps are pinned to 2020-01-01 (``JUNK_TS_MS``) so they sort *before*
     any real record. Producing a *different* junk count per partition
     (``JUNK_PER_PARTITION``) shifts the target offsets relative to the source by
     a different amount per partition (source offset k -> target offset N_p + k),
     which is exactly the divergence the migration must cope with.

  2. Produce the REAL records to the SOURCE topic ``orders``. MirrorMaker 2 will
     later replicate these into ``src.orders`` (appended *after* the junk),
     preserving their recent, strictly-increasing timestamps.

Run AFTER topics exist (T6 creates them); this script never creates topics.
"""
from __future__ import annotations

import argparse
import os
import sys
import time

# Make the in-repo ``offset_migration`` package importable when run by path.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from kafka import KafkaProducer  # noqa: E402

from offset_migration.config import (  # noqa: E402
    JUNK_PER_PARTITION,
    JUNK_TS_MS,
    PARTITIONS,
    REAL_RECORDS_PER_PARTITION,
    SECURITY_PROTOCOL,
    SOURCE_TOPIC,
    TARGET_TOPIC,
)

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


def make_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        security_protocol=SECURITY_PROTOCOL,
        key_serializer=lambda v: v.encode("utf-8"),
        value_serializer=lambda v: v.encode("utf-8"),
        acks="all",
    )


def main(argv: "list[str] | None" = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default=None,
                        help="bootstrap servers; default reads .cluster/bootstrap.txt")
    parser.add_argument("--db", default=None,
                        help="optional path (unused by seeder; accepted for harness symmetry)")
    args = parser.parse_args(argv)

    bootstrap = read_bootstrap(args.bootstrap)
    print(f"[seeder] bootstrap={bootstrap}")
    print(f"[seeder] source_topic={SOURCE_TOPIC} target_topic={TARGET_TOPIC} "
          f"partitions={PARTITIONS}")

    producer = make_producer(bootstrap)

    # counts[(topic, partition)] -> int produced
    counts: "dict[tuple[str, int], int]" = {}
    # real_ts_range[partition] -> (min_ts, max_ts)
    real_ts_range: "dict[int, tuple[int, int]]" = {}

    # --- Step 1: pre-seed junk into the TARGET topic to diverge offsets. ---
    for p in range(PARTITIONS):
        junk_n = JUNK_PER_PARTITION.get(p, 0)
        for i in range(junk_n):
            producer.send(
                TARGET_TOPIC,
                key=f"junk-{p}-{i}",
                value="junk",
                partition=p,
                timestamp_ms=JUNK_TS_MS,
            )
        counts[(TARGET_TOPIC, p)] = junk_n
        print(f"[seeder] pre-seeded {junk_n} junk record(s) into "
              f"{TARGET_TOPIC} p{p} (ts={JUNK_TS_MS})")

    # --- Step 2: produce REAL records into the SOURCE topic. ---
    base_ts = int(time.time() * 1000)
    print(f"[seeder] real-record base timestamp_ms={base_ts}")
    for p in range(PARTITIONS):
        # Safe defaults: REAL_RECORDS_PER_PARTITION is constant >=1, loop always runs.
        first_ts = base_ts
        last_ts = base_ts
        for i in range(REAL_RECORDS_PER_PARTITION):
            ts = base_ts + i * 1000  # strictly increasing per partition
            producer.send(
                SOURCE_TOPIC,
                key=f"k{i}",
                value=f"v{i}",
                partition=p,
                timestamp_ms=ts,
            )
            last_ts = ts
        counts[(SOURCE_TOPIC, p)] = REAL_RECORDS_PER_PARTITION
        real_ts_range[p] = (first_ts, last_ts)

    producer.flush()
    producer.close()

    # --- Summary ---
    print("[seeder] === summary ===")
    for (topic, partition) in sorted(counts):
        print(f"[seeder]   {topic} p{partition}: produced={counts[(topic, partition)]}")
    for p in sorted(real_ts_range):
        lo, hi = real_ts_range[p]
        print(f"[seeder]   {SOURCE_TOPIC} p{p}: real ts range [{lo}, {hi}] "
              f"(span={hi - lo} ms over {REAL_RECORDS_PER_PARTITION} records)")
    print("[seeder] done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
