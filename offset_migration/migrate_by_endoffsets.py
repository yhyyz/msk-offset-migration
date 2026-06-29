#!/usr/bin/env python3
"""Scheme B — endOffsets snapshot (offset migration at a frozen cutover).

WHAT THIS DOES
    For each partition, read the NEW (MM2 target) topic's end offset E' and write
    it back to the OffsetDB as the consumer's resume point. After cutover the
    consumer starts at E' on the NEW cluster.

PRECONDITION (caller's responsibility — enforced operationally, not by this tool)
    Scheme B is only correct under a clean, frozen cutover:
        1. PRODUCERS STOPPED on the old cluster.
        2. CONSUMER DRAINED to lag 0 (everything produced has been processed).
        3. MM2 DRAINED — replication has fully caught up so the target is frozen
           at end offset E'.
    Capture endOffsets in this window — i.e. BEFORE producers switch over to the
    new cluster. If producers are still writing (or MM2 is still replicating), E'
    is a moving target and records between snapshot and cutover would be skipped;
    use Scheme A (timestamp mapping) instead when you cannot freeze.

RESULT
    With the target frozen at E', the next post-cutover record lands at E' and is
    consumed with no gap and no re-read.

The offset math lives in ``offset_migration.core.translate_endoffsets`` and is
unit-tested with zero infrastructure; this CLI is only the live wiring.
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import List

# Make the in-repo ``offset_migration`` package importable when run by path.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from offset_migration import core  # noqa: E402
from offset_migration.config import PARTITIONS, SECURITY_PROTOCOL, TARGET_TOPIC  # noqa: E402
from offset_migration.kafka_adapter import KafkaPythonClient  # noqa: E402
from offset_migration.offsetdb import OffsetDB  # noqa: E402

_DEFAULT_BOOTSTRAP_FILE = os.path.join(_REPO_ROOT, ".cluster", "bootstrap.txt")


def read_bootstrap(explicit: "str | None") -> str:
    """Return the bootstrap string from an explicit value or .cluster/bootstrap.txt."""
    if explicit:
        return explicit.strip()
    with open(_DEFAULT_BOOTSTRAP_FILE, "r", encoding="utf-8") as fh:
        value = fh.read().strip()
    if not value:
        raise SystemExit(f"empty bootstrap file: {_DEFAULT_BOOTSTRAP_FILE}")
    return value


def parse_partitions(spec: str) -> List[int]:
    """Parse the --partitions value.

    Two accepted forms:
      * a single integer COUNT  -> range(count), e.g. "2" -> [0, 1]
      * a comma-separated LIST  -> those exact partitions, e.g. "0,1,3" -> [0,1,3]
    """
    spec = spec.strip()
    if "," in spec:
        return [int(part.strip()) for part in spec.split(",") if part.strip() != ""]
    return list(range(int(spec)))


def main(argv: "list[str] | None" = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--bootstrap", default=None,
                        help="bootstrap servers; default reads .cluster/bootstrap.txt")
    parser.add_argument("--new-bootstrap", default=None,
                        help="NEW cluster bootstrap (default: --bootstrap)")
    parser.add_argument("--new-topic", default=TARGET_TOPIC,
                        help=f"NEW (MM2 target) topic (default {TARGET_TOPIC})")
    parser.add_argument("--db", required=True, help="path to the OffsetDB sqlite file")
    parser.add_argument("--new-cluster", required=True,
                        help="cluster_id to write for the NEW cluster (e.g. 'new')")
    parser.add_argument("--partitions", default=str(PARTITIONS),
                        help=("partitions to migrate: an integer COUNT -> range "
                              f"(default {PARTITIONS} -> 0..{PARTITIONS - 1}), or a "
                              "comma list like '0,1,3'"))
    parser.add_argument("--security-protocol", default=SECURITY_PROTOCOL,
                        help=("client security protocol (default PLAINTEXT). For MSK "
                              "IAM use SASL_SSL with OAUTHBEARER / AWS_MSK_IAM via the "
                              "aws-msk-iam-sasl-signer token provider — see "
                              "kafka_adapter.py module docstring for the wiring."))
    args = parser.parse_args(argv)

    base_bootstrap = read_bootstrap(args.bootstrap)
    new_bootstrap = args.new_bootstrap.strip() if args.new_bootstrap else base_bootstrap
    partitions = parse_partitions(args.partitions)

    print(f"[scheme_b] new_bootstrap={new_bootstrap}")
    print(f"[scheme_b] new_topic={args.new_topic} partitions={partitions} "
          f"security_protocol={args.security_protocol}")
    print("[scheme_b] PRECONDITION: producers stopped + consumer drained + MM2 drained "
          "(target frozen at E'); capture endOffsets BEFORE producers switch.")

    # Client is built lazily here in main() (never at import) and reuses a single
    # KafkaConsumer; constructing it does not contact the cluster.
    new_client = KafkaPythonClient(new_bootstrap, security_protocol=args.security_protocol)
    db = OffsetDB(args.db)
    try:
        for p in partitions:
            new_off = core.translate_endoffsets(new_client, args.new_topic, p)
            db.upsert(args.new_cluster, args.new_topic, p, new_off)
            print(f"p{p}: {args.new_cluster}/{args.new_topic} endOffset -> @{new_off}")
    finally:
        db.close()
        new_client.close()

    print("[scheme_b] done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
