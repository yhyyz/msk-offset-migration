#!/usr/bin/env python3
"""Scheme A — timestamp mapping (offset migration at cutover).

WHAT THIS DOES
    For each partition, take the consumer's externally-stored position on the OLD
    topic (committed_offset + timestamp of the last processed message) and resolve
    the equivalent resume offset on the NEW topic by *timestamp*, writing it back
    to the OffsetDB under the NEW topic. The consumer — now pointed at the NEW
    cluster — then resumes from that NEW-topic offset.

WHY TIMESTAMP MAPPING
    MirrorMaker 2 does not preserve offset values (the target topic has its own
    offsets, shifted by any pre-existing/junk records) but it DOES preserve each
    record's original timestamp. So the timestamp of the last-processed message is
    the stable key that maps OLD position -> NEW position.

OPERATIONAL PROPERTIES (Scheme A)
    * Producers basically don't stop — no production freeze required.
    * Order-independent: partitions are translated independently and in any order.
    * The "+1" in the core lands the consumer on the first UNprocessed message;
      the boundary can re-deliver AT MOST ONE record, so consumers MUST dedup by
      business key (idempotent processing). See README.
    * Caught-up partitions (no record newer than last_msg_ts yet) resolve to the
      NEW topic's end offset, so the next replicated/produced record is read once.

The offset math itself lives in ``offset_migration.core.translate_timestamp`` and
is unit-tested with zero infrastructure; this CLI is only the live wiring.
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
from offset_migration.config import PARTITIONS, SECURITY_PROTOCOL, SOURCE_TOPIC, TARGET_TOPIC  # noqa: E402
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
    parser.add_argument("--old-bootstrap", default=None,
                        help="OLD cluster bootstrap (default: --bootstrap)")
    parser.add_argument("--new-bootstrap", default=None,
                        help="NEW cluster bootstrap (default: --bootstrap)")
    parser.add_argument("--old-topic", default=SOURCE_TOPIC,
                        help=f"OLD topic the consumer read (default {SOURCE_TOPIC})")
    parser.add_argument("--new-topic", default=TARGET_TOPIC,
                        help=f"NEW (MM2 target) topic (default {TARGET_TOPIC})")
    parser.add_argument("--db", required=True, help="path to the OffsetDB sqlite file")
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
    old_bootstrap = args.old_bootstrap.strip() if args.old_bootstrap else base_bootstrap
    new_bootstrap = args.new_bootstrap.strip() if args.new_bootstrap else base_bootstrap
    partitions = parse_partitions(args.partitions)

    print(f"[scheme_a] old_bootstrap={old_bootstrap} new_bootstrap={new_bootstrap}")
    print(f"[scheme_a] old_topic={args.old_topic} new_topic={args.new_topic} "
          f"partitions={partitions} security_protocol={args.security_protocol}")

    # Clients are built lazily here in main() (never at import) and reuse a single
    # KafkaConsumer each; constructing them does not contact the cluster.
    new_client = KafkaPythonClient(new_bootstrap, security_protocol=args.security_protocol)
    old_client = KafkaPythonClient(old_bootstrap, security_protocol=args.security_protocol)
    db = OffsetDB(args.db)
    try:
        for p in partitions:
            row = db.get(args.old_topic, p)
            if row is None:
                print(f"[scheme_a] WARNING: no OffsetDB row for {args.old_topic} p{p}; skipping")
                continue
            committed_offset, last_ts = row
            new_off = core.translate_timestamp(
                new_client,
                args.new_topic,
                p,
                committed_offset,
                last_ts,
                old_client=old_client,
                old_topic=args.old_topic,
            )
            # Write the resume point under the NEW topic so the consumer (now
            # pointed at the NEW cluster) picks up from the NEW-topic offset.
            db.upsert(args.new_topic, p, new_off, last_ts)
            print(f"p{p}: old({args.old_topic})@{committed_offset} ts={last_ts} "
                  f"-> new({args.new_topic})@{new_off}")
    finally:
        db.close()
        new_client.close()
        old_client.close()

    print("[scheme_a] done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
