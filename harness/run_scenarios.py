#!/usr/bin/env python3
"""T7 — live scenario validation against the real MSK cluster + MM2.

Runs the actual deliverable CLIs (consumer_sim + migrate_by_timestamp & migrate_by_endoffsets) end-to-end
and asserts the resume point by reading the real record at the resulting offset.

Scheme A resolves with offsets_for_times(last_msg_ts) (NOT +1): resume lands on
the last-processed message / first same-ts record and reprocesses from there
(consumer dedups), never skipping an unprocessed record.

Topology produced by harness/bringup.sh:
  src.orders p0: junk[0..4] + real k0..k9 [5..14]  (end=15)
  src.orders p1: junk[0..2] + real k0..k9 [3..12]  (end=13)
  real record k{i} timestamp = base + i*1000 (preserved by MM2)
"""
from __future__ import annotations
import os, sys, time, subprocess

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO)
from kafka import KafkaProducer  # noqa: E402
from harness import verifier as V  # noqa: E402
from offset_migration.offsetdb import OffsetDB  # noqa: E402
from offset_migration.kafka_adapter import KafkaPythonClient  # noqa: E402
from offset_migration import core  # noqa: E402

BS = open(os.path.join(_REPO, ".cluster", "bootstrap.txt")).read().strip()
PY = sys.executable
TMP = "/tmp/opencode"
RESULTS = []


def run(cmd):
    print("    $ " + " ".join(cmd))
    r = subprocess.run(cmd, cwd=_REPO, capture_output=True, text=True)
    for line in (r.stdout or "").strip().splitlines():
        print("      " + line)
    if r.returncode != 0:
        print("      STDERR: " + (r.stderr or "").strip())
    return r


def produce(topic, partition, key, value, ts_ms):
    p = KafkaProducer(bootstrap_servers=BS.split(","), security_protocol="PLAINTEXT",
                      key_serializer=lambda k: k.encode(), value_serializer=lambda v: v.encode())
    md = p.send(topic, key=key, value=value, partition=partition, timestamp_ms=ts_ms).get(timeout=15)
    p.flush(); p.close()
    print(f"      produced {topic} p{partition} {key} @offset {md.offset} ts={ts_ms}")
    return md.offset


def wait_end(topic, partition, target, tries=15):
    for _ in range(tries):
        if V.end_offset(BS, topic, partition) >= target:
            break
        time.sleep(5)
    return V.end_offset(BS, topic, partition)


def check(name, cond, detail):
    RESULTS.append((name, bool(cond)))
    print(f"  [{'PASS' if cond else 'FAIL'}] {name} :: {detail}")


def sim(db, topic, partition, *flags):
    return run([PY, os.path.join(_REPO, "harness", "consumer_sim.py"),
                "--bootstrap", BS, "--db", db, "--cluster", "old", "--topic", topic,
                "--partition", str(partition), *flags])


def scheme_a(db):
    return run([PY, "-m", "offset_migration.migrate_by_timestamp",
                "--bootstrap", BS, "--db", db, "--partitions", "2",
                "--old-cluster", "old", "--new-cluster", "new"])


def scheme_b(db):
    return run([PY, "-m", "offset_migration.migrate_by_endoffsets",
                "--bootstrap", BS, "--db", db, "--partitions", "2",
                "--new-cluster", "new"])


def rm(db):
    try: os.remove(db)
    except FileNotFoundError: pass


# S1 — Scheme A backlog: committed=6 -> resume at last-processed k5 (reprocess), offset 10
print("\n=== S1: Scheme A BACKLOG (orders p0 committed=6) -> src.orders @10 == k5 (reprocess) ===")
db = f"{TMP}/s1.db"; rm(db)
sim(db, "orders", 0, "--upto", "6")
scheme_a(db)
d = OffsetDB(db); got = d.get("new", "src.orders", 0); d.close()
rec = V.read_at(BS, "src.orders", 0, 10)
check("S1 new_off == 10", got and got[0] == 10, f"DB(src.orders,p0)={got}")
check("S1 record@10 is k5 (reprocess-safe, no skip)", rec and rec["key"] == "k5", f"record={rec}")

# S1b — reverse-lookup (offset-only DB) -> same @10 (ts re-read live from OLD cluster)
print("\n=== S1b: Scheme A REVERSE-LOOKUP (offset-only DB, no ts) -> same @10 ===")
db = f"{TMP}/s1b.db"; rm(db)
sim(db, "orders", 0, "--upto", "6", "--no-timestamp")
scheme_a(db)
d = OffsetDB(db); got = d.get("new", "src.orders", 0); d.close()
check("S1b reverse-lookup new_off == 10", got and got[0] == 10,
      f"DB(src.orders,p0)={got} (ts re-read live from OLD cluster offset 5)")

# S2 — caught-up: last-processed k9 present on target -> reprocess @12, then read new k10
print("\n=== S2: Scheme A CAUGHT-UP (orders p1 drained) -> @12 == k9 (reprocess), then new k10 @13 ===")
db = f"{TMP}/s2.db"; rm(db)
sim(db, "orders", 1, "--caughtup")
scheme_a(db)
d = OffsetDB(db); got = d.get("new", "src.orders", 1); d.close()
rec12 = V.read_at(BS, "src.orders", 1, 12)
check("S2 new_off == 12 (reprocess last k9, no skip)",
      got and got[0] == 12 and rec12 and rec12["key"] == "k9",
      f"DB(src.orders,p1)={got}, record@12={rec12}")
ts9 = V.read_at(BS, "orders", 1, 9)["timestamp"]
produce("orders", 1, "k10", "v10", ts9 + 1000)
end_after = wait_end("src.orders", 1, 14)
rec13 = V.read_at(BS, "src.orders", 1, 13)
check("S2 resume@12 then reads new k10 @13 (no skip)",
      end_after >= 14 and rec13 and rec13["key"] == "k10",
      f"src.orders p1 end={end_after}, record@13={rec13}")

# S2b — None fallback: last_msg_ts newer than everything on target -> end_offsets (live, direct core)
print("\n=== S2b: Scheme A TARGET-BEHIND -> offsets_for_times None -> end_offsets (live) ===")
cli = KafkaPythonClient(BS)
end_p1 = V.end_offset(BS, "src.orders", 1)
future_ts = ts9 + 10_000_000
res = core.translate_timestamp(cli, "src.orders", 1, committed_offset=999, last_msg_ts=future_ts)
cli.close()
check("S2b None-fallback resolves to live end_offsets", res == end_p1,
      f"future_ts beyond target -> resume={res}, live end={end_p1}")

# S3 — Scheme B endOffsets snapshot (src.orders p0), then post-cutover produce
print("\n=== S3: Scheme B endOffsets SNAPSHOT (src.orders p0) then post-cutover produce ===")
db = f"{TMP}/s3.db"; rm(db)
end_before = V.end_offset(BS, "src.orders", 0)
scheme_b(db)
d = OffsetDB(db); snap = d.get("new", "src.orders", 0); d.close()
check("S3 snapshot == frozen end (15)", snap and snap[0] == end_before == 15,
      f"DB(src.orders,p0)={snap}, live end_before={end_before}")
now = int(time.time() * 1000)
for i, k in enumerate(["n0", "n1", "n2"]):
    produce("src.orders", 0, k, f"nv{i}", now + i)
wait_end("src.orders", 0, 18)
r15 = V.read_at(BS, "src.orders", 0, 15)
r16 = V.read_at(BS, "src.orders", 0, 16)
r17 = V.read_at(BS, "src.orders", 0, 17)
r14 = V.read_at(BS, "src.orders", 0, 14)
check("S3 consumer from snapshot reads exactly the 3 new (n0,n1,n2)",
      r15 and r15["key"] == "n0" and r16 and r16["key"] == "n1" and r17 and r17["key"] == "n2",
      f"@15={r15 and r15['key']} @16={r16 and r16['key']} @17={r17 and r17['key']}")
check("S3 nothing before snapshot re-read (offset 14 is last old k9)",
      r14 and r14["key"] == "k9", f"record@14={r14}")

# S4 — never-consumed -> beginning offset
print("\n=== S4: Scheme A NEVER-CONSUMED (offset 0, no ts) -> beginning offset ===")
db = f"{TMP}/s4.db"; rm(db)
sim(db, "orders", 0, "--upto", "0")
scheme_a(db)
d = OffsetDB(db); got = d.get("new", "src.orders", 0); d.close()
begin = V.begin_offset(BS, "src.orders", 0)
check("S4 never-consumed -> beginning offset", got and got[0] == begin,
      f"DB(src.orders,p0)={got}, begin_offset={begin}")

# S-dup — duplicate-timestamp SKIP-SAFETY (the oracle's bug) verified LIVE
print("\n=== S-dup: DUPLICATE-TIMESTAMP skip-safety (live) -> resume includes the unprocessed twin ===")
dup_ts = int(time.time() * 1000) + 50_000
o_d0 = produce("src.orders", 0, "d0", "dv0", dup_ts)
o_d1 = produce("src.orders", 0, "d1", "dv1", dup_ts)   # SAME ts as d0; treat d1 as unprocessed
wait_end("src.orders", 0, o_d1 + 1)
cli = KafkaPythonClient(BS)
resume = core.translate_timestamp(cli, "src.orders", 0, committed_offset=999, last_msg_ts=dup_ts)
skip_plus1 = cli.offsets_for_times("src.orders", 0, dup_ts + 1)
cli.close()
check("S-dup resume lands on first dup (d0) -> d1 NOT skipped",
      resume == o_d0, f"resume={resume} (d0@{o_d0}, d1@{o_d1}); last_ts+1 would give {skip_plus1} (skips d1)")

# summary
print("\n==================== SCENARIO SUMMARY ====================")
ok = sum(1 for _, c in RESULTS if c)
for name, c in RESULTS:
    print(f"  {'PASS' if c else 'FAIL'}  {name}")
print(f"  {ok}/{len(RESULTS)} checks PASSED")
sys.exit(0 if ok == len(RESULTS) else 1)
