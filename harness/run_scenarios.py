#!/usr/bin/env python3
"""T7 — live scenario validation against the real MSK cluster + MM2.

Runs the actual deliverable CLIs (consumer_sim + migrate_scheme_a/b) end-to-end
and asserts the resume point by reading the real record at the resulting offset.

Topology produced by harness/bringup.sh on this cluster:
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
        e = V.end_offset(BS, topic, partition)
        if e >= target:
            return e
        time.sleep(5)
    return V.end_offset(BS, topic, partition)


def check(name, cond, detail):
    RESULTS.append((name, bool(cond)))
    print(f"  [{'PASS' if cond else 'FAIL'}] {name} :: {detail}")


def sim(db, topic, partition, *flags):
    return run([PY, os.path.join(_REPO, "harness", "consumer_sim.py"),
                "--bootstrap", BS, "--db", db, "--topic", topic,
                "--partition", str(partition), *flags])


def scheme_a(db):
    return run([PY, "-m", "offset_migration.migrate_scheme_a",
                "--bootstrap", BS, "--db", db, "--partitions", "2"])


def scheme_b(db):
    return run([PY, "-m", "offset_migration.migrate_scheme_b",
                "--bootstrap", BS, "--db", db, "--partitions", "2"])


def rm(db):
    try: os.remove(db)
    except FileNotFoundError: pass


# ---------------------------------------------------------------- S1
print("\n=== S1: Scheme A BACKLOG (orders p0, committed=6) -> src.orders @11 == k6 ===")
db = f"{TMP}/s1.db"; rm(db)
sim(db, "orders", 0, "--upto", "6")
scheme_a(db)
d = OffsetDB(db); got = d.get("src.orders", 0); d.close()
rec = V.read_at(BS, "src.orders", 0, 11)
check("S1 new_off == 11", got and got[0] == 11, f"DB(src.orders,p0)={got}")
check("S1 record@11 is k6", rec and rec["key"] == "k6", f"record={rec}")

# ---------------------------------------------------------------- S1b
print("\n=== S1b: Scheme A REVERSE-LOOKUP (offset-only DB, no ts) -> same @11 ===")
db = f"{TMP}/s1b.db"; rm(db)
sim(db, "orders", 0, "--upto", "6", "--no-timestamp")
scheme_a(db)
d = OffsetDB(db); got = d.get("src.orders", 0); d.close()
check("S1b reverse-lookup new_off == 11", got and got[0] == 11,
      f"DB(src.orders,p0)={got} (ts re-read live from OLD cluster offset 5)")

# ---------------------------------------------------------------- S2
print("\n=== S2: Scheme A CAUGHT-UP boundary (orders p1 drained) -> endOffset 13, then read new ===")
db = f"{TMP}/s2.db"; rm(db)
sim(db, "orders", 1, "--caughtup")
scheme_a(db)
d = OffsetDB(db); got = d.get("src.orders", 1); d.close()
check("S2 boundary new_off == 13 (end fallback)", got and got[0] == 13, f"DB(src.orders,p1)={got}")
# produce one NEW source record (ts strictly after k9), let MM2 replicate, consumer reads exactly it
ts9 = V.read_at(BS, "orders", 1, 9)["timestamp"]
produce("orders", 1, "k10", "v10", ts9 + 1000)
end_after = wait_end("src.orders", 1, 14)
rec13 = V.read_at(BS, "src.orders", 1, 13)
check("S2 resume@13 reads the new k10 (no skip/dup)",
      end_after >= 14 and rec13 and rec13["key"] == "k10",
      f"src.orders p1 end={end_after}, record@13={rec13}")

# ---------------------------------------------------------------- S3
print("\n=== S3: Scheme B endOffsets SNAPSHOT (src.orders p0) then post-cutover produce ===")
db = f"{TMP}/s3.db"; rm(db)
end_before = V.end_offset(BS, "src.orders", 0)        # expect 15 (frozen)
scheme_b(db)                                          # captures BEFORE producing new
d = OffsetDB(db); snap = d.get("src.orders", 0); d.close()
check("S3 snapshot == frozen end (15)", snap and snap[0] == end_before == 15,
      f"DB(src.orders,p0)={snap}, live end_before={end_before}")
now = int(time.time() * 1000)
for i, k in enumerate(["n0", "n1", "n2"]):
    produce("src.orders", 0, k, f"nv{i}", now + i)    # direct to NEW cluster (post-cutover)
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

# ---------------------------------------------------------------- S4
print("\n=== S4: Scheme A NEVER-CONSUMED (offset 0, no ts) -> beginning offset ===")
db = f"{TMP}/s4.db"; rm(db)
sim(db, "orders", 0, "--upto", "0")
scheme_a(db)
d = OffsetDB(db); got = d.get("src.orders", 0); d.close()
begin = V.begin_offset(BS, "src.orders", 0)
check("S4 never-consumed -> beginning offset", got and got[0] == begin,
      f"DB(src.orders,p0)={got}, begin_offset={begin}")

# ---------------------------------------------------------------- summary
print("\n==================== SCENARIO SUMMARY ====================")
ok = sum(1 for _, c in RESULTS if c)
for name, c in RESULTS:
    print(f"  {'PASS' if c else 'FAIL'}  {name}")
print(f"  {ok}/{len(RESULTS)} checks PASSED")
sys.exit(0 if ok == len(RESULTS) else 1)
