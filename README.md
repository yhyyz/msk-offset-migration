# MSK Consumer-Offset Migration Toolkit (MirrorMaker 2)

Two production-ready scripts to re-map **externally-stored consumer offsets** when migrating
Kafka workloads from an OLD cluster to a NEW cluster with **MirrorMaker 2** (one-way old→new).

> **Visual plan & deep-dive (Chinese, with diagrams):** https://dxs9dnjebzm6y.cloudfront.net/tmp/msk-migration-plan-v6.html
> — the two offset-cutover schemes, MM2 deployment guidance (incl. `sync.group.offsets`), and MSK Tiered Storage.

## The problem this solves

The customer's consumers store their committed position **per partition in an external database**
(not in Kafka's `__consumer_offsets`) and self-`seek()`. MirrorMaker 2:

- **does NOT preserve offset values** — the target topic gets its own, independent offsets, and
- **DOES preserve each record's timestamp.**

So the offsets sitting in the DB are meaningless on the new cluster and must be re-mapped at cutover.
**Neither script changes consumer code** — the consumer always does `read DB offset → seek`; the
scripts only rewrite the DB offset to the correct NEW-cluster value.

## Two schemes

| | **Scheme A — timestamp mapping** | **Scheme B — endOffsets snapshot** |
|---|---|---|
| File | [`migrate_by_timestamp.py`](offset_migration/migrate_by_timestamp.py) | [`migrate_by_endoffsets.py`](offset_migration/migrate_by_endoffsets.py) |
| Producer downtime | **Basically none** (brief reconnect) | **Stops** for the whole drain window |
| Needs consumer drained? | No — can carry backlog | Yes — `lag=0` |
| Depends on cutover ordering? | No (order-independent) | Yes (snapshot **before** producers switch) |
| Reprocessing | last-processed msg / same-ts group → **dedup by business key** (never skips) | None |
| Complexity | Higher (timestamp reverse-lookup) | One line (`endOffsets`) |
| Use when | Producers can't stop / backlog is large | Short stop is OK / want the simplest, exact path |

### Scheme A algorithm (per partition)
```
1. never-consumed (committed_offset==0 & no ts)  -> beginning_offsets(new_topic)
2. offset-only DB (ts is None, offset>0)         -> ts = old.record_ts_at(old_topic, offset-1)
3. resume = new.offsets_for_times(new_topic, ts)   # first record with ts >= last; NOT ts+1
4. target-behind (offsets_for_times == None)     -> resume = end_offsets(new_topic)
```
Step 3 uses `ts` (the last-processed timestamp), **not** `ts+1`: resume lands on the first record at
or after that timestamp, so the consumer **reprocesses** the last-processed message / same-timestamp
group and **never skips** an unprocessed record. (With `ts+1`, records sharing the boundary
millisecond would be silently skipped — data loss dedup cannot recover.) Consumers therefore **must
dedup by business key**; reprocessing is bounded by the size of the same-timestamp group.

> **Caveat:** skip-safety assumes per-partition timestamps are **non-decreasing** (the normal case for
> a single producer using `CreateTime`). If producers write wildly out-of-order timestamps within a
> partition, an unprocessed record with `ts < last_msg_ts` could still be skipped — inherent to any
> timestamp-based mapping. Also ensure the NEW topic uses `CreateTime` (not `LogAppendTime`), or MM2's
> preserved timestamps won't line up.

### Scheme B (per partition)
Precondition (operational): producers stopped + consumer drained + MM2 drained → target frozen at `E'`.
Then `resume = end_offsets(new_topic)`. Capture `endOffsets` **before** producers switch to the new
cluster, otherwise `E'` is a moving target and records between snapshot and cutover are skipped.

## Layout
```
offset_migration/
  core.py            pure offset-translation logic (unit-tested, client-agnostic)
  kafka_adapter.py   live kafka-python OffsetClient (PLAINTEXT default; IAM documented)
  offsetdb.py        SQLite external offset store (keyed by cluster_id, topic, partition)
  migrate_by_timestamp.py / migrate_by_endoffsets.py   CLIs
  config.py          pinned constants for the validation cluster
tests/test_core.py   mocked RED→GREEN unit tests (no infra)
harness/             seeder, consumer_sim, verifier, MM2 config, bringup + scenario runners
scripts/             provision_msk.sh, teardown.sh
evidence/            red/ green/ (unit) + surface/ (live logs) — gitignored, generated at runtime
```

## OffsetDB schema
SQLite, one row per `(cluster_id, topic, partition)`:
| column | meaning |
|---|---|
| `cluster_id` | which cluster this offset is for (e.g. `old` / `new`). Lets old & new offsets coexist under the **same topic name**, so the consumer never changes the topic it queries — at cutover it only flips its `cluster_id` (alongside `bootstrap.servers`). Migration is re-runnable & rollbackable (old rows are kept). |
| `committed_offset` | next offset the consumer would read (Kafka commit semantics) |
| `last_msg_ts` | timestamp (ms) of the last **processed** message (offset `committed_offset-1`); `NULL` ⇒ Scheme A reverse-looks it up from the OLD cluster |

## Usage

```bash
# Scheme A — timestamp mapping (writes NEW-cluster rows by timestamp; OLD rows kept)
# With IdentityReplicationPolicy the topic name is the SAME on both clusters;
# cluster_id (old/new) is what distinguishes the rows.
python3 -m offset_migration.migrate_by_timestamp \
  --old-bootstrap OLD:9092 --new-bootstrap NEW:9092 \
  --old-cluster old --new-cluster new \
  --old-topic orders --new-topic orders \
  --db offsets.db --partitions 2

# Scheme B — endOffsets snapshot (run inside the frozen cutover window)
python3 -m offset_migration.migrate_by_endoffsets \
  --new-bootstrap NEW:9092 --new-cluster new --new-topic orders \
  --db offsets.db --partitions 2
```
`--partitions` accepts a COUNT (`2` → `[0,1]`) or a comma list (`0,1,3`).

**Cutover** = the consumer flips its `cluster_id` (`old` → `new`) alongside `bootstrap.servers`;
its topic name and code stay unchanged. Old rows remain for rollback.

### MSK IAM (production)
Validation ran over **PLAINTEXT (9092)** because auth is irrelevant to offset-translation correctness.
For a real IAM-secured MSK cluster, pass `--security-protocol SASL_SSL` and wire the
`aws-msk-iam-sasl-signer` `OAUTHBEARER` token provider (`MSKAuthTokenProvider.generate_auth_token(region)`)
— see the wiring block in [`kafka_adapter.py`](offset_migration/kafka_adapter.py).

## How this was validated (live, end-to-end)

A real **Amazon MSK** cluster (2× `kafka.t3.small`, Kafka 3.6.0, PLAINTEXT, us-east-1) was provisioned and
**real MirrorMaker 2** replicated `orders` → `src.orders` on it (`DefaultReplicationPolicy`). To make the
test non-trivial, the target was **pre-seeded with junk** (older timestamps) before MM2 ran (5 in p0, 3 in
p1) — this **forces the target offsets to diverge** from the source (`src.orders` p0 end=15, p1 end=13), so a
naive "copy the offset" implementation would fail.

**Smoke** (`evidence/surface/mm2_smoke.log`, generated locally by the harness — gitignored): replication counts matched
(p0=15, p1=13), no self-replication loop (`src.src.*` absent), and **timestamps were preserved** (source
`k0` at offset 0 → target offset 5 with identical timestamp).

**Scenarios** (`evidence/surface/scenarios.log`, generated locally) — **11/11 PASS**:

| Scenario | Result |
|---|---|
| S1 Scheme A backlog (committed@6) | → `src.orders`@**10** = **k5** (resume at last-processed; reprocess-safe) |
| S1b Scheme A reverse-lookup (offset-only DB) | ts re-read live from OLD cluster → also @**10** |
| S2 Scheme A caught-up (drained, k9 present) | → @**12** = **k9** (reprocess last); new `k10` produced & MM2-replicated → read at @13 (no skip) |
| S2b Scheme A target-behind | `offsets_for_times`→None on a real partition → falls back to live `endOffsets` |
| S3 Scheme B snapshot | captured end=**15** before producing; 3 post-cutover records read exactly at 15–17; offset 14 still = last old **k9** |
| S4 never-consumed (offset 0, no ts) | → **beginning offset 0** |
| S-dup **duplicate-timestamp skip-safety** | two records share a timestamp; resume lands on the **first** → the unprocessed twin is **NOT skipped** (`ts+1` would skip it) |

Unit tests (mocked, no infra): RED → GREEN (logs `evidence/red/pytest_red.txt` / `evidence/green/pytest_green.txt`, generated locally) — **9 cases**: backlog, caught-up
(reprocess), target-behind None→end, never-consumed, offset-only reverse-lookup, per-partition
independence, **duplicate-timestamp skip-safety**, missing-ts error, Scheme B.

## Reproduce
```bash
python3 -m pytest tests/test_core.py -q        # unit (no infra)
bash scripts/provision_msk.sh                   # ~25 min: MSK 2-broker/2-AZ, PLAINTEXT
bash harness/bringup.sh                          # topics + seed + MM2 + smoke
python3 harness/run_scenarios.py                 # S1–S4 live assertions
bash scripts/teardown.sh                         # delete cluster + clean up (mandatory)
```
