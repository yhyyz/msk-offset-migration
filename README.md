# MSK Consumer-Offset Migration Toolkit (MirrorMaker 2)

Two production-ready scripts to re-map **externally-stored consumer offsets** when migrating
Kafka workloads from an OLD cluster to a NEW cluster with **MirrorMaker 2** (one-way old→new).

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
| File | [`migrate_scheme_a.py`](offset_migration/migrate_scheme_a.py) | [`migrate_scheme_b.py`](offset_migration/migrate_scheme_b.py) |
| Producer downtime | **Basically none** (brief reconnect) | **Stops** for the whole drain window |
| Needs consumer drained? | No — can carry backlog | Yes — `lag=0` |
| Depends on cutover ordering? | No (order-independent) | Yes (snapshot **before** producers switch) |
| Reprocessing | ≤1 record at the boundary → **dedup by business key** | None |
| Complexity | Higher (timestamp reverse-lookup) | One line (`endOffsets`) |
| Use when | Producers can't stop / backlog is large | Short stop is OK / want the simplest, exact path |

### Scheme A algorithm (per partition)
```
1. never-consumed (committed_offset==0 & no ts)  -> beginning_offsets(new_topic)
2. offset-only DB (ts is None, offset>0)         -> ts = old.record_ts_at(old_topic, offset-1)
3. resume = new.offsets_for_times(new_topic, ts + 1)   # +1 skips the last processed msg
4. caught-up (offsets_for_times == None)         -> resume = end_offsets(new_topic)
```
The `+1` lands the consumer on the first **unprocessed** message; the boundary may re-deliver at most
one record, so **consumers must be idempotent / dedup by business key**.

### Scheme B (per partition)
Precondition (operational): producers stopped + consumer drained + MM2 drained → target frozen at `E'`.
Then `resume = end_offsets(new_topic)`. Capture `endOffsets` **before** producers switch to the new
cluster, otherwise `E'` is a moving target and records between snapshot and cutover are skipped.

## Layout
```
offset_migration/
  core.py            pure offset-translation logic (unit-tested, client-agnostic)
  kafka_adapter.py   live kafka-python OffsetClient (PLAINTEXT default; IAM documented)
  offsetdb.py        SQLite external offset store
  migrate_scheme_a.py / migrate_scheme_b.py   CLIs
  config.py          pinned constants for the validation cluster
tests/test_core.py   mocked RED→GREEN unit tests (no infra)
harness/             seeder, consumer_sim, verifier, MM2 config, bringup + scenario runners
scripts/             provision_msk.sh, teardown.sh
evidence/            red/ green/ (unit) + surface/ (live logs)
```

## OffsetDB schema
SQLite, one row per `(topic, partition)`:
| column | meaning |
|---|---|
| `committed_offset` | next offset the consumer would read (Kafka commit semantics) |
| `last_msg_ts` | timestamp (ms) of the last **processed** message (offset `committed_offset-1`); `NULL` ⇒ Scheme A reverse-looks it up from the OLD cluster |

## Usage

```bash
# Scheme A — timestamp mapping (rewrites DB offsets old→new by timestamp)
python3 -m offset_migration.migrate_scheme_a \
  --old-bootstrap OLD:9092 --new-bootstrap NEW:9092 \
  --old-topic orders --new-topic src.orders \
  --db offsets.db --partitions 2

# Scheme B — endOffsets snapshot (run inside the frozen cutover window)
python3 -m offset_migration.migrate_scheme_b \
  --new-bootstrap NEW:9092 --new-topic src.orders \
  --db offsets.db --partitions 2
```
`--partitions` accepts a COUNT (`2` → `[0,1]`) or a comma list (`0,1,3`).

### MSK IAM (production)
Validation ran over **PLAINTEXT (9092)** because auth is irrelevant to offset-translation correctness.
For a real IAM-secured MSK cluster, pass `--security-protocol SASL_SSL` and wire the
`aws-msk-iam-sasl-signer` `OAUTHBEARER` token provider (`MSKAuthTokenProvider.generate_auth_token(region)`)
— see the wiring block in [`kafka_adapter.py`](offset_migration/kafka_adapter.py).

## How this was validated (live, end-to-end)

A real **Amazon MSK** cluster (2× `kafka.t3.small`, Kafka 3.6.0, PLAINTEXT, us-east-1) was provisioned and
**real MirrorMaker 2** replicated `orders` → `src.orders` on it (`DefaultReplicationPolicy`). To make the
test non-trivial, the target was **pre-seeded with junk** (ts=2020) before MM2 ran (5 in p0, 3 in p1) — this
**forces the target offsets to diverge** from the source (`src.orders` p0 end=15, p1 end=13), so a naive
"copy the offset" implementation would fail.

**Smoke** ([`evidence/surface/mm2_smoke.log`](evidence/surface/mm2_smoke.log)): replication counts matched
(p0=15, p1=13), no self-replication loop (`src.src.orders` absent), and **timestamps were preserved**
(source `k0` ts=1782698734797 → target offset 5 same ts; `k9` → target offset 12 same ts).

**Scenarios** ([`evidence/surface/scenarios.log`](evidence/surface/scenarios.log)) — **9/9 PASS**:

| Scenario | Result |
|---|---|
| S1 Scheme A backlog (committed@6) | → `src.orders`@**11**, record there is **k6** (first unprocessed) |
| S1b Scheme A reverse-lookup (offset-only DB) | ts re-read live from OLD cluster → also @**11** |
| S2 Scheme A caught-up boundary (drained) | `offsets_for_times`→None on a real caught-up partition → `endOffsets`=**13**; new `k10` produced & MM2-replicated → consumer @13 reads exactly **k10** |
| S3 Scheme B snapshot | captured end=**15** before producing; 3 post-cutover records read exactly at 15–17; offset 14 still = last old **k9** |
| S4 never-consumed (offset 0, no ts) | → **beginning offset 0** |

Unit tests (mocked, no infra): RED → GREEN in
[`evidence/red/pytest_red.txt`](evidence/red/pytest_red.txt) /
[`evidence/green/pytest_green.txt`](evidence/green/pytest_green.txt) — 7 cases covering backlog, boundary,
never-consumed, offset-only reverse-lookup, per-partition independence, Scheme B.

## Reproduce
```bash
python3 -m pytest tests/test_core.py -q        # unit (no infra)
bash scripts/provision_msk.sh                   # ~25 min: MSK 2-broker/2-AZ, PLAINTEXT
bash harness/bringup.sh                          # topics + seed + MM2 + smoke
python3 harness/run_scenarios.py                 # S1–S4 live assertions
bash scripts/teardown.sh                         # delete cluster + clean up (mandatory)
```
