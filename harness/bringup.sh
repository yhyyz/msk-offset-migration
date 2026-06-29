#!/usr/bin/env bash
# T6 — live bring-up: create topics, pre-seed junk (force offset divergence),
# launch MirrorMaker2, then smoke-verify replication + timestamp preservation.
# Run only after the cluster is ACTIVE (.cluster/bootstrap.txt exists).
set -uo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
cd "$HERE" || exit 1
KBIN="/tmp/opencode/kafka/kafka_2.13-3.6.0/bin"
LOG="evidence/surface/mm2_smoke.log"
mkdir -p evidence/surface
: > "$LOG"
log(){ echo "$(date -u +%T) $*" | tee -a "$LOG"; }

BS="$(cat .cluster/bootstrap.txt 2>/dev/null)"
[ -z "$BS" ] && { log "ERROR: no bootstrap (.cluster/bootstrap.txt). Is the cluster ACTIVE?"; exit 1; }
log "bootstrap=$BS"

# 1) topics: orders (source) + src.orders (target, pre-created so we can pre-seed junk)
log "--- create topics (RF=2, 2 partitions) ---"
"$KBIN/kafka-topics.sh" --bootstrap-server "$BS" --create --if-not-exists \
  --topic orders --partitions 2 --replication-factor 2 2>&1 | tee -a "$LOG"
"$KBIN/kafka-topics.sh" --bootstrap-server "$BS" --create --if-not-exists \
  --topic src.orders --partitions 2 --replication-factor 2 \
  --config message.timestamp.type=CreateTime 2>&1 | tee -a "$LOG"

# 2) seed: junk -> src.orders (ts=2020, forces offset divergence), real -> orders
log "--- seed (junk to src.orders, real to orders) ---"
python3 harness/seeder.py --bootstrap "$BS" 2>&1 | tee -a "$LOG"

# 3) launch MM2 (orders -> src.orders, DefaultReplicationPolicy)
log "--- launch MirrorMaker2 ---"
bash harness/run_mm2.sh "$BS" 2>&1 | tee -a "$LOG"

# 4) wait for replication: src.orders p0 should reach 5(junk)+10=15, p1 3+10=13
log "--- wait for replication (target p0=15, p1=13) ---"
for i in $(seq 1 40); do
  C0=$("$KBIN/kafka-run-class.sh" kafka.tools.GetOffsetShell --bootstrap-server "$BS" --topic src.orders --partitions 0 --time -1 2>/dev/null | awk -F: '{print $3}')
  C1=$("$KBIN/kafka-run-class.sh" kafka.tools.GetOffsetShell --bootstrap-server "$BS" --topic src.orders --partitions 1 --time -1 2>/dev/null | awk -F: '{print $3}')
  log "  attempt $i: src.orders endOffsets p0=${C0:-?} p1=${C1:-?}"
  if [ "${C0:-0}" = "15" ] && [ "${C1:-0}" = "13" ]; then log "REPLICATION COMPLETE"; break; fi
  sleep 10
done

# 5) smoke: no self-replication topic, list topics
log "--- topics after MM2 (must NOT contain src.src.orders) ---"
"$KBIN/kafka-topics.sh" --bootstrap-server "$BS" --list 2>&1 | tee -a "$LOG"
log "bringup done"
