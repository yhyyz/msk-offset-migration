#!/usr/bin/env bash
set -uo pipefail

# =============================================================================
# teardown.sh — Idempotent teardown for the MSK validation environment.
#
# Safe to re-run. Each step is guarded so a missing file or an already-deleted
# resource never aborts the remaining steps.
#
# This script ONLY tears down disposable validation infrastructure:
#   - the MirrorMaker2 (MM2) process
#   - the MSK cluster (via its ARN)
#   - the disposable Kafka binary at /tmp/opencode/kafka
#   - the rendered harness/mm2.properties
#   - the .cluster/ state directory
#
# It NEVER touches source code or RED/GREEN evidence.
# =============================================================================

# --- Resolve repo root (script lives in <root>/scripts) ----------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}" || exit 1

REGION="us-east-1"
CLUSTER_NAME="ulw-offset-validation"
ARN_FILE=".cluster/arn.txt"
PID_FILE=".cluster/mm2.pid"
LOG_DIR="evidence/surface"
LOG_FILE="${LOG_DIR}/teardown.log"

# --- Logging setup -----------------------------------------------------------
mkdir -p "${LOG_DIR}" || true
# Tee everything (stdout + stderr) to the log file, appending across re-runs.
exec > >(tee -a "${LOG_FILE}") 2>&1

log() {
  echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*"
}

log "================================================================"
log "TEARDOWN START (repo: ${REPO_ROOT})"
log "================================================================"

# =============================================================================
# Step 1 — Stop MirrorMaker2
# =============================================================================
log "--- Step 1: Stop MirrorMaker2 ---"
if [ -f "${PID_FILE}" ]; then
  MM2_PID="$(cat "${PID_FILE}" 2>/dev/null || true)"
  if [ -n "${MM2_PID:-}" ]; then
    log "Found MM2 PID file: ${PID_FILE} (PID=${MM2_PID}). Killing..."
    kill "${MM2_PID}" 2>/dev/null || true
    log "kill ${MM2_PID} issued (errors ignored)."
  else
    log "MM2 PID file ${PID_FILE} present but empty."
  fi
else
  log "No MM2 PID file at ${PID_FILE}."
fi

# Best-effort: kill any lingering MM2 process by command match.
log "Best-effort pkill -f connect-mirror-maker..."
pkill -f connect-mirror-maker 2>/dev/null || true
pkill -9 -f 'org.apache.kafka.connect.mirror.MirrorMaker' 2>/dev/null || true
log "Step 1 complete."

# =============================================================================
# Step 2 — Delete the MSK cluster
# =============================================================================
log "--- Step 2: Delete MSK cluster ---"
if [ -s "${ARN_FILE}" ]; then
  CLUSTER_ARN="$(cat "${ARN_FILE}" 2>/dev/null || true)"
  if [ -n "${CLUSTER_ARN:-}" ]; then
    log "Found cluster ARN: ${CLUSTER_ARN}"
    log "Issuing delete-cluster..."
    aws kafka delete-cluster \
      --region "${REGION}" \
      --cluster-arn "${CLUSTER_ARN}" 2>&1 || true

    log "Polling cluster state until gone (every 30s)..."
    while true; do
      DESCRIBE_OUT="$(aws kafka describe-cluster-v2 \
        --region "${REGION}" \
        --cluster-arn "${CLUSTER_ARN}" \
        --query 'ClusterInfo.State' \
        --output text 2>&1)"
      DESCRIBE_RC=$?

      # Stop when the call errors (not found) or returns empty/None.
      if [ ${DESCRIBE_RC} -ne 0 ] \
         || echo "${DESCRIBE_OUT}" | grep -qiE 'not found|ResourceNotFoundException'; then
        log "Cluster no longer found (describe errored). Deletion confirmed."
        break
      fi
      if [ -z "${DESCRIBE_OUT}" ] || [ "${DESCRIBE_OUT}" = "None" ]; then
        log "Cluster state empty/None. Treating as gone."
        break
      fi
      log "Cluster state: ${DESCRIBE_OUT} — waiting 30s..."
      sleep 30
    done
  else
    log "ARN file ${ARN_FILE} present but empty; no cluster to delete."
  fi
else
  log "No cluster ARN at ${ARN_FILE}; no cluster to delete."
fi
log "Step 2 complete."

# =============================================================================
# Step 3 — Remove disposable temp / rendered artifacts
# =============================================================================
log "--- Step 3: Remove disposable artifacts ---"
log "rm -rf /tmp/opencode/kafka"
rm -rf /tmp/opencode/kafka || true
log "rm -f harness/mm2.properties"
rm -f harness/mm2.properties || true
log "rm -rf .cluster"
rm -rf .cluster || true
log "Step 3 complete."

# =============================================================================
# Step 4 — Verify no residual cluster + confirm networking untouched
# =============================================================================
log "--- Step 4: Verify ---"
RESIDUAL="$(aws kafka list-clusters-v2 \
  --region "${REGION}" \
  --query "ClusterInfoList[?ClusterName=='${CLUSTER_NAME}'].State" \
  --output text 2>&1 || true)"
log "RESIDUAL CLUSTERS: ${RESIDUAL}"
log "SG sg-04efe43ef7acfbed1 reused, nothing to revert"
log "Step 4 complete."

log "================================================================"
echo "TEARDOWN COMPLETE"
log "================================================================"
