#!/usr/bin/env bash
# Render harness/mm2.properties from the template and launch MirrorMaker 2 in the
# background against the validation cluster.
#
# Usage:
#   harness/run_mm2.sh [BOOTSTRAP]
#
# BOOTSTRAP comes from arg1 if given, else .cluster/bootstrap.txt.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

KAFKA_HOME="/tmp/opencode/kafka/kafka_2.13-3.6.0"
MM2_BIN="${KAFKA_HOME}/bin/connect-mirror-maker.sh"
TMPL="${SCRIPT_DIR}/mm2.properties.tmpl"
RENDERED="${SCRIPT_DIR}/mm2.properties"
BOOTSTRAP_FILE="${REPO_ROOT}/.cluster/bootstrap.txt"
PID_FILE="${REPO_ROOT}/.cluster/mm2.pid"
LOG_FILE="${REPO_ROOT}/evidence/surface/mm2.log"

# --- Resolve bootstrap ---
if [[ $# -ge 1 && -n "${1:-}" ]]; then
  BOOTSTRAP="$1"
else
  if [[ ! -f "$BOOTSTRAP_FILE" ]]; then
    echo "ERROR: no bootstrap arg and missing $BOOTSTRAP_FILE" >&2
    exit 1
  fi
  BOOTSTRAP="$(tr -d '[:space:]' < "$BOOTSTRAP_FILE")"
fi

if [[ -z "$BOOTSTRAP" ]]; then
  echo "ERROR: empty bootstrap value" >&2
  exit 1
fi

# --- Preflight ---
if [[ ! -f "$TMPL" ]]; then
  echo "ERROR: missing template $TMPL" >&2
  exit 1
fi
if [[ ! -x "$MM2_BIN" ]]; then
  echo "ERROR: connect-mirror-maker.sh not found/executable at $MM2_BIN" >&2
  exit 1
fi

mkdir -p "${REPO_ROOT}/.cluster" "${REPO_ROOT}/evidence/surface"

# --- Render template (| delimiter; bootstrap has no '|') ---
sed "s|{{BOOTSTRAP}}|${BOOTSTRAP}|g" "$TMPL" > "$RENDERED"

if grep -q '{{' "$RENDERED"; then
  echo "ERROR: unsubstituted placeholder remains in $RENDERED" >&2
  exit 1
fi
echo "Rendered $RENDERED (bootstrap=${BOOTSTRAP})"

# --- Launch MM2 in the background ---
nohup "$MM2_BIN" "$RENDERED" > "$LOG_FILE" 2>&1 &
MM2_PID=$!
echo "$MM2_PID" > "$PID_FILE"

echo "MM2 started pid=${MM2_PID}"
echo "  log=${LOG_FILE}"
echo "  pidfile=${PID_FILE}"
