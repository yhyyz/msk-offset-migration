#!/usr/bin/env bash
# T1 — provision the smallest viable MSK cluster for offset-migration validation.
# PLAINTEXT(9092) + unauthenticated, default SG (EC2 already in it), Kafka 3.6.0.
# Tries 1-broker/1-AZ; falls back to 2-broker/2-AZ if the API rejects single broker.
set -uo pipefail

REGION="us-east-1"
NAME="ulw-offset-validation"
SG="sg-04efe43ef7acfbed1"
VER="3.6.0"
ITYPE="kafka.t3.small"
SUBNET_1A="subnet-04d4481e923cf8faa"
SUBNET_1B="subnet-056af24e77e8f8d25"
HERE="$(cd "$(dirname "$0")/.." && pwd)"
LOG="$HERE/evidence/surface/provision.log"
mkdir -p "$HERE/evidence/surface" "$HERE/.cluster"

log(){ echo "$(date -u +%FT%TZ) $*" | tee -a "$LOG"; }
log "=== provision start ==="

create() {  # $1=nodes  $2=subnets-json
  aws kafka create-cluster --region "$REGION" \
    --cluster-name "$NAME" \
    --kafka-version "$VER" \
    --number-of-broker-nodes "$1" \
    --broker-node-group-info "{\"InstanceType\":\"$ITYPE\",\"ClientSubnets\":$2,\"SecurityGroups\":[\"$SG\"],\"StorageInfo\":{\"EbsStorageInfo\":{\"VolumeSize\":10}}}" \
    --encryption-info '{"EncryptionInTransit":{"ClientBroker":"PLAINTEXT","InCluster":false}}' \
    --client-authentication '{"Unauthenticated":{"Enabled":true}}' 2>&1
}

OUT="$(create 1 "[\"$SUBNET_1A\"]")"; log "create(1-broker): $OUT"
ARN="$(echo "$OUT" | grep -o 'arn:aws:kafka[^"]*' | head -1)"
if [ -z "$ARN" ]; then
  log "1-broker rejected -> trying 2-broker/2-AZ"
  OUT="$(create 2 "[\"$SUBNET_1A\",\"$SUBNET_1B\"]")"; log "create(2-broker): $OUT"
  ARN="$(echo "$OUT" | grep -o 'arn:aws:kafka[^"]*' | head -1)"
fi
[ -z "$ARN" ] && { log "CREATE FAILED"; exit 1; }
echo "$ARN" > "$HERE/.cluster/arn.txt"; log "ARN=$ARN"

# poll to ACTIVE
while true; do
  ST="$(aws kafka describe-cluster-v2 --region "$REGION" --cluster-arn "$ARN" --query 'ClusterInfo.State' --output text 2>&1)"
  log "state=$ST"
  case "$ST" in
    ACTIVE) break;;
    FAILED) log "CLUSTER FAILED"; exit 1;;
  esac
  sleep 30
done

BS="$(aws kafka get-bootstrap-brokers --region "$REGION" --cluster-arn "$ARN" --query 'BootstrapBrokerString' --output text 2>&1)"
echo "$BS" > "$HERE/.cluster/bootstrap.txt"
log "BOOTSTRAP=$BS"
log "=== provision done ==="
