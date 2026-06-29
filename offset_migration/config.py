"""Static configuration for the MSK offset-migration validation.

All values are pinned to the live recon of the EC2/VPC this runs in so the
validation harness needs zero manual editing.
"""

# ---- AWS / MSK ----
REGION = "us-east-1"
VPC_ID = "vpc-0e1bd10042a247fdf"
SUBNET_1A = "subnet-04d4481e923cf8faa"   # us-east-1a (same subnet as this EC2)
SUBNET_1B = "subnet-056af24e77e8f8d25"   # us-east-1b
SUBNET_1C = "subnet-060dfa6695f6d2d46"   # us-east-1c
SECURITY_GROUP = "sg-04efe43ef7acfbed1"  # default SG: self-ref all-traffic ingress + all egress
KAFKA_VERSION = "3.6.0"
INSTANCE_TYPE = "kafka.t3.small"
CLUSTER_NAME = "ulw-offset-validation"

# ---- Topics / replication ----
# MM2 DefaultReplicationPolicy renames <topic> -> <alias>.<topic> on the target.
SOURCE_TOPIC = "orders"
MM2_SOURCE_ALIAS = "src"
TARGET_TOPIC = "src.orders"
PARTITIONS = 2
REAL_RECORDS_PER_PARTITION = 10

# Pre-seed the TARGET topic with junk BEFORE MM2 runs so target offsets diverge
# from source offsets (source k -> target N+k). Different N per partition proves
# per-partition independence. Junk timestamps are in the past so offsets_for_times
# on a real record's timestamp correctly skips the junk.
JUNK_PER_PARTITION = {0: 5, 1: 3}
JUNK_TS_MS = 1577836800000  # 2020-01-01T00:00:00Z

# ---- Client ----
SECURITY_PROTOCOL = "PLAINTEXT"  # validation runs over 9092; see README for IAM option
