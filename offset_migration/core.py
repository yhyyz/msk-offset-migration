"""Core offset-translation logic (pure, client-agnostic).

This module is the crux of both migration schemes. It is deliberately decoupled
from any Kafka client: it talks to an `OffsetClient` (see protocol below), which
is implemented for real by `kafka_adapter.KafkaPythonClient` and for tests by a
`MockKafkaClient`. That lets the subtle offset math be unit-tested with zero
infrastructure (RED->GREEN) and then exercised unchanged against a live cluster.

Background
----------
When migrating with MirrorMaker 2 (one-way old->new):
  * MM2 does NOT preserve offset values — the target topic has its OWN offsets.
  * MM2 DOES preserve the original record timestamp.
  * The customer's consumers store their committed position PER PARTITION in an
    external DB and self-seek; those stored offsets are meaningless on the new
    cluster and must be re-mapped at cutover.

Scheme A (timestamp mapping) — `translate_timestamp`
  Order-independent; producers basically don't stop. Resolve the new-cluster
  offset from the timestamp of the last-processed message.

Scheme B (endOffsets snapshot) — `translate_endoffsets`
  Requires producers stopped + consumer drained + MM2 drained (target frozen at
  E'); then the resume point is simply the target end offset.
"""
from __future__ import annotations

from typing import Optional, Protocol


class OffsetClient(Protocol):
    """Kafka operations needed for offset translation.

    Implemented by `kafka_adapter.KafkaPythonClient` (live) and `MockKafkaClient`
    (tests). All methods are per (topic, partition).
    """

    def offsets_for_times(self, topic: str, partition: int, timestamp_ms: int) -> Optional[int]:
        """Earliest offset whose record timestamp >= `timestamp_ms`.

        Returns None if no such message exists yet (the timestamp is at/after the
        current log end) — this is the "caught-up" signal Scheme A relies on.
        """
        ...

    def end_offsets(self, topic: str, partition: int) -> int:
        """Log-end offset (the offset the next produced record will get)."""
        ...

    def beginning_offsets(self, topic: str, partition: int) -> int:
        """Earliest still-available offset."""
        ...

    def record_ts_at(self, topic: str, partition: int, offset: int) -> Optional[int]:
        """Timestamp (ms) of the record AT `offset`, or None if unavailable.

        Used only by Scheme A's reverse-lookup path when the external DB stored an
        offset but no timestamp: we read it from the OLD cluster.
        """
        ...


def translate_timestamp(
    new_client: "OffsetClient",
    new_topic: str,
    partition: int,
    committed_offset: int,
    last_msg_ts: Optional[int],
    *,
    old_client: "Optional[OffsetClient]" = None,
    old_topic: Optional[str] = None,
) -> int:
    """Scheme A. Return the NEW-cluster resume offset for one partition.

    Algorithm (unified — handles backlog, caught-up boundary, never-consumed,
    and offset-only DB):

        1. never-consumed: committed_offset == 0 AND last_msg_ts is None
           -> return new_client.beginning_offsets(new_topic, partition)

        2. offset-only DB (last_msg_ts is None, committed_offset > 0):
           reverse-look-up the timestamp of the last PROCESSED message on the OLD
           cluster: last_msg_ts = old_client.record_ts_at(old_topic, partition,
           committed_offset - 1)

        3. resume = new_client.offsets_for_times(new_topic, partition,
                                                 last_msg_ts + 1)
           The "+1" skips the already-processed last message and lands on the
           first UNprocessed one. (Boundary reprocess is <=1 record; consumers
           must dedup by business key — documented in README.)

        4. caught-up boundary: if offsets_for_times returned None (no record with
           ts > last_msg_ts yet) -> resume = new_client.end_offsets(new_topic,
           partition). The next replicated/produced record lands there and is read
           exactly once.

    Returns the integer offset to write back to the external DB.
    """
    # 1. never-consumed: nothing committed and no timestamp -> start from the head.
    if committed_offset == 0 and last_msg_ts is None:
        return new_client.beginning_offsets(new_topic, partition)

    # 2. offset-only DB: no stored timestamp, but a real committed offset exists.
    #    Reverse-look-up the timestamp of the last PROCESSED message on the OLD
    #    cluster (at committed_offset - 1).
    if last_msg_ts is None:
        if old_client is None or old_topic is None:
            raise ValueError(
                "offset-only DB path requires old_client and old_topic to reverse-look-up the last-processed timestamp"
            )
        last_msg_ts = old_client.record_ts_at(old_topic, partition, committed_offset - 1)
        if last_msg_ts is None:
            raise ValueError(
                f"could not reverse-look-up timestamp at old offset {committed_offset - 1} for {old_topic}[{partition}]: record unavailable on the old cluster"
            )

    # 3. resume at the first UNprocessed message (+1 skips the last processed one).
    resume = new_client.offsets_for_times(new_topic, partition, last_msg_ts + 1)

    # 4. caught-up boundary: no record with ts > last_msg_ts yet -> the log end is
    #    the resume point, so the next replicated/produced record is read once.
    if resume is None:
        resume = new_client.end_offsets(new_topic, partition)

    return resume


def translate_endoffsets(new_client: "OffsetClient", new_topic: str, partition: int) -> int:
    """Scheme B. Return the NEW-cluster resume offset = target end offset.

    Precondition (caller's responsibility, enforced operationally, not here):
    producers stopped, consumer drained to lag=0, MM2 drained — so the target is
    frozen at E'. The resume point is then simply the target end offset; the next
    post-cutover record lands at E' and is consumed with no gap and no re-read.
    """
    return new_client.end_offsets(new_topic, partition)
