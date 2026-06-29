"""S5 unit tests for the pure offset-translation core (Scheme A + Scheme B).

A `MockKafkaClient` implements the `OffsetClient` protocol from in-memory
per-(topic, partition) (offset, timestamp_ms) lists, so the offset math is pinned
with zero infrastructure. The same logic runs unchanged against a live cluster.

Scheme A resolves the resume point with `offsets_for_times(last_msg_ts)` (NOT
`+1`): it lands on the first record whose ts >= last_msg_ts, so it reprocesses the
last-processed message / same-timestamp group (consumer dedups by key) and NEVER
skips an unprocessed record. `offsets_for_times` returns None only when the target
has no record at/after last_msg_ts (caught-up / target-behind) -> end_offsets.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import pytest

from offset_migration.core import translate_endoffsets, translate_timestamp

JUNK_TS = 1_577_836_800_000  # 2020-01-01Z — strictly < every real ts
BASE_TS = 1_704_067_200_000  # 2024-01-01Z
TS: List[int] = [BASE_TS + i * 1000 for i in range(10)]  # t0..t9, unique, 1s apart


class MockKafkaClient:
    def __init__(self, records: Optional[Dict[Tuple[str, int], List[Tuple[int, int]]]] = None) -> None:
        self._records: Dict[Tuple[str, int], List[Tuple[int, int]]] = {}
        for key, recs in (records or {}).items():
            self._records[key] = sorted(recs, key=lambda r: r[0])

    def _recs(self, topic: str, partition: int) -> List[Tuple[int, int]]:
        return self._records.get((topic, partition), [])

    def offsets_for_times(self, topic: str, partition: int, timestamp_ms: int) -> Optional[int]:
        candidates = [off for off, ts in self._recs(topic, partition) if ts >= timestamp_ms]
        return min(candidates) if candidates else None

    def end_offsets(self, topic: str, partition: int) -> int:
        recs = self._recs(topic, partition)
        return max(off for off, _ in recs) + 1 if recs else self.beginning_offsets(topic, partition)

    def beginning_offsets(self, topic: str, partition: int) -> int:
        recs = self._recs(topic, partition)
        return min(off for off, _ in recs) if recs else 0

    def record_ts_at(self, topic: str, partition: int, offset: int) -> Optional[int]:
        for off, ts in self._recs(topic, partition):
            if off == offset:
                return ts
        return None


def build_target_records(junk_count: int) -> List[Tuple[int, int]]:
    junk = [(off, JUNK_TS) for off in range(junk_count)]
    real = [(junk_count + i, TS[i]) for i in range(10)]
    return junk + real


# (a) BACKLOG: last processed = t5 -> resume at the t5 record itself (offset 10),
# reprocessing it; the consumer dedups. (junk N=5 -> real t_i at offset 5+i.)
def test_backlog_resumes_at_last_processed_timestamp() -> None:
    client = MockKafkaClient({("target", 0): build_target_records(5)})
    result = translate_timestamp(client, "target", 0, committed_offset=6, last_msg_ts=TS[5])
    assert result == 10


# (b1) CAUGHT-UP, last-processed record still on target -> reprocess it (offset 14).
def test_caught_up_last_msg_present_reprocesses() -> None:
    client = MockKafkaClient({("target", 0): build_target_records(5)})
    result = translate_timestamp(client, "target", 0, committed_offset=10, last_msg_ts=TS[9])
    assert result == 14


# (b2) TARGET-BEHIND: last_msg_ts newer than everything on target -> None -> end.
def test_none_fallback_to_end_offset() -> None:
    client = MockKafkaClient({("target", 0): build_target_records(5)})
    assert client.offsets_for_times("target", 0, TS[9] + 1) is None
    result = translate_timestamp(client, "target", 0, committed_offset=10, last_msg_ts=TS[9] + 1)
    assert result == 15
    assert result == client.end_offsets("target", 0)


# (c) NEVER-CONSUMED -> beginning offset.
def test_never_consumed_returns_beginning_offset() -> None:
    client = MockKafkaClient({("target", 0): build_target_records(5)})
    result = translate_timestamp(client, "target", 0, committed_offset=0, last_msg_ts=None)
    assert result == client.beginning_offsets("target", 0) == 0


# (d) OFFSET-ONLY DB -> reverse-lookup ts from OLD cluster, same outcome as backlog.
def test_offset_only_db_reverse_lookup_matches_backlog() -> None:
    new_client = MockKafkaClient({("target", 0): build_target_records(5)})
    old_client = MockKafkaClient({("source", 0): [(i, TS[i]) for i in range(6)]})
    assert old_client.record_ts_at("source", 0, 5) == TS[5]
    result = translate_timestamp(
        new_client, "target", 0, committed_offset=6, last_msg_ts=None,
        old_client=old_client, old_topic="source",
    )
    assert result == 10


def test_offset_only_db_missing_timestamp_raises() -> None:
    new_client = MockKafkaClient({("target", 0): build_target_records(5)})
    old_client = MockKafkaClient({("source", 0): []})
    with pytest.raises(ValueError):
        translate_timestamp(
            new_client, "target", 0, committed_offset=6, last_msg_ts=None,
            old_client=old_client, old_topic="source",
        )


# (e) PER-PARTITION INDEPENDENCE: same last ts, differing junk -> differing offsets.
def test_per_partition_independence() -> None:
    client = MockKafkaClient({
        ("target", 0): build_target_records(5),
        ("target", 1): build_target_records(3),
    })
    r0 = translate_timestamp(client, "target", 0, committed_offset=6, last_msg_ts=TS[5])
    r1 = translate_timestamp(client, "target", 1, committed_offset=6, last_msg_ts=TS[5])
    assert (r0, r1) == (10, 8)


# (g) DUPLICATE-TIMESTAMP SKIP-SAFETY (the bug the oracle caught): two records
# share the boundary ts; the last-processed was the first of them, the second is
# UNPROCESSED. offsets_for_times(last_ts) must land on the first (reprocess) and
# NOT skip the unprocessed twin. last_ts+1 would skip it (data loss) — asserted.
def test_duplicate_timestamp_does_not_skip_unprocessed() -> None:
    recs = [(0, JUNK_TS), (1, JUNK_TS), (2, TS[0]), (3, TS[1]), (4, TS[1]), (5, TS[2])]
    client = MockKafkaClient({("target", 0): recs})
    result = translate_timestamp(client, "target", 0, committed_offset=4, last_msg_ts=TS[1])
    assert result == 3                                       # reprocess from first TS[1]; offset 4 NOT skipped
    assert client.offsets_for_times("target", 0, TS[1] + 1) == 5  # the +1 bug would skip offset 4


# (f) Scheme B -> end offset.
def test_translate_endoffsets_returns_end_offset() -> None:
    client = MockKafkaClient({("target", 0): build_target_records(5)})
    assert translate_endoffsets(client, "target", 0) == client.end_offsets("target", 0) == 15


# (h) Adapter guard (defect #2): a compacted/retention-deleted offset makes poll
# return a LATER record; record_ts_at must reject the mismatch (return None) rather
# than silently use that record's timestamp.
def test_adapter_record_ts_at_rejects_offset_mismatch() -> None:
    from kafka import TopicPartition
    from offset_migration.kafka_adapter import KafkaPythonClient

    class _Rec:
        def __init__(self, offset: int, timestamp: int) -> None:
            self.offset = offset
            self.timestamp = timestamp

    class _FakeConsumer:
        def __init__(self, rec: "Optional[_Rec]") -> None:
            self._rec = rec

        def assign(self, tps) -> None: ...
        def seek(self, tp, offset) -> None: ...
        def poll(self, timeout_ms=0, max_records=None):
            return {TopicPartition("t", 0): ([self._rec] if self._rec else [])}
        def close(self) -> None: ...

    mismatch = KafkaPythonClient("dummy:9092")
    mismatch._consumer = _FakeConsumer(_Rec(7, 12345))
    assert mismatch.record_ts_at("t", 0, 5) is None

    exact = KafkaPythonClient("dummy:9092")
    exact._consumer = _FakeConsumer(_Rec(5, 99999))
    assert exact.record_ts_at("t", 0, 5) == 99999

    empty = KafkaPythonClient("dummy:9092")
    empty._consumer = _FakeConsumer(None)
    assert empty.record_ts_at("t", 0, 5) is None
