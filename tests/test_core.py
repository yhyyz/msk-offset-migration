"""S5 unit tests for the pure offset-translation core (Scheme A + Scheme B).

These tests pin down the offset math with zero infrastructure: a `MockKafkaClient`
implements the `OffsetClient` protocol from in-memory per-(topic, partition) record
lists of (offset, timestamp_ms). The same logic runs unchanged against a live
cluster via the real adapter (written separately).

kafka-python parity note: `offsets_for_times` returns None for a partition when no
record has ts >= the query timestamp (the "caught-up" signal Scheme A relies on);
the mock mirrors that exactly.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import pytest

from offset_migration.core import translate_endoffsets, translate_timestamp


# --- timestamps -------------------------------------------------------------
# Junk (pre-migration / replayed) records carry an old-year timestamp; the real
# post-migration records carry 2024 timestamps, strictly increasing and spaced
# 1s apart so every comparison below is unambiguous.
JUNK_TS = 1_577_836_800_000  # 2020-01-01T00:00:00Z (ms) — strictly < every real ts
BASE_TS = 1_704_067_200_000  # 2024-01-01T00:00:00Z (ms)
TS: List[int] = [BASE_TS + i * 1000 for i in range(10)]  # t0..t9


class MockKafkaClient:
    """In-memory `OffsetClient` for tests.

    Holds per-(topic, partition) lists of (offset, timestamp_ms) records and
    answers the four offset queries the core needs, matching kafka-python
    semantics.
    """

    def __init__(self, records: Optional[Dict[Tuple[str, int], List[Tuple[int, int]]]] = None) -> None:
        self._records: Dict[Tuple[str, int], List[Tuple[int, int]]] = {}
        for key, recs in (records or {}).items():
            self._records[key] = sorted(recs, key=lambda r: r[0])

    def _recs(self, topic: str, partition: int) -> List[Tuple[int, int]]:
        return self._records.get((topic, partition), [])

    def offsets_for_times(self, topic: str, partition: int, timestamp_ms: int) -> Optional[int]:
        # Earliest offset whose record ts >= query; None if no such record (caught-up).
        candidates = [off for off, ts in self._recs(topic, partition) if ts >= timestamp_ms]
        return min(candidates) if candidates else None

    def end_offsets(self, topic: str, partition: int) -> int:
        recs = self._recs(topic, partition)
        if not recs:
            return self.beginning_offsets(topic, partition)
        return max(off for off, _ in recs) + 1

    def beginning_offsets(self, topic: str, partition: int) -> int:
        recs = self._recs(topic, partition)
        if not recs:
            return 0
        return min(off for off, _ in recs)

    def record_ts_at(self, topic: str, partition: int, offset: int) -> Optional[int]:
        for off, ts in self._recs(topic, partition):
            if off == offset:
                return ts
        return None


def build_target_records(junk_count: int) -> List[Tuple[int, int]]:
    """Target partition layout: `junk_count` junk records (ts=JUNK_TS) at the head,
    then 10 real records carrying TS[0..9].

    With junk_count junk records, the real message of logical index i sits at
    target offset (junk_count + i).
    """
    junk = [(off, JUNK_TS) for off in range(junk_count)]
    real = [(junk_count + i, TS[i]) for i in range(10)]
    return junk + real


# --- (a) BACKLOG ------------------------------------------------------------
def test_backlog_resumes_at_first_unprocessed_by_timestamp() -> None:
    # junk N=5 -> real msgs at offsets 5..14 carrying TS[0..9].
    client = MockKafkaClient({("target", 0): build_target_records(5)})
    # Last processed real msg has ts t5 (logical index 5, target offset 10).
    # Resume = first real msg with ts >= t5+1 = logical index 6 -> target offset 11.
    result = translate_timestamp(
        client, "target", 0, committed_offset=6, last_msg_ts=TS[5]
    )
    assert result == 11


# --- (b) CAUGHT-UP BOUNDARY -------------------------------------------------
def test_caught_up_boundary_returns_end_offset() -> None:
    client = MockKafkaClient({("target", 0): build_target_records(5)})
    # last_msg_ts is the latest real ts -> no record has ts >= t9+1 -> None.
    assert client.offsets_for_times("target", 0, TS[9] + 1) is None
    result = translate_timestamp(
        client, "target", 0, committed_offset=14, last_msg_ts=TS[9]
    )
    assert result == 15
    assert result == client.end_offsets("target", 0)


# --- (c) NEVER-CONSUMED -----------------------------------------------------
def test_never_consumed_returns_beginning_offset() -> None:
    client = MockKafkaClient({("target", 0): build_target_records(5)})
    result = translate_timestamp(
        client, "target", 0, committed_offset=0, last_msg_ts=None
    )
    assert result == client.beginning_offsets("target", 0)
    assert result == 0


# --- (d) OFFSET-ONLY DB (reverse lookup on OLD cluster) ---------------------
def test_offset_only_db_reverse_lookup_matches_backlog() -> None:
    new_client = MockKafkaClient({("target", 0): build_target_records(5)})
    # On the OLD cluster, the last-processed msg sits at offset committed_offset-1=5
    # and carries ts t5; record_ts_at(source, 0, 5) -> t5.
    old_client = MockKafkaClient({("source", 0): [(i, TS[i]) for i in range(6)]})
    assert old_client.record_ts_at("source", 0, 5) == TS[5]
    result = translate_timestamp(
        new_client,
        "target",
        0,
        committed_offset=6,
        last_msg_ts=None,
        old_client=old_client,
        old_topic="source",
    )
    # Same outcome as the BACKLOG case (a).
    assert result == 11


def test_offset_only_db_missing_timestamp_raises() -> None:
    new_client = MockKafkaClient({("target", 0): build_target_records(5)})
    # OLD cluster has no record at offset 5 -> record_ts_at returns None -> ValueError.
    old_client = MockKafkaClient({("source", 0): []})
    with pytest.raises(ValueError):
        translate_timestamp(
            new_client,
            "target",
            0,
            committed_offset=6,
            last_msg_ts=None,
            old_client=old_client,
            old_topic="source",
        )


# --- (e) PER-PARTITION INDEPENDENCE -----------------------------------------
def test_per_partition_independence() -> None:
    client = MockKafkaClient(
        {
            ("target", 0): build_target_records(5),  # junk N=5 -> logical 6 at offset 11
            ("target", 1): build_target_records(3),  # junk N=3 -> logical 6 at offset 9
        }
    )
    # Same last-processed timestamp (t5) on both partitions; differing junk counts
    # yield differing target offsets for the same logical resume point.
    r0 = translate_timestamp(client, "target", 0, committed_offset=6, last_msg_ts=TS[5])
    r1 = translate_timestamp(client, "target", 1, committed_offset=6, last_msg_ts=TS[5])
    assert r0 == 11
    assert r1 == 9
    assert r0 != r1


# --- (f) Scheme B -----------------------------------------------------------
def test_translate_endoffsets_returns_end_offset() -> None:
    client = MockKafkaClient({("target", 0): build_target_records(5)})
    assert translate_endoffsets(client, "target", 0) == 15
    assert translate_endoffsets(client, "target", 0) == client.end_offsets("target", 0)
