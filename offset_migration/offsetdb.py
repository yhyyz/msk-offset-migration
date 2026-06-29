"""External offset store (SQLite) standing in for the customer's "consumers keep
their committed offset in a database" setup.

Schema: one row per (topic, partition).
  committed_offset  - next offset the consumer would read (Kafka commit semantics)
  last_msg_ts       - timestamp (ms) of the LAST PROCESSED message (offset-1).
                      May be NULL for an offset-only DB (Scheme A reverse-looks-up).
"""
from __future__ import annotations

import sqlite3
from typing import List, Optional, Tuple

_DDL = """
CREATE TABLE IF NOT EXISTS offsets (
    topic            TEXT    NOT NULL,
    partition        INTEGER NOT NULL,
    committed_offset INTEGER NOT NULL,
    last_msg_ts      INTEGER,
    PRIMARY KEY (topic, partition)
);
"""


class OffsetDB:
    def __init__(self, path: str):
        self.path = path
        self._c = sqlite3.connect(path)
        self._c.execute(_DDL)
        self._c.commit()

    def upsert(self, topic: str, partition: int, committed_offset: int,
               last_msg_ts: Optional[int] = None) -> None:
        self._c.execute(
            "INSERT INTO offsets(topic,partition,committed_offset,last_msg_ts) "
            "VALUES(?,?,?,?) "
            "ON CONFLICT(topic,partition) DO UPDATE SET "
            "committed_offset=excluded.committed_offset, last_msg_ts=excluded.last_msg_ts",
            (topic, partition, committed_offset, last_msg_ts),
        )
        self._c.commit()

    def get(self, topic: str, partition: int) -> Optional[Tuple[int, Optional[int]]]:
        row = self._c.execute(
            "SELECT committed_offset,last_msg_ts FROM offsets WHERE topic=? AND partition=?",
            (topic, partition),
        ).fetchone()
        return (row[0], row[1]) if row else None

    def partitions(self, topic: str) -> List[int]:
        return [r[0] for r in self._c.execute(
            "SELECT partition FROM offsets WHERE topic=? ORDER BY partition", (topic,)
        ).fetchall()]

    def dump(self, topic: Optional[str] = None) -> List[Tuple]:
        if topic:
            return list(self._c.execute(
                "SELECT topic,partition,committed_offset,last_msg_ts FROM offsets "
                "WHERE topic=? ORDER BY partition", (topic,)))
        return list(self._c.execute(
            "SELECT topic,partition,committed_offset,last_msg_ts FROM offsets "
            "ORDER BY topic,partition"))

    def close(self) -> None:
        self._c.close()
