"""External offset store (SQLite) standing in for the customer's "consumers keep
their committed offset in a database" setup.

Schema: one row per (cluster_id, topic, partition).
  cluster_id        - which cluster this offset belongs to (e.g. 'old' / 'new', or
                      a cluster alias / bootstrap id). Lets old & new offsets coexist
                      under the SAME topic name, so the consumer never changes the
                      topic it queries — at cutover it only flips its cluster_id
                      (alongside bootstrap). Migration is re-runnable and rollbackable.
  committed_offset  - next offset the consumer would read (Kafka commit semantics)
  last_msg_ts       - timestamp (ms) of the LAST PROCESSED message (offset-1).
                      May be NULL for an offset-only DB (Scheme A reverse-looks-up).
"""
from __future__ import annotations

import sqlite3
from typing import List, Optional, Tuple

_DDL = """
CREATE TABLE IF NOT EXISTS offsets (
    cluster_id       TEXT    NOT NULL,
    topic            TEXT    NOT NULL,
    partition        INTEGER NOT NULL,
    committed_offset INTEGER NOT NULL,
    last_msg_ts      INTEGER,
    PRIMARY KEY (cluster_id, topic, partition)
);
"""


class OffsetDB:
    def __init__(self, path: str):
        self.path = path
        self._c = sqlite3.connect(path)
        self._c.execute(_DDL)
        self._c.commit()

    def upsert(self, cluster_id: str, topic: str, partition: int, committed_offset: int,
               last_msg_ts: Optional[int] = None) -> None:
        self._c.execute(
            "INSERT INTO offsets(cluster_id,topic,partition,committed_offset,last_msg_ts) "
            "VALUES(?,?,?,?,?) "
            "ON CONFLICT(cluster_id,topic,partition) DO UPDATE SET "
            "committed_offset=excluded.committed_offset, last_msg_ts=excluded.last_msg_ts",
            (cluster_id, topic, partition, committed_offset, last_msg_ts),
        )
        self._c.commit()

    def get(self, cluster_id: str, topic: str,
            partition: int) -> Optional[Tuple[int, Optional[int]]]:
        row = self._c.execute(
            "SELECT committed_offset,last_msg_ts FROM offsets "
            "WHERE cluster_id=? AND topic=? AND partition=?",
            (cluster_id, topic, partition),
        ).fetchone()
        return (row[0], row[1]) if row else None

    def partitions(self, cluster_id: str, topic: str) -> List[int]:
        return [r[0] for r in self._c.execute(
            "SELECT partition FROM offsets WHERE cluster_id=? AND topic=? ORDER BY partition",
            (cluster_id, topic),
        ).fetchall()]

    def dump(self, cluster_id: Optional[str] = None, topic: Optional[str] = None) -> "List[Tuple[object, ...]]":
        sql = ("SELECT cluster_id,topic,partition,committed_offset,last_msg_ts "
               "FROM offsets")
        conds, args = [], []
        if cluster_id is not None:
            conds.append("cluster_id=?"); args.append(cluster_id)
        if topic is not None:
            conds.append("topic=?"); args.append(topic)
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        sql += " ORDER BY cluster_id,topic,partition"
        return list(self._c.execute(sql, tuple(args)))

    def close(self) -> None:
        self._c.close()
