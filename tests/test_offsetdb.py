"""Unit tests for the OffsetDB cluster_id dimension.

The point of cluster_id: old & new offsets coexist under the SAME topic name, so
the consumer never changes the topic it queries (it only flips its cluster_id at
cutover), migration is re-runnable, and rollback stays possible.
"""
from offset_migration.offsetdb import OffsetDB


def test_old_and_new_coexist_same_topic_partition(tmp_path) -> None:
    db = OffsetDB(str(tmp_path / "o.db"))
    try:
        db.upsert("old", "orders", 0, 6, 111)
        db.upsert("new", "orders", 0, 10, 111)          # same (topic,partition), different cluster
        assert db.get("old", "orders", 0) == (6, 111)   # old row NOT overwritten by the new one
        assert db.get("new", "orders", 0) == (10, 111)
    finally:
        db.close()


def test_upsert_updates_within_same_cluster(tmp_path) -> None:
    db = OffsetDB(str(tmp_path / "o.db"))
    try:
        db.upsert("old", "orders", 1, 5)
        db.upsert("old", "orders", 1, 9, 222)
        assert db.get("old", "orders", 1) == (9, 222)
        assert db.get("old", "orders", 0) is None
    finally:
        db.close()


def test_partitions_and_dump_filter_by_cluster(tmp_path) -> None:
    db = OffsetDB(str(tmp_path / "o.db"))
    try:
        db.upsert("old", "orders", 0, 1)
        db.upsert("old", "orders", 1, 2)
        db.upsert("new", "orders", 0, 3)
        assert db.partitions("old", "orders") == [0, 1]
        assert db.partitions("new", "orders") == [0]
        assert len(db.dump("old")) == 2
        assert len(db.dump("new")) == 1
        assert len(db.dump()) == 3
    finally:
        db.close()
