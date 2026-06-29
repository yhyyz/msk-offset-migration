"""Integration-test harness for the MSK offset-migration validation.

These modules drive a live MSK cluster (PLAINTEXT:9092) to seed data, simulate a
consumer's committed position in an external OffsetDB, run MirrorMaker 2, and
verify resume points. They are also importable as standalone scripts by path.
"""
