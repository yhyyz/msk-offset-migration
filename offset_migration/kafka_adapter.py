"""Live `OffsetClient` implementation backed by kafka-python 2.3.0.

This is the thin I/O shim that lets the pure, unit-tested offset math in
``core.py`` run unchanged against a real Kafka/MSK cluster. It implements the
``OffsetClient`` protocol (see ``core.py``); it contains **no** translation logic
of its own — that lives in ``core.translate_timestamp`` / ``core.translate_endoffsets``.

Design notes
------------
* One ``KafkaConsumer`` is created lazily and reused across all queries. Lazy
  creation means merely *constructing* a ``KafkaPythonClient`` never touches the
  network — the consumer (and its bootstrap connection) is only built on the
  first offset query. That keeps ``--help`` and ``import`` cheap and offline.
* ``group_id=None`` + ``enable_auto_commit=False``: we never join a consumer
  group or commit anything; this client only *reads* offset metadata and (for the
  reverse-lookup path) a single record. The external OffsetDB is the source of
  truth for committed positions.

IAM / SASL_SSL wiring (MSK) — documented dead code, NOT enabled by default
--------------------------------------------------------------------------
The validation cluster runs PLAINTEXT on 9092, so that is the default working
path. For a production MSK cluster using IAM auth you would pass
``security_protocol="SASL_SSL"`` and supply an OAUTHBEARER token provider. The
wiring looks like this (kept as a comment so the optional dependency
``aws-msk-iam-sasl-signer`` is **not** required at import time)::

    # from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    #
    # class _MSKTokenProvider:
    #     def __init__(self, region: str) -> None:
    #         self._region = region
    #     def token(self) -> str:
    #         token, _expiry_ms = MSKAuthTokenProvider.generate_auth_token(self._region)
    #         return token
    #
    # consumer = KafkaConsumer(
    #     bootstrap_servers=bootstrap.split(","),
    #     security_protocol="SASL_SSL",
    #     sasl_mechanism="OAUTHBEARER",
    #     sasl_oauth_token_provider=_MSKTokenProvider(region="us-east-1"),
    #     group_id=None,
    #     enable_auto_commit=False,
    # )

To actually enable it, uncomment the block above (and ``pip install
aws-msk-iam-sasl-signer``) and route it through ``_build_consumer`` when
``security_protocol`` indicates IAM. The default PLAINTEXT path below is
unchanged and self-contained.
"""
from __future__ import annotations

from typing import Optional

from kafka import KafkaConsumer, TopicPartition


class KafkaPythonClient:
    """Live ``OffsetClient`` over a single reused, lazily-created KafkaConsumer.

    All methods are per ``(topic, partition)``. The consumer is built on first
    use so constructing this object is side-effect free (no network), which is
    why the CLIs can instantiate it inside ``main()`` without a live cluster
    being present at parse/help time.
    """

    def __init__(self, bootstrap: str, security_protocol: str = "PLAINTEXT") -> None:
        self.bootstrap = bootstrap
        self.security_protocol = security_protocol
        self._consumer: Optional[KafkaConsumer] = None

    # -- consumer lifecycle --------------------------------------------------
    def _build_consumer(self) -> KafkaConsumer:
        """Create the underlying KafkaConsumer (PLAINTEXT default path).

        For IAM/SASL_SSL, see the module docstring's commented token-provider
        block; that path is intentionally not wired here so the optional
        ``aws-msk-iam-sasl-signer`` package is never imported at import time.
        """
        return KafkaConsumer(
            bootstrap_servers=self.bootstrap.split(","),
            security_protocol=self.security_protocol,
            group_id=None,
            enable_auto_commit=False,
        )

    def _get_consumer(self) -> KafkaConsumer:
        if self._consumer is None:
            self._consumer = self._build_consumer()
        return self._consumer

    @staticmethod
    def _tp(topic: str, partition: int) -> TopicPartition:
        return TopicPartition(topic, partition)

    # -- OffsetClient protocol ----------------------------------------------
    def offsets_for_times(self, topic: str, partition: int, timestamp_ms: int) -> Optional[int]:
        """Earliest offset whose record timestamp >= ``timestamp_ms`` (else None).

        kafka-python returns ``{tp: OffsetAndTimestamp(offset, timestamp) | None}``;
        the value is ``None`` when no record has a timestamp >= the query (the
        "caught-up" signal Scheme A relies on).
        """
        tp = self._tp(topic, partition)
        result = self._get_consumer().offsets_for_times({tp: timestamp_ms})
        oat = result.get(tp)
        return oat.offset if oat is not None else None

    def end_offsets(self, topic: str, partition: int) -> int:
        """Log-end offset (offset the next produced record will receive)."""
        tp = self._tp(topic, partition)
        consumer = self._get_consumer()
        consumer.assign([tp])
        return consumer.end_offsets([tp])[tp]

    def beginning_offsets(self, topic: str, partition: int) -> int:
        """Earliest still-available offset."""
        tp = self._tp(topic, partition)
        consumer = self._get_consumer()
        consumer.assign([tp])
        return consumer.beginning_offsets([tp])[tp]

    def record_ts_at(self, topic: str, partition: int, offset: int) -> Optional[int]:
        """Timestamp (ms) of the record AT ``offset``, or None if unavailable.

        Assigns the partition, seeks to ``offset`` and polls for exactly one
        record. Returns its ``.timestamp`` or None if nothing came back.
        """
        tp = self._tp(topic, partition)
        consumer = self._get_consumer()
        consumer.assign([tp])
        consumer.seek(tp, offset)
        batch = consumer.poll(timeout_ms=5000, max_records=1)
        records = batch.get(tp) or []
        if not records or records[0].offset != offset:
            return None
        return records[0].timestamp

    # -- cleanup -------------------------------------------------------------
    def close(self) -> None:
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
