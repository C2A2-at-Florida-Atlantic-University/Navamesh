"""Tests for the cloud-sync retry queue and its failure handling.

These exercise CloudSyncWorker._flush() directly (no background thread) against a
real on-disk SQLite CloudSyncQueue and Mock cloud writers. A duck-typed
FakeApiException stands in for influxdb_client's ApiException so the tests do not
require influxdb-client to be installed.
"""
import json

import pytest
from unittest.mock import Mock

from navamesh.mqtt_to_db import CloudSyncQueue, CloudSyncWorker
import navamesh.mqtt_to_db as m


class FakeApiException(Exception):
    """Mimics influxdb_client.rest.ApiException (duck-typed on .status/.body)."""

    def __init__(self, status, body="", reason=""):
        super().__init__(f"HTTP {status}: {body}")
        self.status = status
        self.body = body
        self.reason = reason


RETENTION_BODY = (
    "observed timestamp 2026-05-18T19:49:01Z is outside the retention period; "
    "minimum acceptable timestamp is 2026-05-20T00:00:00Z"
)


@pytest.fixture
def queue(tmp_path):
    q = CloudSyncQueue(str(tmp_path / "q.db"))
    yield q
    q.close()


def _worker(queue, influx=None, pg=None):
    return CloudSyncWorker(
        queue=queue,
        pg_cloud=pg or Mock(),
        influx_cloud=influx or Mock(),
        interval=30,
    )


def _insert_raw(queue, target, payload_str, queued_at=123):
    """Insert a row with an arbitrary (possibly malformed) payload string."""
    with queue._lock:
        queue._conn.execute(
            "INSERT INTO sync_queue (queued_at, target, payload, attempts) "
            "VALUES (?, ?, ?, 0)",
            (queued_at, target, payload_str),
        )
        queue._conn.commit()


def test_stale_retention_error_is_removed_and_subsequent_entries_flushed(queue):
    influx = Mock()
    # Head row fails with retention 400; second row succeeds.
    influx.write_soil.side_effect = [FakeApiException(400, RETENTION_BODY), None]
    queue.enqueue("influx", {"node_id": "!a", "soil_raw": 1})
    queue.enqueue("influx", {"node_id": "!b", "soil_raw": 2})

    _worker(queue, influx=influx)._flush()

    assert queue.size() == 0                  # both rows cleared
    assert queue.count_dead_letters() == 0    # retention row dropped, not dead-lettered
    assert influx.write_soil.call_count == 2  # processing continued past the bad head


def test_transient_error_remains_queued_and_stops_flush(queue):
    influx = Mock()
    influx.write_soil.side_effect = ConnectionError("cloud unreachable")
    queue.enqueue("influx", {"node_id": "!a"})
    queue.enqueue("influx", {"node_id": "!b"})

    _worker(queue, influx=influx)._flush()

    assert queue.size() == 2                  # nothing lost
    assert queue.count_dead_letters() == 0
    assert influx.write_soil.call_count == 1  # FIFO preserved: stopped at the head


def test_unrelated_permanent_error_is_dead_lettered(queue):
    influx = Mock()
    influx.write_soil.side_effect = [FakeApiException(422, "unparseable line protocol"), None]
    queue.enqueue("influx", {"node_id": "!a"})
    queue.enqueue("influx", {"node_id": "!b"})

    _worker(queue, influx=influx)._flush()

    assert queue.size() == 0
    assert queue.count_dead_letters() == 1    # bad row preserved for inspection
    assert influx.write_soil.call_count == 2  # second row still flushed


def test_malformed_payload_is_dead_lettered(queue):
    influx = Mock()
    _insert_raw(queue, "influx", "{not valid json")
    queue.enqueue("influx", {"node_id": "!b"})

    _worker(queue, influx=influx)._flush()

    assert queue.size() == 0
    assert queue.count_dead_letters() == 1
    influx.write_soil.assert_called_once()    # corrupt row didn't block the valid one


def test_unknown_target_is_dead_lettered(queue):
    influx = Mock()
    queue.enqueue("bogus_target", {"node_id": "!a"})
    queue.enqueue("influx", {"node_id": "!b"})

    _worker(queue, influx=influx)._flush()

    assert queue.size() == 0
    assert queue.count_dead_letters() == 1
    influx.write_soil.assert_called_once()    # not silently marked as flushed


def test_dead_letter_preserves_queued_at(queue):
    influx = Mock()
    influx.write_soil.side_effect = FakeApiException(422, "bad")
    _insert_raw(queue, "influx", json.dumps({"node_id": "!a"}), queued_at=999)

    _worker(queue, influx=influx)._flush()

    rows = queue.list_dead_letters()
    assert len(rows) == 1
    # list_dead_letters -> (id, orig_id, queued_at, failed_at, target, error, payload)
    assert rows[0][2] == 999                  # queued_at preserved via the API
    with queue._lock:
        queued_at = queue._conn.execute(
            "SELECT queued_at FROM sync_dead_letter"
        ).fetchone()[0]
    assert queued_at == 999                   # original timestamp preserved atomically


def _dead_letter_error(queue):
    return queue.list_dead_letters()[0][5]    # error column


def test_error_diagnostics_are_redacted_and_truncated(queue):
    influx = Mock()
    long_body = "token=SECRET123 " + ("x" * 5000)
    influx.write_soil.side_effect = FakeApiException(422, long_body)
    queue.enqueue("influx", {"node_id": "!a"})

    _worker(queue, influx=influx)._flush()

    error = _dead_letter_error(queue)
    assert "SECRET123" not in error
    assert "token=***" in error
    assert "truncated" in error


def test_secret_in_reason_is_redacted(queue):
    influx = Mock()
    influx.write_soil.side_effect = FakeApiException(
        422, "bad request", reason="token=REASONSECRET"
    )
    queue.enqueue("influx", {"node_id": "!a"})

    _worker(queue, influx=influx)._flush()

    error = _dead_letter_error(queue)
    assert "REASONSECRET" not in error
    assert "token=***" in error


def test_bearer_authorization_header_is_fully_redacted(queue):
    influx = Mock()
    influx.write_soil.side_effect = FakeApiException(
        401, "Authorization: Bearer BEARERSECRET\nunauthorized"
    )
    queue.enqueue("influx", {"node_id": "!a"})

    _worker(queue, influx=influx)._flush()

    error = _dead_letter_error(queue)
    assert "BEARERSECRET" not in error        # the header value must not survive
    assert "unauthorized" in error            # surrounding context is preserved


def test_dsn_password_component_is_redacted(queue):
    pg = Mock()
    pg.upsert_node.side_effect = FakeApiException(
        400, "could not connect to postgresql://navamesh:DSNSECRET@cloud.example:5432/db"
    )
    queue.enqueue("pg", {"node_id": "!a"})

    _worker(queue, pg=pg)._flush()

    error = _dead_letter_error(queue)
    assert "DSNSECRET" not in error
    assert "navamesh:***@cloud.example" in error


def test_writer_unavailable_keeps_items_queued(queue):
    influx = Mock()
    influx.connected = False                  # destination is down
    queue.enqueue("influx", {"node_id": "!a"})
    queue.enqueue("influx", {"node_id": "!b"})

    _worker(queue, influx=influx)._flush()

    assert queue.size() == 2                   # nothing deleted
    assert queue.count_dead_letters() == 0     # not dead-lettered either
    influx.write_soil.assert_not_called()      # never silently "succeeded"


def test_unknown_error_capped_after_max_attempts(queue, monkeypatch):
    monkeypatch.setattr(m, "CLOUD_QUEUE_MAX_ATTEMPTS", 3)
    influx = Mock()
    influx.write_soil.side_effect = RuntimeError("mystery failure")
    queue.enqueue("influx", {"node_id": "!a"})
    worker = _worker(queue, influx=influx)

    worker._flush()  # attempts -> 1
    worker._flush()  # attempts -> 2
    assert queue.size() == 1                  # below cap: still retried (no data loss)
    assert queue.count_dead_letters() == 0

    worker._flush()  # attempts -> 3 (>= cap)
    assert queue.size() == 0
    assert queue.count_dead_letters() == 1    # eventually dead-lettered, unblocks head


def test_schema_migration_from_legacy_db(tmp_path):
    """An old DB without `attempts`/`sync_dead_letter` opens and is flushable."""
    import sqlite3

    db = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(db)
    conn.execute(
        """
        CREATE TABLE sync_queue (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            queued_at INTEGER NOT NULL,
            target    TEXT    NOT NULL,
            payload   TEXT    NOT NULL
        )
        """
    )
    conn.execute(
        "INSERT INTO sync_queue (queued_at, target, payload) VALUES (?, ?, ?)",
        (100, "influx", json.dumps({"node_id": "!legacy"})),
    )
    conn.commit()
    conn.close()

    queue = CloudSyncQueue(db)
    try:
        # Migration added the new column/table and the legacy row is intact.
        assert queue.size() == 1
        assert queue.count_dead_letters() == 0
        influx = Mock()
        _worker(queue, influx=influx)._flush()
        assert queue.size() == 0
        influx.write_soil.assert_called_once()
    finally:
        queue.close()
