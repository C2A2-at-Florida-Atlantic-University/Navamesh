"""A delayed or replayed packet must not rewrite a node's recency or position.

Observed corrupting live data on 2026-08-21: !0b9aed49's last_seen jumped from
20:52 back to 2026-08-18 23:42 while the node was actively reporting.

Two layers, because either alone leaves the hole open. `apply_payload()` guards
the cached in-memory state -- without it a replayed position overwrites lat/lon
in the cache, and the next genuine packet of any other kind carries that stale
position into Postgres stamped with a current timestamp, passing any SQL check.
The upsert guard then backstops whatever reaches Postgres with an older
timestamp than the row holds, chiefly the cloud sync flusher replaying
serialized states long after newer ones were written.
"""
from types import SimpleNamespace

import pytest

from navamesh.mqtt_to_db import MqttToDbIngestor, NodeState, PostgresWriter


# ── apply_payload freshness ──────────────────────────────────────────────────

def _bare_ingestor() -> MqttToDbIngestor:
    ing = MqttToDbIngestor.__new__(MqttToDbIngestor)
    ing.cfg = SimpleNamespace(root_sensors="farm/sensors", root_nodes="farm/nodes")
    return ing


def test_a_replayed_position_cannot_overwrite_a_newer_one():
    ing = _bare_ingestor()
    state = NodeState(node_id="!0b9aed49")

    ing.apply_payload(state, "position", {"ts": 1755800000, "lat": 35.9, "lon": -108.7})
    ing.apply_payload(state, "position", {"ts": 1755500000, "lat": 12.0, "lon": 12.0})

    assert (state.lat, state.lon) == (35.9, -108.7)
    assert state.last_seen_ts == 1755800000


def test_a_replayed_packet_cannot_pull_last_seen_backwards():
    ing = _bare_ingestor()
    state = NodeState(node_id="!0b9aed49")

    ing.apply_payload(state, "soil_raw", {"ts": 1755800000, "value": 2400})
    ing.apply_payload(state, "soil_raw", {"ts": 1755300000, "value": 4095})

    assert state.last_seen_ts == 1755800000
    assert state.soil_raw == 2400


def test_retained_topics_hydrate_in_any_order_with_their_own_timestamps():
    """The startup case a single whole-state timestamp would break.

    On connect the ingestor receives every retained topic for a node at once, in
    arbitrary order, each carrying the timestamp of when it was published. An
    older-than-newest rule applied across kinds would discard all but the first.
    """
    ing = _bare_ingestor()
    state = NodeState(node_id="!0b9aed49")

    ing.apply_payload(state, "soil_raw", {"ts": 1755800000, "value": 2400})
    ing.apply_payload(state, "position", {"ts": 1755700000, "lat": 35.9, "lon": -108.7})
    ing.apply_payload(state, "battery", {"ts": 1755600000, "batteryLevel": 82})

    assert state.soil_raw == 2400
    assert (state.lat, state.lon) == (35.9, -108.7)
    assert state.battery_level == 82
    assert state.last_seen_ts == 1755800000


def test_a_fresh_packet_of_one_kind_still_updates_after_a_stale_one_of_another():
    ing = _bare_ingestor()
    state = NodeState(node_id="!0b9aed49")

    ing.apply_payload(state, "position", {"ts": 1755800000, "lat": 35.9, "lon": -108.7})
    ing.apply_payload(state, "position", {"ts": 1755500000, "lat": 12.0, "lon": 12.0})
    ing.apply_payload(state, "soil_band", {"ts": 1755900000, "value": "DAMP"})

    assert state.soil_band == "DAMP"
    assert (state.lat, state.lon) == (35.9, -108.7)   # stale position stayed rejected
    assert state.last_seen_ts == 1755900000


def test_an_equal_timestamp_is_still_applied():
    """A reading and a position published in the same second must both land."""
    ing = _bare_ingestor()
    state = NodeState(node_id="!0b9aed49")

    ing.apply_payload(state, "soil_raw", {"ts": 1755800000, "value": 2400})
    ing.apply_payload(state, "soil_raw", {"ts": 1755800000, "value": 2450})

    assert state.soil_raw == 2450


# ── the Postgres backstop ────────────────────────────────────────────────────

class _CapturingConn:
    """Minimal psycopg stand-in that records the statement it is handed."""

    def __init__(self):
        self.statements = []

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params):
        self.statements.append(query)


def _rendered(state: NodeState, postgis: bool) -> str:
    writer = PostgresWriter("postgresql://unused", "mesh_nodes")
    conn = _CapturingConn()
    writer._conn = conn
    writer._postgis = postgis
    writer.upsert_node(state, "spirit-farm", "sensor")
    return conn.statements[-1]


@pytest.mark.parametrize(
    "state, postgis",
    [
        (NodeState(node_id="!0b9aed49", lat=35.9, lon=-108.7, last_seen_ts=1755800000), True),
        (NodeState(node_id="!0b9aed49", lat=35.9, lon=-108.7, last_seen_ts=1755800000), False),
        (NodeState(node_id="!0b9aed49", last_seen_ts=1755800000), False),
    ],
    ids=["postgis", "plain-latlon", "no-coords"],
)
def test_every_upsert_branch_guards_recency(state, postgis):
    """All three branches, because the ingestor picks between them per packet and
    a guard on only the one being exercised looks identical in testing."""
    sql = _rendered(state, postgis)
    assert "last_seen = GREATEST(mesh_nodes.last_seen, EXCLUDED.last_seen)" in sql
    assert "last_seen = EXCLUDED.last_seen" not in sql
    assert "metadata  = EXCLUDED.metadata" not in sql


def test_position_columns_are_guarded_too():
    """A stale replay overwriting a newer position is the same bug as last_seen,
    and it is the one that makes a farmer's SET_LOCATION look like it failed."""
    sql = _rendered(
        NodeState(node_id="!0b9aed49", lat=35.9, lon=-108.7, last_seen_ts=1755800000), True
    )
    for column in ("lat", "lon", "geom"):
        assert f"EXCLUDED.{column}" in sql
        assert f"ELSE mesh_nodes.{column}" in sql
