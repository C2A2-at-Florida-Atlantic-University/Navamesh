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


# ── InfluxDB point timestamps ────────────────────────────────────────────────
#
# The guards above stop a replayed packet rewriting the *cache*. They do not
# stop it being written to the time series under the wrong date, because the
# writers took their timestamp from state.last_seen_ts -- the newest timestamp
# from ANY payload kind -- rather than from the packet in hand.
#
# Measured on spirit-farm-pi, 2026-08-27: an ingestor MQTT reconnect at
# 2026-08-26 17:00 UTC replayed retained battery topics holding readings taken
# on 2026-08-22. Link packets had kept last_seen_ts current throughout, so six
# nodes' four-day-old battery levels were written stamped 2026-08-26 and read as
# fresh measurements on the public site.

from navamesh.mqtt_to_db import InfluxWriter, _point_time, _state_to_dict


AUG_22 = 1787397653   # when the reading was actually taken
AUG_26 = 1787763600   # when a reconnect replayed it


def _writer_recording_points():
    """An InfluxWriter whose _write_api records what it was handed."""
    w = InfluxWriter.__new__(InfluxWriter)
    w._bucket, w._org = "b", "o"
    written = []
    w._write_api = SimpleNamespace(write=lambda bucket, org, record: written.append(record))
    return w, written


def test_a_replayed_reading_is_stamped_when_it_was_taken_not_when_it_arrived():
    state = NodeState(node_id="!045de249")
    state.battery_level = 91.0
    # Link traffic has carried recency forward while the battery topic sat retained.
    state.last_seen_ts = AUG_26
    assert _point_time(state, AUG_22).timestamp() == AUG_22, (
        "the packet's own timestamp must win over last_seen_ts"
    )


def test_write_soil_stamps_the_packet_not_the_node_s_recency():
    w, written = _writer_recording_points()
    state = NodeState(node_id="!045de249")
    state.battery_level = 91.0
    state.last_seen_ts = AUG_26
    w.write_soil(state, AUG_22)
    assert len(written) == 1
    # Point stores the time it was given; compare through its own line protocol.
    assert str(AUG_22) in written[0].to_line_protocol()


def test_last_seen_is_only_the_fallback_when_no_packet_time_is_known():
    state = NodeState(node_id="!045de249")
    state.last_seen_ts = AUG_26
    assert _point_time(state, None).timestamp() == AUG_26


def test_a_queued_cloud_write_carries_the_packet_time_so_a_late_flush_is_not_re_dated():
    state = NodeState(node_id="!045de249")
    state.last_seen_ts = AUG_26
    assert _state_to_dict(state, point_ts=AUG_22)["point_ts"] == AUG_22
    # Rows queued by an older build have no such key; readers must tolerate that.
    assert _state_to_dict(state)["point_ts"] is None


def test_write_outputs_uses_the_timestamp_apply_payload_recorded_for_that_kind():
    """The two halves have to agree: apply_payload files the packet's ts under
    its kind, and write_outputs must read it back from there. A mismatch is
    invisible -- the write still succeeds, just under the wrong date."""
    ing = _bare_ingestor()
    state = NodeState(node_id="!045de249")

    calls = []
    ing.influx = SimpleNamespace(
        enabled=True,
        write_soil=lambda st, pts: calls.append(("soil", pts)),
        write_link=lambda st, pts: calls.append(("link", pts)),
        write_position=lambda st, pts: calls.append(("position", pts)),
    )
    ing.influx_cloud = SimpleNamespace(enabled=False)
    ing.pg = SimpleNamespace(enabled=False)
    ing.pg_cloud = SimpleNamespace(enabled=False)

    # A stale battery replay arriving while link traffic keeps recency current.
    ing.apply_payload(state, "link", {"ts": AUG_26, "rxRssi": -73, "rxSnr": 6.0})
    ing.apply_payload(state, "battery", {"ts": AUG_22, "batteryLevel": 91.0})
    assert state.last_seen_ts == AUG_26      # recency is still the newest packet
    ing.write_outputs(state, "battery")

    assert calls == [("soil", AUG_22)], (
        "the battery reading must be stamped 2026-08-22, not dragged to 2026-08-26"
    )


# ── battery_last_ts: the same guarantee soil_last_ts gives, for battery ──────
#
# Stored on the gateway rather than derived by each consumer, so that any Pi
# running this project dates its battery readings the same way. Deriving it
# downstream meant every reader reaching into its own time series and reaching
# its own conclusion -- and a reader with no time series (the farmer's app)
# having no way to tell a fresh 91% from a five-day-old one at all.

def test_battery_last_ts_tracks_battery_not_any_packet():
    ing = _bare_ingestor()
    state = NodeState(node_id="!045de249")

    ing.apply_payload(state, "battery", {"ts": AUG_22, "batteryLevel": 91.0})
    assert state.battery_last_ts() == AUG_22

    # A NodeInfo beacon four days later moves recency and must not move this.
    ing.apply_payload(state, "info", {"ts": AUG_26, "longName": "Node F"})
    ing.apply_payload(state, "link", {"ts": AUG_26, "rxRssi": -73, "rxSnr": 6.0})
    assert state.last_seen_ts == AUG_26
    assert state.battery_last_ts() == AUG_22, (
        "a beacon says the radio was heard, never that the battery was measured"
    )


def test_battery_and_soil_stamps_are_independent():
    """On the legacy text firmware both ride one message, so conflating them
    looks harmless. On raw-ADC firmware, and on any node sending device
    telemetry, they diverge -- which is when a single stamp starts lying."""
    ing = _bare_ingestor()
    state = NodeState(node_id="!045de249")
    ing.apply_payload(state, "soil_percent", {"ts": AUG_22, "value": 100})
    ing.apply_payload(state, "battery", {"ts": AUG_26, "batteryLevel": 91.0})
    assert state.soil_last_ts() == AUG_22
    assert state.battery_last_ts() == AUG_26


def test_never_reported_stays_none_rather_than_becoming_now():
    state = NodeState(node_id="!045de249")
    assert state.battery_last_ts() is None
    assert state.soil_last_ts() is None


def test_metadata_publishes_both_stamps():
    ing = _bare_ingestor()
    state = NodeState(node_id="!045de249")
    ing.apply_payload(state, "battery", {"ts": AUG_22, "batteryLevel": 91.0})
    md = state.metadata("Spirit Farm", "sensor")
    assert md["battery_last_ts"] == AUG_22
    assert md["soil_last_ts"] is None


def test_both_stamps_are_carried_forward_by_the_upsert():
    """A restart empties applied_ts, so a live link packet arriving before the
    retained battery topic is replayed would otherwise write a null over a good
    stored value. Both keys must appear in the carry-forward, and NULLIF must be
    there too -- `->` on a JSON null yields the jsonb literal 'null', which
    COALESCE happily selects, silently doing nothing."""
    w = PostgresWriter.__new__(PostgresWriter)
    w._table_name = "mesh_nodes"
    sql = w._metadata_carry_forward_sql()
    for key in ("soil_last_ts", "battery_last_ts"):
        assert f"'{key}'" in sql
        assert f"NULLIF(EXCLUDED.metadata->'{key}'" in sql
