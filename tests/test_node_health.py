"""Telling a node that is gone from one that is present but not measuring.

The middle state is the one that cost real time. A field node left in CLIENT role
acks commands, broadcasts NodeInfo, sits in the picker and reports a healthy link
while never sending a single soil reading. !d60add90 lived its whole life like
that -- 19 transmissions, 0 readings, against ~1950/275 for its siblings -- and
read as an unreliable radio rather than an unprovisioned one.

`last_seen` alone cannot see it, because every one of those packets moves
`last_seen`. It takes a second timestamp for soil specifically, which is what
soil_last_ts is.
"""
import pytest

try:
    from navamesh.reticulum_bridge import (
        NodeSnapshot,
        classify_node_health,
        _health_note,
        node_stale_after_seconds,
        NODE_REPORTING,
        NODE_NOT_REPORTING,
        NODE_UNHEARD,
        handle_command,
        ReticulumBridgeConfig,
    )
except (ImportError, SystemExit) as exc:  # rns/lxmf/staticmap/dotenv not installed
    pytest.skip(f"reticulum_bridge unavailable: {exc}", allow_module_level=True)


NOW = 1_756_000_000
STALE_AFTER = 3600.0        # explicit, so no test waits out an 8-hour interval


def test_a_node_reporting_on_schedule_is_reporting():
    assert classify_node_health(NOW, NOW - 60, NOW - 60, STALE_AFTER) == NODE_REPORTING


def test_a_node_heard_recently_with_no_readings_ever_is_not_reporting():
    """The CLIENT-role case: plenty of packets, never a reading."""
    assert classify_node_health(NOW, NOW - 60, None, STALE_AFTER) == NODE_NOT_REPORTING


def test_a_node_whose_readings_stopped_is_not_reporting():
    """"In a while", not "ever" -- a probe that dies after a month of good data is
    the same problem to the farmer as one that never worked."""
    assert classify_node_health(
        NOW, NOW - 60, NOW - int(STALE_AFTER) - 1, STALE_AFTER
    ) == NODE_NOT_REPORTING


def test_a_node_sending_nothing_at_all_is_unheard():
    assert classify_node_health(
        NOW, NOW - int(STALE_AFTER) - 1, NOW - int(STALE_AFTER) - 1, STALE_AFTER
    ) == NODE_UNHEARD


def test_a_never_heard_node_is_unheard_not_merely_quiet():
    assert classify_node_health(NOW, None, None, STALE_AFTER) == NODE_UNHEARD


def test_unheard_outranks_not_reporting():
    """A node nothing has been heard from should not be described as one that is
    present but idle -- the action to take is different."""
    assert classify_node_health(
        NOW, NOW - int(STALE_AFTER) - 1, None, STALE_AFTER
    ) == NODE_UNHEARD


def test_one_missed_transmission_does_not_condemn_a_node():
    """A single lost packet is ordinary on a LoRa link; the threshold sits above
    one interval on purpose."""
    interval = node_stale_after_seconds()
    assert classify_node_health(NOW, NOW - 60, NOW - int(interval) + 60) == NODE_REPORTING


def test_health_note_is_absent_for_a_healthy_node():
    snap = NodeSnapshot(node_id="!ok", ts=NOW - 60, soil_last_ts=NOW - 60)
    assert _health_note(snap, now=NOW) is None


def test_health_note_says_what_to_do_not_which_state_it_is_in():
    quiet = NodeSnapshot(node_id="!q", ts=NOW - 60, soil_last_ts=None)
    gone = NodeSnapshot(node_id="!g", ts=None, soil_last_ts=None)
    assert _health_note(quiet, now=NOW) == "no soil readings recently"
    assert _health_note(gone, now=NOW) == "not heard from recently"


# ── the farmer-facing surface ────────────────────────────────────────────────

def _cfg() -> ReticulumBridgeConfig:
    return ReticulumBridgeConfig(
        rns_config_dir="/tmp/rns", lxmf_storage_dir="/tmp/lxmf", display_name="test",
        announce_interval=180, map_tile_url="", map_tile_fallback="",
        map_max_dimension=640, map_jpeg_quality=75, map_max_bytes=120_000,
        soil_wet_threshold=60.0, soil_dry_threshold=30.0,
        map_outlier_guard_enabled=False, map_mesh_neighbor_radius_m=800.0,
        map_outlier_min_nodes=3, map_separate_farm_distance_m=3000.0,
        cache_lat_min=None, cache_lat_max=None, cache_lon_min=None, cache_lon_max=None,
        cache_zoom_min=14, cache_zoom_max=19, pg_dsn="",
    )


def test_nodes_flags_stale_nodes_without_hiding_them():
    """Flagged, not filtered. A forgotten node is still standing in the field --
    hiding it from the list is how it gets forgotten. But it must not look
    identical to a live one in the list the farmer taps to send a command."""
    nodes = {
        "!live0001": NodeSnapshot(node_id="!live0001", ts=None, soil_last_ts=None),
        "!quiet001": NodeSnapshot(node_id="!quiet001", ts=None, soil_last_ts=None),
    }
    import time as _time
    now = int(_time.time())
    nodes["!live0001"].ts = now - 60
    nodes["!live0001"].soil_last_ts = now - 60

    text, image = handle_command("nodes", nodes, _cfg())
    assert image is None
    assert "!live0001" in text and "!quiet001" in text      # neither is hidden

    lines = text.splitlines()

    # The id line carries the id and nothing else, for every node, flagged or not.
    # This is the app's wire format, not a layout preference: parse_nodes_reply()
    # keeps each "!"-prefixed line WHOLE and uses it as the node id, so a warning
    # appended here became part of the id and every command aimed at that node
    # went to "!quiet001  ⚠️ no readings in ...". Anything to say about a node
    # goes on the continuation line beneath it, which that parser drops.
    for node_id in ("!live0001", "!quiet001"):
        id_line = next(l for l in lines if node_id in l)
        assert id_line.strip() == node_id

    # Flagged, not filtered: the quiet one still says so, on the continuation
    # line belonging to it. Collected by walking from each id to the next one so
    # the assertion does not depend on listing order, or on a node that has
    # nothing to add emitting a continuation line at all.
    def detail_for(node_id: str) -> str:
        start = next(i for i, l in enumerate(lines) if l.strip() == node_id)
        out = []
        for line in lines[start + 1:]:
            if line.strip().startswith("!") or not line.strip():
                break
            out.append(line)
        return "\n".join(out)

    assert "⚠️" in detail_for("!quiet001")
    assert "⚠️" not in detail_for("!live0001")
    assert "1 of 2 need checking" in text


def test_nodes_explains_a_quiet_sensor_still_answers_commands():
    """Otherwise the warning reads as "this node is unreachable", and the farmer
    stops trying to fix the thing that is actually wrong."""
    import time as _time
    now = int(_time.time())
    # Heard a minute ago, but no soil reading ever: present, not measuring.
    nodes = {"!quiet001": NodeSnapshot(node_id="!quiet001", ts=now - 60, soil_last_ts=None)}
    text, _ = handle_command("nodes", nodes, _cfg())
    assert "will answer commands" in text and "not measuring" in text
    assert "check power" not in text          # not this node's problem


def test_nodes_explains_an_unheard_sensor_differently():
    """Caught by running it: a list of nodes nothing has been heard from was being
    annotated with advice about sensors that answer commands, which sends the
    reader after the wrong fault."""
    nodes = {"!gone0001": NodeSnapshot(node_id="!gone0001", ts=None, soil_last_ts=None)}
    text, _ = handle_command("nodes", nodes, _cfg())
    assert "not answering at all" in text and "check power" in text
    assert "will answer commands" not in text


def test_nodes_explains_both_states_when_both_are_present():
    import time as _time
    now = int(_time.time())
    nodes = {
        "!quiet001": NodeSnapshot(node_id="!quiet001", ts=now - 60, soil_last_ts=None),
        "!gone0001": NodeSnapshot(node_id="!gone0001", ts=None, soil_last_ts=None),
    }
    text, _ = handle_command("nodes", nodes, _cfg())
    assert "2 of 2 need checking" in text
    assert "will answer commands" in text and "check power" in text


def test_nodes_stays_quiet_when_every_node_is_healthy():
    import time as _time
    now = int(_time.time())
    nodes = {"!live0001": NodeSnapshot(node_id="!live0001", ts=now - 60, soil_last_ts=now - 60)}
    text, _ = handle_command("nodes", nodes, _cfg())
    assert "⚠️" not in text and "need checking" not in text
