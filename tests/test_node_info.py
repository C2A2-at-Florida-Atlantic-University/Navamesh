"""Tests for Meshtastic NODEINFO_APP support: the gateway-side extractor, the
MQTT topic helper, and the mqtt_to_db ingestion that stores app renames in
mesh_nodes metadata so map labels can use them.

The extractor and topics modules have no heavy deps; the ingestor pieces
(classify_topic/apply_payload) are tested on a bare instance without touching
MQTT or databases, matching how the pure helpers are tested elsewhere.
"""
from types import SimpleNamespace

import pytest

from navamesh import topics
from navamesh.processors.node_info import extract_node_info
from navamesh.mqtt_to_db import MqttToDbIngestor, NodeState


# ── extract_node_info ─────────────────────────────────────────────────────────

def test_extract_node_info_handles_decoded_user_camel_case():
    packet = {
        "fromId": "!abcd1234",
        "decoded": {
            "portnum": "NODEINFO_APP",
            "user": {"id": "!abcd1234", "longName": "North Field", "shortName": "NF01"},
        },
    }
    info = extract_node_info(packet)
    assert info is not None
    assert info["fromId"] == "!abcd1234"
    assert info["longName"] == "North Field"
    assert info["shortName"] == "NF01"
    assert isinstance(info["ts"], int)


def test_extract_node_info_handles_decoded_user_snake_case():
    packet = {
        "decoded": {
            "user": {"id": "!abcd1234", "long_name": "North Field", "short_name": "NF01"},
        },
    }
    info = extract_node_info(packet)
    assert info == {
        "ts": info["ts"],
        "fromId": "!abcd1234",
        "longName": "North Field",
        "shortName": "NF01",
    }


def test_extract_node_info_handles_top_level_user_and_from_id_fallback():
    packet = {
        "fromId": "!abcd1234",
        "user": {"longName": "North Field"},  # no user.id -> falls back to fromId
    }
    info = extract_node_info(packet)
    assert info is not None
    assert info["fromId"] == "!abcd1234"
    assert info["longName"] == "North Field"
    assert info["shortName"] is None


def test_extract_node_info_ignores_packets_without_names():
    assert extract_node_info({"fromId": "!abcd1234"}) is None
    assert extract_node_info({"decoded": {"portnum": "TELEMETRY_APP"}}) is None
    # user present but names empty/whitespace-only
    assert extract_node_info(
        {"fromId": "!abcd1234", "decoded": {"user": {"id": "!abcd1234", "longName": "  "}}}
    ) is None


def test_extract_node_info_ignores_packets_without_stable_id():
    assert extract_node_info({"decoded": {"user": {"longName": "North Field"}}}) is None


# ── topic helper ──────────────────────────────────────────────────────────────

def test_node_info_topic():
    assert topics.node_info("farm/nodes", "!abcd1234") == "farm/nodes/!abcd1234/info"


# ── mqtt_to_db ingestion ──────────────────────────────────────────────────────

def _bare_ingestor() -> MqttToDbIngestor:
    """Ingestor instance without running __init__ (no MQTT/DB connections);
    classify_topic only needs self.cfg roots."""
    ing = MqttToDbIngestor.__new__(MqttToDbIngestor)
    ing.cfg = SimpleNamespace(root_sensors="farm/sensors", root_nodes="farm/nodes")
    return ing


def test_classify_topic_recognizes_info():
    ing = _bare_ingestor()
    assert ing.classify_topic("farm/nodes/!abcd1234/info") == ("info", "!abcd1234")
    # existing kinds unchanged
    assert ing.classify_topic("farm/nodes/!abcd1234/battery") == ("battery", "!abcd1234")
    assert ing.classify_topic("farm/sensors/soil/!abcd1234/percent") == (
        "soil_percent", "!abcd1234",
    )


def test_mqtt_to_db_applies_node_info_to_metadata():
    ing = _bare_ingestor()
    state = NodeState(node_id="!abcd1234")
    ing.apply_payload(
        state, "info",
        {"ts": 1700000000, "fromId": "!abcd1234", "longName": "North Field", "shortName": "NF01"},
    )
    assert state.long_name == "North Field"
    assert state.short_name == "NF01"
    assert state.display_name == "NF01"          # short first
    assert state.last_seen_ts == 1700000000      # info alone still bumps last_seen

    meta = state.metadata("FAU Garden", "field-node")
    assert meta["long_name"] == "North Field"
    assert meta["short_name"] == "NF01"
    assert meta["display_name"] == "NF01"
    # existing metadata fields preserved
    assert meta["location"] == "FAU Garden"
    assert "soil_percent" in meta and "battery_level" in meta


def test_mqtt_to_db_node_info_display_falls_back_to_long_name():
    ing = _bare_ingestor()
    state = NodeState(node_id="!abcd1234")
    ing.apply_payload(state, "info", {"longName": "North Field", "shortName": "  "})
    assert state.short_name is None              # empty string normalized to None
    assert state.display_name == "North Field"


def test_mqtt_to_db_node_info_accepts_snake_case_payload():
    ing = _bare_ingestor()
    state = NodeState(node_id="!abcd1234")
    ing.apply_payload(state, "info", {"long_name": "North Field", "short_name": "NF01"})
    assert state.display_name == "NF01"


def test_mqtt_to_db_node_info_does_not_touch_telemetry_fields():
    ing = _bare_ingestor()
    state = NodeState(node_id="!abcd1234", soil_percent=45.0, battery_level=82.0)
    ing.apply_payload(state, "info", {"shortName": "NF01"})
    assert state.soil_percent == 45.0
    assert state.battery_level == 82.0
