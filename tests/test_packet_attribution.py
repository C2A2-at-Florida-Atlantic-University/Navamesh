"""Readings must never be filed under a node called "unknown".

`fromId` is resolved by the Meshtastic library from its own node database, so it
is empty for any node that transmits before its NodeInfo has been heard -- the
normal order after a gateway restart. Those readings were filed under "unknown",
which also appeared in the app's node picker and, with a position, on the map.

It self-heals in the worst way: once NodeInfo lands the readings start landing
correctly, so the gap reads as a brief outage rather than as measurements written
to the wrong node. The deployment Pi has an `unknown` row with real readings in
it, which is what this closes.
"""
import pytest

from navamesh.processors.node_id import nodenum_to_id, resolve_node_id
from navamesh.processors.link import extract_link
from navamesh.processors.position import extract_position
from navamesh.processors.telemetry import extract_battery
from navamesh.processors.node_info import extract_node_info


# ── nodenum_to_id ─────────────────────────────────────────────────────────────

def test_nodenum_formats_as_lowercase_hex_id():
    assert nodenum_to_id(0x0B9AED49) == "!0b9aed49"
    assert nodenum_to_id(0xCFD91E1C) == "!cfd91e1c"


def test_nodenum_accepts_decimal_string():
    assert nodenum_to_id("195147593") == "!0ba1b749"


def test_nodenum_masks_a_sign_extended_value():
    """A uint32 nodenum above 0x7FFFFFFF arrives negative on some JSON paths."""
    assert nodenum_to_id(0x9AAE7F49 - (1 << 32)) == "!9aae7f49"


@pytest.mark.parametrize("value", [0, 0xFFFFFFFF, None, True, False, "", "abc", 3.5])
def test_nodenum_rejects_values_that_cannot_be_a_sender(value):
    """0 is unset and 0xFFFFFFFF is broadcast; neither may become a node id."""
    assert nodenum_to_id(value) is None


# ── resolve_node_id ───────────────────────────────────────────────────────────

def test_resolve_prefers_from_id_when_the_library_resolved_it():
    packet = {"fromId": "!cfd91e1c", "from": 0x0B9AED49}
    assert resolve_node_id(packet) == "!cfd91e1c"


def test_resolve_falls_back_to_user_id_then_numeric():
    assert resolve_node_id({"decoded": {"user": {"id": "!abcd1234"}}}) == "!abcd1234"
    assert resolve_node_id({"user": {"id": "!abcd1234"}}) == "!abcd1234"
    assert resolve_node_id({"from": 0x0B9AED49}) == "!0b9aed49"


def test_resolve_ignores_a_blank_from_id():
    """The library leaves it empty rather than absent for an unknown sender."""
    assert resolve_node_id({"fromId": "   ", "from": 0x0B9AED49}) == "!0b9aed49"


def test_resolve_returns_none_when_nothing_identifies_the_sender():
    assert resolve_node_id({}) is None
    assert resolve_node_id({"from": 0}) is None
    assert resolve_node_id(None) is None


# ── the extractors no longer invent "unknown" ────────────────────────────────

def test_link_is_attributed_from_the_numeric_field():
    link = extract_link({"from": 0x0B9AED49, "rxRssi": -97, "rxSnr": 6.5})
    assert link is not None and link["fromId"] == "!0b9aed49"


def test_position_is_attributed_from_the_numeric_field():
    """A position filed under "unknown" also puts a pin on the farmer's map."""
    packet = {"from": 0x0B9AED49, "decoded": {"position": {"latitude": 35.9, "longitude": -108.7}}}
    pos = extract_position(packet)
    assert pos is not None and pos["fromId"] == "!0b9aed49"


def test_battery_is_attributed_from_the_numeric_field():
    packet = {
        "from": 0x0B9AED49,
        "decoded": {"portnum": "TELEMETRY_APP", "telemetry": {"deviceMetrics": {"batteryLevel": 82}}},
    }
    bat = extract_battery(packet)
    assert bat is not None and bat["fromId"] == "!0b9aed49"


def test_node_info_is_attributed_from_the_numeric_field():
    """Previously returned None, dropping the rename entirely."""
    packet = {"from": 0x0B9AED49, "decoded": {"user": {"longName": "North Field"}}}
    info = extract_node_info(packet)
    assert info is not None and info["fromId"] == "!0b9aed49"


