"""
test_command_proto.py — Downlink command encoding and ack decoding.

The coexistence tests matter most here. SoilReading and NavameshCommand happen to use
the same field numbers (1-4), so they ARE mutually parseable as raw bytes; what keeps
them apart is that they ride different portnums. These tests pin that down, because if
someone later "simplifies" the design back onto a single portnum, soil readings would
start being decoded as commands and vice versa.
"""

import base64

import pytest

from navamesh.processors.command_proto import (
    ACK_PORTNUM,
    COMMAND_PORTNUM,
    CommandValidationError,
    encode_command,
    extract_command_ack,
    is_command_ack,
)
from navamesh.processors.soil_proto import extract_soil_reading
from navamesh.proto import navamesh_pb2


def _ack_packet(ack: navamesh_pb2.NavameshAck, portnum=ACK_PORTNUM, as_b64=False):
    payload = ack.SerializeToString()
    if as_b64:
        payload = base64.b64encode(payload).decode("ascii")
    return {"fromId": "!abc12345", "decoded": {"portnum": portnum, "payload": payload}}


# ── encode_command ───────────────────────────────────────────────────────────────

def test_encode_ble_window_roundtrips():
    raw = encode_command("ble", command_id=5, value=30)
    cmd = navamesh_pb2.NavameshCommand()
    cmd.ParseFromString(raw)
    assert cmd.command_type == navamesh_pb2.BLE_WINDOW
    assert cmd.command_id == 5
    assert cmd.duration_minutes == 30


def test_encode_interval_uses_interval_field_not_duration():
    raw = encode_command("interval", command_id=6, value=1800)
    cmd = navamesh_pb2.NavameshCommand()
    cmd.ParseFromString(raw)
    assert cmd.command_type == navamesh_pb2.SET_TELEMETRY_INTERVAL
    assert cmd.interval_seconds == 1800
    assert cmd.duration_minutes == 0


def test_encode_quiet_on_defers_to_the_node_default():
    """
    With no explicit duration, send 0 — the proto's "use the node's own default" (24 h).

    Regression guard: this previously defaulted to QUIET_MAX_MINUTES, so an unqualified
    "pause this node" silenced it for the full 3-day ceiling. Confirmed on hardware, which
    logged "auto-resume in 4320 min". The auto-resume safety net still exists either way —
    the firmware clamps 0 to its default and never treats it as "forever".
    """
    raw = encode_command("quiet", command_id=7, quiet_on=True)
    cmd = navamesh_pb2.NavameshCommand()
    cmd.ParseFromString(raw)
    assert cmd.command_type == navamesh_pb2.QUIET_MODE_ENTER
    assert cmd.duration_minutes == 0


def test_encode_quiet_on_honours_an_explicit_duration():
    raw = encode_command("quiet", command_id=7, quiet_on=True, value=120)
    cmd = navamesh_pb2.NavameshCommand()
    cmd.ParseFromString(raw)
    assert cmd.duration_minutes == 120


@pytest.mark.parametrize("minutes", [0, 4321])
def test_encode_rejects_out_of_range_explicit_quiet_duration(minutes):
    """An explicit 0 is a mistake, not a request for the default — reject it."""
    with pytest.raises(CommandValidationError):
        encode_command("quiet", command_id=7, quiet_on=True, value=minutes)


def test_encode_quiet_off():
    raw = encode_command("quiet", command_id=8, quiet_on=False)
    cmd = navamesh_pb2.NavameshCommand()
    cmd.ParseFromString(raw)
    assert cmd.command_type == navamesh_pb2.QUIET_MODE_EXIT


@pytest.mark.parametrize("cmd_id", [0, -1])
def test_encode_rejects_non_positive_command_id(cmd_id):
    """command_id 0 is reserved for unsolicited acks; the firmware rejects it."""
    with pytest.raises(CommandValidationError):
        encode_command("ble", command_id=cmd_id, value=15)


@pytest.mark.parametrize("minutes", [0, 241, 99999])
def test_encode_rejects_out_of_range_ble_window(minutes):
    with pytest.raises(CommandValidationError):
        encode_command("ble", command_id=1, value=minutes)


@pytest.mark.parametrize("seconds", [1, 59, 86401])
def test_encode_rejects_out_of_range_interval(seconds):
    with pytest.raises(CommandValidationError):
        encode_command("interval", command_id=1, value=seconds)


def test_encode_rejects_unknown_verb():
    with pytest.raises(CommandValidationError):
        encode_command("reboot", command_id=1, value=1)


def test_encode_requires_a_value_where_one_is_needed():
    with pytest.raises(CommandValidationError):
        encode_command("ble", command_id=1)
    with pytest.raises(CommandValidationError):
        encode_command("quiet", command_id=1)


# ── extract_command_ack ──────────────────────────────────────────────────────────

def test_extract_ack_happy_path():
    ack = navamesh_pb2.NavameshAck(
        command_id=5, command_type=navamesh_pb2.BLE_WINDOW, ok=True, applied_value=30
    )
    got = extract_command_ack(_ack_packet(ack))
    assert got["command_id"] == 5
    assert got["ok"] is True
    assert got["applied_value"] == 30
    assert got["command_type_name"] == "BLE_WINDOW"
    assert got["unsolicited"] is False


def test_extract_ack_accepts_base64_payload():
    """meshtastic is unpinned and hands us bytes or base64 str depending on version."""
    ack = navamesh_pb2.NavameshAck(command_id=9, ok=True)
    assert extract_command_ack(_ack_packet(ack, as_b64=True))["command_id"] == 9


def test_extract_ack_accepts_string_portnum():
    """259 is not in meshtastic's PortNum enum, so it may arrive as int or str."""
    ack = navamesh_pb2.NavameshAck(command_id=9, ok=True)
    assert extract_command_ack(_ack_packet(ack, portnum="259")) is not None


def test_unsolicited_ack_is_flagged():
    """command_id 0 means quiet mode self-expired with nobody having asked."""
    ack = navamesh_pb2.NavameshAck(
        command_id=0, command_type=navamesh_pb2.QUIET_MODE_EXIT, ok=True
    )
    got = extract_command_ack(_ack_packet(ack))
    assert got["unsolicited"] is True


def test_extract_ack_ignores_other_portnums():
    ack = navamesh_pb2.NavameshAck(command_id=5, ok=True)
    assert extract_command_ack(_ack_packet(ack, portnum="PRIVATE_APP")) is None
    assert extract_command_ack(_ack_packet(ack, portnum=COMMAND_PORTNUM)) is None


def test_extract_ack_rejects_empty_and_garbage_without_raising():
    assert extract_command_ack(
        {"decoded": {"portnum": ACK_PORTNUM, "payload": b""}}
    ) is None
    assert extract_command_ack(
        {"decoded": {"portnum": ACK_PORTNUM, "payload": b"\xff\xff\xff\xff\xff"}}
    ) is None
    assert extract_command_ack({}) is None


def test_portnum_true_is_not_treated_as_one():
    """bool is an int subclass; True must not match portnum 1 or anything else."""
    assert is_command_ack({"decoded": {"portnum": True, "payload": b"x"}}) is False


# ── coexistence with the soil path ───────────────────────────────────────────────

def test_soil_reading_is_never_decoded_as_a_command_ack():
    sr = navamesh_pb2.SoilReading(
        raw_adc=1842, battery_percent=85, battery_mv=4020, uptime_seconds=86400
    )
    soil_packet = {
        "fromId": "!abc12345",
        "decoded": {"portnum": "PRIVATE_APP", "payload": sr.SerializeToString()},
    }
    assert extract_command_ack(soil_packet) is None
    # ...and the soil path still reads it correctly.
    assert extract_soil_reading(soil_packet)["raw_adc"] == 1842


def test_command_ack_is_never_decoded_as_a_soil_reading():
    ack = navamesh_pb2.NavameshAck(
        command_id=5, command_type=navamesh_pb2.BLE_WINDOW, ok=True, applied_value=30
    )
    # Same bytes, but on the ack portnum: the soil decoder must not claim it.
    assert extract_soil_reading(_ack_packet(ack)) is None
