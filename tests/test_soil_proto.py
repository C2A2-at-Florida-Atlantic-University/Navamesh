"""
Tests for the FORMAT C path: navamesh.SoilReading protobuf on PortNum 256.

The central guarantee under test is that the raw ADC the node measured survives
byte-for-byte into what the Pi stores, and that ONLY the derived percentage is
ever clamped.
"""

import base64

import pytest

from navamesh.calibration import adc_to_percent
from navamesh.processors.soil_proto import (
    extract_soil_reading,
    is_soil_reading,
    make_soil_mqtt_payloads,
)
from navamesh.proto import navamesh_pb2

DRY = 3120
WET = 1567


def _encode(raw_adc=1842, battery_percent=85, battery_mv=4020, uptime_seconds=86400):
    """Encode exactly what the firmware's pb_encode_to_bytes() would produce."""
    return navamesh_pb2.SoilReading(
        raw_adc=raw_adc,
        battery_percent=battery_percent,
        battery_mv=battery_mv,
        uptime_seconds=uptime_seconds,
    ).SerializeToString()


def _packet(payload, portnum="PRIVATE_APP", from_id="!a1b2c3d4"):
    return {"fromId": from_id, "channel": 1,
            "decoded": {"portnum": portnum, "payload": payload}}


# --------------------------------------------------------------------------
# Decoding
# --------------------------------------------------------------------------

def test_decodes_private_app_reading():
    r = extract_soil_reading(_packet(_encode()))
    assert r == {
        "raw_adc": 1842,
        "battery_percent": 85,
        "battery_mv": 4020,
        "uptime_seconds": 86400,
    }


def test_accepts_base64_payload():
    """meshtastic is unpinned; some versions hand back base64 str, not bytes."""
    r = extract_soil_reading(_packet(base64.b64encode(_encode()).decode()))
    assert r is not None and r["raw_adc"] == 1842


def test_ignores_other_portnums():
    assert extract_soil_reading(_packet(_encode(), portnum="TEXT_MESSAGE_APP")) is None
    assert not is_soil_reading(_packet(b"", portnum="TELEMETRY_APP"))


def test_undecodable_private_app_payload_returns_none_not_raises():
    """PortNum 256 is shared by every private app on the mesh."""
    assert extract_soil_reading(_packet(b"\xff\xff\xff\xff\xff\xff")) is None
    assert extract_soil_reading(_packet(None)) is None


def test_proto3_omits_zeros_so_partial_readings_still_decode():
    """
    Proto3 drops zero-valued scalars from the wire; absent fields must read back
    as 0 rather than tripping the decoder.
    """
    payload = _encode(raw_adc=1842, battery_percent=0, battery_mv=0, uptime_seconds=0)
    r = extract_soil_reading(_packet(payload))
    assert r == {"raw_adc": 1842, "battery_percent": 0, "battery_mv": 0, "uptime_seconds": 0}


def test_empty_payload_rejected_to_avoid_phantom_zero_readings():
    """
    An all-zeros SoilReading encodes to zero bytes -- and so does any other
    application's empty PRIVATE_APP packet, since portnum 256 is shared. We reject
    b"" deliberately: a phantom soil_raw=0 corrupting the dataset is worse than
    dropping a reading that would require uptime_seconds == 0 to occur at all.
    """
    assert _encode(raw_adc=0, battery_percent=0, battery_mv=0, uptime_seconds=0) == b""
    assert extract_soil_reading(_packet(b"")) is None


# --------------------------------------------------------------------------
# Calibration — clamps the percentage, never the raw ADC
# --------------------------------------------------------------------------

def test_calibration_endpoints():
    assert adc_to_percent(DRY, DRY, WET) == 0.0
    assert adc_to_percent(WET, DRY, WET) == 100.0


def test_percentage_clamps_but_raw_does_not():
    # Drier than the dry point, and wetter than the wet point.
    for raw in (4095, 500):
        pct = adc_to_percent(raw, DRY, WET)
        assert 0.0 <= pct <= 100.0
        raw_pl, pct_pl, _ = make_soil_mqtt_payloads("!n", {
            "raw_adc": raw, "battery_percent": 50,
            "battery_mv": 3700, "uptime_seconds": 1,
        }, DRY, WET)
        # The clamp applied to the percentage must NOT have touched the raw value.
        assert raw_pl["value"] == raw
        assert 0.0 <= pct_pl["value"] <= 100.0

    assert adc_to_percent(4095, DRY, WET) == 0.0
    assert adc_to_percent(500, DRY, WET) == 100.0


def test_degenerate_calibration_returns_none_rather_than_dividing_by_zero():
    assert adc_to_percent(1842, 2000, 2000) is None
    raw_pl, pct_pl, _ = make_soil_mqtt_payloads("!n", {
        "raw_adc": 1842, "battery_percent": 50,
        "battery_mv": 3700, "uptime_seconds": 1,
    }, 2000, 2000)
    # Raw still publishes even when the curve is unusable — it is authoritative.
    assert raw_pl["value"] == 1842
    assert pct_pl is None


def test_recalibration_changes_percent_but_not_raw():
    """The whole point of the change: retune without touching the measurement."""
    reading = {"raw_adc": 1842, "battery_percent": 85,
               "battery_mv": 4020, "uptime_seconds": 86400}
    a_raw, a_pct, _ = make_soil_mqtt_payloads("!n", reading, 3120, 1567)
    b_raw, b_pct, _ = make_soil_mqtt_payloads("!n", reading, 3500, 1200)
    assert a_raw["value"] == b_raw["value"] == 1842
    assert a_pct["value"] != b_pct["value"]


# --------------------------------------------------------------------------
# MQTT payload shapes — must match what mqtt_to_db.apply_payload() expects
# --------------------------------------------------------------------------

def test_mqtt_payload_shapes():
    r = extract_soil_reading(_packet(_encode()))
    raw_pl, pct_pl, bat_pl = make_soil_mqtt_payloads("!a1b2c3d4", r, DRY, WET)

    assert raw_pl["value"] == 1842 and raw_pl["fromId"] == "!a1b2c3d4"
    assert "ts" in raw_pl
    assert pct_pl["value"] == pytest.approx(82.29, abs=0.01)

    # apply_payload() reads exactly these keys off the battery topic.
    assert bat_pl["batteryLevel"] == 85.0
    assert bat_pl["batteryUsb"] is False
    assert bat_pl["uptimeSeconds"] == 86400
    # Voltage survives here, unlike the FORMAT B text path which drops it.
    assert bat_pl["voltage"] == 4.02


def test_external_power_maps_to_battery_usb():
    r = extract_soil_reading(_packet(_encode(battery_percent=101)))
    _, _, bat_pl = make_soil_mqtt_payloads("!n", r, DRY, WET)
    assert bat_pl["batteryUsb"] is True
    assert bat_pl["batteryLevel"] == 100.0


# --------------------------------------------------------------------------
# End-to-end: samples -> average -> protobuf -> Pi soil_raw
# --------------------------------------------------------------------------

def test_adc_survives_unchanged_end_to_end():
    """
    Mirrors the firmware exactly: 5 samples -> integer average -> raw_adc
    -> encode -> decode -> the value published to farm/sensors/soil/<id>/raw.
    """
    samples = [1840, 1844, 1841, 1843, 1842]
    averaged = sum(samples) // len(samples)  # firmware: total / ANALOG_SOIL_SAMPLES
    assert averaged == 1842

    wire = _encode(raw_adc=averaged)
    decoded = extract_soil_reading(_packet(wire))
    assert decoded["raw_adc"] == averaged

    raw_pl, _, _ = make_soil_mqtt_payloads("!a1b2c3d4", decoded, DRY, WET)
    # This is the value mqtt_to_db.apply_payload() assigns to NodeState.soil_raw.
    assert raw_pl["value"] == averaged == 1842
