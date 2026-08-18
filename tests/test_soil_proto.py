"""
Tests for the FORMAT C path: navamesh.SoilReading protobuf on PortNum 256.

The central guarantee under test is that the raw ADC the node measured survives
byte-for-byte into what the Pi stores, and that the derived band never alters it.
"""

import base64

import pytest

from navamesh.calibration import (
    DAMP as DAMP_BAND,
    DRY as DRY_BAND,
    WET as WET_BAND,
    adc_to_band,
    adc_to_percent,
)
from navamesh.processors.soil_proto import (
    extract_soil_reading,
    is_soil_reading,
    make_soil_mqtt_payloads,
)
from navamesh.proto import navamesh_pb2

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
# Calibration — bands, and the raw ADC is never altered
# --------------------------------------------------------------------------

def _reading(raw_adc):
    return {"raw_adc": raw_adc, "battery_percent": 50,
            "battery_mv": 3700, "uptime_seconds": 1}


# Every individual probe position from the bench calibration (15-18 Aug 2026),
# with the band it must classify into. If a threshold is ever retuned and one of
# these lands on the wrong side, that retune contradicts the measurements.
BENCH_POSITIONS = [
    (4095, DRY_BAND), (4095, DRY_BAND), (4095, DRY_BAND),   # 0% dry soil
    (4095, DRY_BAND), (4095, DRY_BAND),                     # 7% and 9.5%
    (980, DAMP_BAND), (2052, DAMP_BAND),                    # 10%
    (1900, DAMP_BAND), (1482, DAMP_BAND),
    (1060, DAMP_BAND), (1133, DAMP_BAND), (1308, DAMP_BAND),  # 15%
    (380, WET_BAND), (219, WET_BAND), (366, WET_BAND),        # 20%
    (223, WET_BAND), (387, WET_BAND), (231, WET_BAND),        # 25%
    (158, WET_BAND), (175, WET_BAND),                         # 30%
    (44, WET_BAND),                                           # free water
]


@pytest.mark.parametrize("raw_adc,expected", BENCH_POSITIONS)
def test_bench_measurements_classify_correctly(raw_adc, expected):
    assert adc_to_band(raw_adc) == expected


def test_rail_is_dry_not_zero_percent():
    """
    A reading on the 4095 rail means "no conductive path" — which is equally
    true of bone-dry soil, of soil at 9.5% moisture, and of a probe sitting in
    an air gap. The old two-point model reported all three as 0.0%; they are
    now DRY with no percentage at all, because the sensor cannot tell them apart.
    """
    assert adc_to_band(4095) == DRY_BAND
    assert adc_to_percent(4095) is None


def test_saturated_readings_get_no_percentage():
    """20% (322) and 25% (280) are statistically identical — 42 counts apart with
    sd ~90 on each. Reporting a number there would invent precision."""
    for raw in (322, 280, 167, 44):
        assert adc_to_band(raw) == WET_BAND
        assert adc_to_percent(raw) is None


def test_percentage_only_inside_the_damp_band():
    assert adc_to_percent(1603) == pytest.approx(10.0, abs=0.1)
    assert adc_to_percent(1167) == pytest.approx(15.0, abs=0.1)
    # Monotonic across the band: wetter soil never reports a lower percentage.
    pcts = [adc_to_percent(a) for a in (1603, 1400, 1167, 700, 450)]
    assert all(b >= a for a, b in zip(pcts, pcts[1:]))


def test_raw_publishes_even_when_percentage_is_absent():
    raw_pl, pct_pl, band_pl, _ = make_soil_mqtt_payloads("!n", _reading(4095))
    # Raw is authoritative and always published, regardless of what we derive.
    assert raw_pl["value"] == 4095
    assert pct_pl is None
    assert band_pl["value"] == DRY_BAND


# --------------------------------------------------------------------------
# MQTT payload shapes — must match what mqtt_to_db.apply_payload() expects
# --------------------------------------------------------------------------

def test_mqtt_payload_shapes():
    r = extract_soil_reading(_packet(_encode()))
    raw_pl, pct_pl, band_pl, bat_pl = make_soil_mqtt_payloads("!a1b2c3d4", r)

    assert raw_pl["value"] == 1842 and raw_pl["fromId"] == "!a1b2c3d4"
    assert "ts" in raw_pl
    assert band_pl["value"] == DAMP_BAND
    # 1842 sits above CURVE[0] (1603 = 10%), i.e. drier than the driest measured
    # point in the DAMP band, so the estimate clamps to 10.0 rather than
    # extrapolating past the data.
    assert pct_pl["value"] == pytest.approx(10.0, abs=0.5)

    # apply_payload() reads exactly these keys off the battery topic.
    assert bat_pl["batteryLevel"] == 85.0
    assert bat_pl["batteryUsb"] is False
    assert bat_pl["uptimeSeconds"] == 86400
    # Voltage survives here, unlike the FORMAT B text path which drops it.
    assert bat_pl["voltage"] == 4.02


def test_external_power_maps_to_battery_usb():
    r = extract_soil_reading(_packet(_encode(battery_percent=101)))
    _, _, _, bat_pl = make_soil_mqtt_payloads("!n", r)
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

    raw_pl, _, _, _ = make_soil_mqtt_payloads("!a1b2c3d4", decoded)
    # This is the value mqtt_to_db.apply_payload() assigns to NodeState.soil_raw.
    assert raw_pl["value"] == averaged == 1842
