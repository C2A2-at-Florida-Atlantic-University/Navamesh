"""
soil_proto.py — Decode the structured Navamesh soil reading (PortNum 256).

FORMAT C (RAK4631 firmware, authoritative):
    A navamesh.SoilReading protobuf on Meshtastic PortNum 256 (PRIVATE_APP),
    carrying the RAW averaged ADC count. The node performs no calibration; the
    DRY/DAMP/WET band is derived here (see navamesh.calibration).

This supersedes the FORMAT B text string in soil_text.py, which is retained so
nodes running older firmware keep working. Unlike FORMAT B, this path also
carries battery voltage, which the text regex silently dropped.
"""

import base64
import time
from typing import Optional, Tuple

from google.protobuf.message import DecodeError

from navamesh.calibration import adc_to_band, adc_to_percent
from navamesh.proto import navamesh_pb2

# Meshtastic's python library reports portnum as the enum *name*.
PRIVATE_APP_PORTNUM = "PRIVATE_APP"

# Stamped into every soil payload this module produces. It marks the value as
# derived from the authoritative PRIVATE_APP raw-ADC path, which lets the bridge
# tell a current retained MQTT message apart from a stale one left over from the
# legacy "Soil: XX%" text era. Extra keys are ignored by mqtt_to_db.apply_payload().
SOIL_SOURCE = "private_app_raw_adc"


def _payload_bytes(payload) -> Optional[bytes]:
    """
    Normalise the packet payload to raw bytes.

    The meshtastic dependency is unpinned, and depending on version `payload`
    arrives either already decoded to bytes or as a base64 str (an artifact of
    protobuf's MessageToDict). Accept both rather than betting on one.
    """
    if payload is None:
        return None
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, str):
        try:
            return base64.b64decode(payload)
        except Exception:
            return None
    return None


def is_soil_reading(packet: dict) -> bool:
    """Quick check: is this a PRIVATE_APP packet we should try to decode?"""
    decoded = packet.get("decoded") or {}
    return decoded.get("portnum") == PRIVATE_APP_PORTNUM


def extract_soil_reading(packet: dict) -> Optional[dict]:
    """
    Parse a navamesh.SoilReading out of a PRIVATE_APP packet.

    Returns a dict with keys:
        raw_adc         (int)   the node's averaged ADC, stored verbatim
        battery_percent (int)   0 means unknown on RAK4631 (BATTERY_PIN builds)
        battery_mv      (int)   millivolts
        uptime_seconds  (int)

    Returns None if this is not a decodable SoilReading. PortNum 256 is shared
    by every private application on the mesh, so a decode failure is a normal,
    expected outcome and must never raise.

    Note: proto3 omits zero-valued scalars from the wire, so absent fields
    correctly read back as 0.
    """
    if not is_soil_reading(packet):
        return None

    decoded = packet.get("decoded") or {}
    raw = _payload_bytes(decoded.get("payload"))
    if raw is None:
        return None

    # An empty payload is rejected on purpose. Proto3 omits zero-valued scalars, so
    # an all-zeros SoilReading encodes to zero bytes -- but so does *any* other
    # application's empty PRIVATE_APP packet, and PortNum 256 is shared by every
    # private app on the mesh. Accepting b"" would let foreign traffic inject a
    # phantom soil_raw=0 into the dataset. A genuine all-zeros reading would require
    # uptime_seconds == 0, i.e. the node transmitting within one second of boot,
    # which cannot happen on a telemetry interval. Dropping that impossible case is
    # far cheaper than corrupting real data.
    if raw == b"":
        return None

    reading = navamesh_pb2.SoilReading()
    try:
        reading.ParseFromString(raw)
    except (DecodeError, ValueError):
        return None

    return {
        "raw_adc": int(reading.raw_adc),
        "battery_percent": int(reading.battery_percent),
        "battery_mv": int(reading.battery_mv),
        "uptime_seconds": int(reading.uptime_seconds),
    }


def make_soil_mqtt_payloads(
    from_id: str, reading: dict
) -> Tuple[dict, Optional[dict], dict, dict]:
    """
    Convert extract_soil_reading() output into MQTT-ready payloads.

    Returns (raw_payload, percent_payload, band_payload, battery_payload):
        raw_payload     -> farm/sensors/soil/<fromId>/raw
        percent_payload -> farm/sensors/soil/<fromId>/percent  (None outside the
                           DAMP band, where the sensor has no resolution)
        band_payload    -> farm/sensors/soil/<fromId>/band     (DRY/DAMP/WET)
        battery_payload -> farm/nodes/<fromId>/battery

    Payload shapes deliberately match what mqtt_to_db.apply_payload() already
    expects, so no ingestor changes are needed.

    The raw ADC is passed through untouched. The band is the authoritative
    derived value; the percentage is a coarse convenience and is omitted rather
    than fabricated wherever the bench data shows the probe cannot resolve.
    """
    ts = int(time.time())
    raw_adc = reading["raw_adc"]

    raw_payload = {
        "value": raw_adc,
        "fromId": from_id,
        "ts": ts,
        "source": SOIL_SOURCE,
    }

    band_payload = {
        "value": adc_to_band(raw_adc),
        "fromId": from_id,
        "ts": ts,
        "source": SOIL_SOURCE,
    }

    percent = adc_to_percent(raw_adc)
    percent_payload = None
    if percent is not None:
        percent_payload = {
            "value": percent,
            "fromId": from_id,
            "ts": ts,
            "source": SOIL_SOURCE,
        }

    battery_percent = reading["battery_percent"]
    battery_payload = {
        "fromId": from_id,
        "ts": ts,
        # >100 means externally powered on boards without BATTERY_PIN.
        "batteryLevel": float(min(battery_percent, 100)),
        "batteryUsb": battery_percent > 100,
        "uptimeSeconds": reading["uptime_seconds"],
        # Unlike the FORMAT B text path, voltage survives here.
        "voltage": round(reading["battery_mv"] / 1000.0, 3),
    }

    return raw_payload, percent_payload, band_payload, battery_payload
