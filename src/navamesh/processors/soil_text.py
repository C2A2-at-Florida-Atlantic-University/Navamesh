"""
soil_text.py — Parse battery/uptime from legacy Meshtastic text messages.

Format B (LEGACY firmware):
    "Soil: 47% | Bat: 82% | Up: 1h 23m"
    "Soil: 0%  | Bat: USB | Up: 0h 3m"

!! THE SOIL PERCENTAGE IN THESE MESSAGES IS NO LONGER AUTHORITATIVE. !!

Soil moisture now comes exclusively from the navamesh.SoilReading protobuf on
PortNum 256 (see soil_proto.py), where the node reports a RAW ADC count and the
percentage is derived here on the Pi. A node-computed percentage cannot be
recalibrated after the fact, which is the whole reason for the change.

This module is retained only to salvage battery / voltage / uptime from nodes
still running old firmware. It deliberately does NOT produce a soil payload:
a node on old firmware simply has no valid soil measurement until it is
reflashed. parse_status_message() still returns soil_percent, but that value is
for logging only and must never be published to farm/sensors/soil/+/percent.
"""

import re
import time
from typing import Optional

# ---------------------------------------------------------------------------
# FORMAT B  — device status string (RAK4631 firmware)
# ---------------------------------------------------------------------------

# Matches the numeric part of "Soil: 47%" or "Soil: 0%"
_SOIL_RE = re.compile(r"Soil\s*:\s*(\d+(?:\.\d+)?)\s*%", re.IGNORECASE)

# Matches "Bat: 82%" or "Bat: USB"  (USB → treated as 100 %)
_BAT_RE = re.compile(r"Bat\s*:\s*(USB|\d+(?:\.\d+)?)\s*%?", re.IGNORECASE)

# Matches "Up: 1h 23m"  (hours and/or minutes are each optional)
#   "Up: 0h 3m"  →  0 hours, 3 minutes
#   "Up: 2h"     →  2 hours, 0 minutes
#   "Up: 45m"    →  0 hours, 45 minutes
_UP_RE = re.compile(
    r"Up\s*:\s*(?:(\d+)\s*h)?\s*(?:(\d+)\s*m)?",
    re.IGNORECASE,
)


def is_status_message(text: str) -> bool:
    """
    Quick check: does this text look like a FORMAT B status string?
    Used by main.py to route the packet to the right parser.
    """
    return bool(_SOIL_RE.search(text or ""))


def parse_status_message(text: str) -> Optional[dict]:
    """
    Parse a FORMAT B status string into a structured dict.

    Returns a dict with keys:
        soil_percent   (float, 0-100)
        battery_level  (float, 0-100;  USB charging → 100.0)
        battery_usb    (bool,  True when the device reported "USB")
        uptime_seconds (int)

    Returns None if the minimum required field (soil %) cannot be parsed.

    Example input:
        "Soil: 47% | Bat: 82% | Up: 1h 23m"
    """
    text = text or ""

    # --- Soil % -----------------------------------------------------------
    soil_match = _SOIL_RE.search(text)
    if not soil_match:
        return None  # not a status message we can use
    soil_pct = float(soil_match.group(1))

    # --- Battery ----------------------------------------------------------
    bat_match = _BAT_RE.search(text)
    battery_usb = False
    battery_level: Optional[float] = None
    if bat_match:
        raw_bat = bat_match.group(1)
        if raw_bat.upper() == "USB":
            battery_usb = True
            battery_level = 100.0   # USB power = treat as full
        else:
            battery_level = float(raw_bat)

    # --- Uptime -----------------------------------------------------------
    up_match = _UP_RE.search(text)
    uptime_seconds = 0
    if up_match:
        hours = int(up_match.group(1) or 0)
        minutes = int(up_match.group(2) or 0)
        uptime_seconds = hours * 3600 + minutes * 60

    return {
        "soil_percent": round(soil_pct, 2),
        "battery_level": battery_level,
        "battery_usb": battery_usb,
        "uptime_seconds": uptime_seconds,
    }


def make_status_battery_payload(from_id: str, parsed: dict) -> Optional[dict]:
    """
    Convert the output of parse_status_message() into a battery MQTT payload.

    Returns None if no battery data was found in the message.

    battery_payload → publish to farm/nodes/<fromId>/battery

    NOTE: there is deliberately no soil payload here. The node-computed
    percentage in a legacy status string is not authoritative and must not reach
    farm/sensors/soil/<fromId>/percent — that topic is now fed exclusively by the
    PRIVATE_APP raw-ADC path in soil_proto.py. This function was previously
    make_status_mqtt_payloads() and returned both.
    """
    if parsed.get("battery_level") is None:
        return None

    return {
        "fromId": from_id,
        "ts": int(time.time()),
        "batteryLevel": parsed["battery_level"],
        "batteryUsb": parsed["battery_usb"],
        "uptimeSeconds": parsed["uptime_seconds"],
        # voltage not present in text messages — left absent
    }
