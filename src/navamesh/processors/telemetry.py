import time
from typing import Optional, Dict

def extract_battery(packet: dict) -> Optional[Dict]:
    """
    Extract battery/voltage from Meshtastic TELEMETRY_APP packets.
    Meshtastic Python decoding often places it at:
      packet["decoded"]["telemetry"]["deviceMetrics"]
    """
    decoded = packet.get("decoded") or {}
    if not isinstance(decoded, dict):
        return None
    if decoded.get("portnum") != "TELEMETRY_APP":
        return None
    tel = decoded.get("telemetry") or {}
    if not isinstance(tel, dict):
        return None
    dm = tel.get("deviceMetrics") or {}
    if not isinstance(dm, dict):
        return None

    from_id = packet.get("fromId") or packet.get("user", {}).get("id") or "unknown"
    ts = int(time.time())

    # Some fields may be missing depending on firmware/settings
    battery_level = dm.get("batteryLevel")
    voltage = dm.get("voltage")

    # If both are missing, skip publishing
    if battery_level is None and voltage is None:
        return None

    return {
        "ts": ts,
        "fromId": from_id,
        "batteryLevel": battery_level,
        "voltage": voltage,
        "uptimeSeconds": dm.get("uptimeSeconds"),
        "channelUtilization": dm.get("channelUtilization"),
        "airUtilTx": dm.get("airUtilTx"),
    }
