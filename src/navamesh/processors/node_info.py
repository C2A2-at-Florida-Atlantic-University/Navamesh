import time
from typing import Optional, Dict


def _clean_name(value) -> Optional[str]:
    """Return a stripped non-empty string, else None."""
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return None


def extract_node_info(packet: dict) -> Optional[Dict]:
    """
    Extract owner/user info from Meshtastic NODEINFO_APP packets.
    Meshtastic Python decoding usually places it at:
      packet["decoded"]["user"] = {"id": "!abc...", "longName": ..., "shortName": ...}
    but field naming varies across firmware/library versions (long_name vs
    longName, user at the packet top level), so be tolerant.

    Returns None unless a stable node ID and at least one usable name exist.
    """
    decoded = packet.get("decoded") or {}
    if not isinstance(decoded, dict):
        decoded = {}

    user = decoded.get("user")
    if not isinstance(user, dict):
        user = packet.get("user")
    if not isinstance(user, dict):
        return None

    long_name = _clean_name(user.get("longName")) or _clean_name(user.get("long_name"))
    short_name = _clean_name(user.get("shortName")) or _clean_name(user.get("short_name"))
    if long_name is None and short_name is None:
        return None

    from_id = _clean_name(user.get("id")) or _clean_name(packet.get("fromId"))
    if from_id is None:
        return None

    return {
        "ts": int(time.time()),
        "fromId": from_id,
        "longName": long_name,
        "shortName": short_name,
    }
