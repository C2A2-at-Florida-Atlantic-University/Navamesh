import time
from typing import Optional, Dict

from .node_id import resolve_node_id

def extract_position(packet: dict) -> Optional[Dict]:
    ts = int(time.time())
    from_id = resolve_node_id(packet) or "unknown"

    lat = lon = alt = sats = hdop = None

    decoded = packet.get("decoded") or {}
    if isinstance(decoded, dict):
        pos = decoded.get("position") or decoded.get("pos")
        if isinstance(pos, dict):
            lat = pos.get("latitude") or pos.get("lat")
            lon = pos.get("longitude") or pos.get("lon")
            alt = pos.get("altitude") or pos.get("alt")
            sats = pos.get("satsInView") or pos.get("sats")
            hdop = pos.get("hdop")

    if lat is None or lon is None:
        pos = packet.get("position")
        if isinstance(pos, dict):
            if "latitudeI" in pos and "longitudeI" in pos:
                lat = pos["latitudeI"] / 1e7
                lon = pos["longitudeI"] / 1e7
                alt = pos.get("altitude")
            else:
                lat = pos.get("latitude") or pos.get("lat")
                lon = pos.get("longitude") or pos.get("lon")
                alt = pos.get("altitude") or pos.get("alt")
            sats = sats or pos.get("satsInView") or pos.get("sats")
            hdop = hdop or pos.get("hdop")

    if lat is None or lon is None:
        return None

    return {
        "ts": ts,
        "fromId": from_id,
        "lat": lat,
        "lon": lon,
        "alt": alt,
        "sats": sats,
        "hdop": hdop,
    }