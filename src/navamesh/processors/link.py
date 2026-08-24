import time
from typing import Optional, Dict

from .node_id import resolve_node_id

def extract_link(packet: dict) -> Optional[Dict]:
    ts = int(time.time())
    from_id = resolve_node_id(packet) or "unknown"

    rx_rssi = packet.get("rxRssi")
    rx_snr = packet.get("rxSnr")

    if rx_rssi is None and rx_snr is None:
        return None

    return {
        "ts": ts,
        "fromId": from_id,
        "rxRssi": rx_rssi,
        "rxSnr": rx_snr,
        "hopLimit": packet.get("hopLimit"),
        "hopStart": packet.get("hopStart"),
        "relayNode": packet.get("relayNode"),
        "transportMechanism": packet.get("transportMechanism"),
    }