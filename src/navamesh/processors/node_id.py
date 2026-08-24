"""Resolve a packet's sender to a Meshtastic node id.

`fromId` is not always present. The Meshtastic library fills it by looking the
numeric sender up in the interface's own node database, so it is empty for any
node that transmits before its NodeInfo has been heard -- which is the normal
order after a gateway restart, since NodeInfo is broadcast on the node's own
schedule and a soil reading is not.

Filing those readings under "unknown" misattributes them, and does it in the way
that hides itself: once NodeInfo lands the readings start landing correctly, so
the gap reads as a brief outage rather than as measurements written to the wrong
node. The numeric `from` field is present on every packet, and a Meshtastic id is
just "!" + the nodenum in lowercase hex, so nothing has to be waited for.
"""
from typing import Optional

# Reserved by Meshtastic: 0 is "unset" and 0xFFFFFFFF is the broadcast address.
# Neither can be a real sender, so neither may become a node id.
_UNSET_NODENUM = 0
_BROADCAST_NODENUM = 0xFFFFFFFF


def nodenum_to_id(value) -> Optional[str]:
    """Format a numeric nodenum as a Meshtastic id, or None if it cannot be one.

    Accepts the int the library normally provides and the decimal string some
    JSON paths produce. The mask is deliberate: a nodenum is a uint32, but it
    arrives sign-extended on some paths, so 0x9aed49xx can present as negative.
    """
    if isinstance(value, bool):  # bool is an int subclass; never a nodenum
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            value = int(value, 10)
        except ValueError:
            return None
    if not isinstance(value, int):
        return None

    nodenum = value & 0xFFFFFFFF
    if nodenum in (_UNSET_NODENUM, _BROADCAST_NODENUM):
        return None
    return f"!{nodenum:08x}"


def resolve_node_id(packet: dict) -> Optional[str]:
    """Best available node id for a packet's sender, or None if truly absent.

    Order matters. `fromId` first because when the library has resolved it, it is
    the same string every other consumer already keys on; the user id next
    because a NodeInfo packet carries it authoritatively; the numeric field last
    because it always works but reconstructs what the other two state directly.
    """
    if not isinstance(packet, dict):
        return None

    from_id = packet.get("fromId")
    if isinstance(from_id, str) and from_id.strip():
        return from_id.strip()

    decoded = packet.get("decoded")
    user = decoded.get("user") if isinstance(decoded, dict) else None
    if not isinstance(user, dict):
        user = packet.get("user")
    if isinstance(user, dict):
        user_id = user.get("id")
        if isinstance(user_id, str) and user_id.strip():
            return user_id.strip()

    return nodenum_to_id(packet.get("from"))
