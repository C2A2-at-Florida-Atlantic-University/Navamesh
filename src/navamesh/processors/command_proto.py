"""
command_proto.py — Encode downlink commands and decode node acknowledgements.

Wire contract with the RAK4631 firmware (src/modules/NavameshCommand.cpp):

    PortNum 258   navamesh.NavameshCommand   Pi -> node   (unicast or broadcast)
    PortNum 259   navamesh.NavameshAck       node -> Pi   (unicast)

These are deliberately NOT PortNum 256. Soil readings keep 256 to themselves, so
nothing here can be confused with a SoilReading and the deployed uplink path did not
have to change at all. Only 256 and 257 are assigned in upstream Meshtastic
(_PortNum_MAX is 511), so 258/259 cannot collide with a stock portnum.
"""

import base64
from typing import Optional

from google.protobuf.message import DecodeError

from navamesh.proto import navamesh_pb2

COMMAND_PORTNUM = 258
ACK_PORTNUM = 259

# Verb strings shared with reticulum_bridge.handle_command() and, transitively, with
# the Sideband wrapper's command_registry.py. Keep all three in step.
VERB_BLE = "ble"
VERB_INTERVAL = "interval"
VERB_QUIET = "quiet"

_VERB_TO_COMMAND_TYPE = {
    VERB_BLE: navamesh_pb2.BLE_WINDOW,
    VERB_INTERVAL: navamesh_pb2.SET_TELEMETRY_INTERVAL,
}

# Bounds mirror the firmware's clamps (NavameshCommand.cpp). Validating here too means
# an operator gets an immediate, readable rejection instead of silently having their
# value clamped by the node and having to notice the discrepancy in the ack.
BLE_WINDOW_MIN_MINUTES = 1
BLE_WINDOW_MAX_MINUTES = 240
# 60 s is the soil-calibration bench cadence; matches the firmware clamp. It is a bench value,
# not a field one (~480x the 8 h default), so the operator UI offers no preset below 5 min.
INTERVAL_MIN_SECONDS = 60
INTERVAL_MAX_SECONDS = 86400
QUIET_MIN_MINUTES = 1
QUIET_MAX_MINUTES = 4320


class CommandValidationError(ValueError):
    """The requested command is malformed or out of range."""


def _payload_bytes(payload) -> Optional[bytes]:
    """
    Normalise the packet payload to raw bytes.

    Same reasoning as soil_proto._payload_bytes: the meshtastic dependency is
    unpinned and hands us either bytes or a base64 str depending on version.
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


def _portnum_matches(value, expected: int) -> bool:
    """
    True if a packet's reported portnum is `expected`.

    258/259 are not members of meshtastic's PortNum enum, so unlike PRIVATE_APP the
    library cannot render them as a name. Depending on version we get the bare int or
    its string form; accept either rather than betting on one.
    """
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return value == expected
    if isinstance(value, str):
        return value.strip() == str(expected)
    return False


def encode_command(
    verb: str,
    command_id: int,
    value: Optional[int] = None,
    quiet_on: Optional[bool] = None,
) -> bytes:
    """
    Build a NavameshCommand payload.

    `command_id` must be a strictly increasing non-zero integer per node. The firmware
    uses it as a replay guard and rejects anything not greater than the last one it
    accepted -- except an exact repeat, which it re-acknowledges so our own retries
    still get an answer.

    Raises CommandValidationError on anything the node would reject or clamp.
    """
    if not isinstance(command_id, int) or command_id <= 0:
        raise CommandValidationError("command_id must be a positive integer")

    cmd = navamesh_pb2.NavameshCommand()
    cmd.command_id = command_id

    if verb == VERB_BLE:
        if value is None:
            raise CommandValidationError("ble requires a duration in minutes")
        if not BLE_WINDOW_MIN_MINUTES <= value <= BLE_WINDOW_MAX_MINUTES:
            raise CommandValidationError(
                f"ble window must be {BLE_WINDOW_MIN_MINUTES}-{BLE_WINDOW_MAX_MINUTES} minutes"
            )
        cmd.command_type = navamesh_pb2.BLE_WINDOW
        cmd.duration_minutes = value

    elif verb == VERB_INTERVAL:
        if value is None:
            raise CommandValidationError("interval requires a value in seconds")
        if not INTERVAL_MIN_SECONDS <= value <= INTERVAL_MAX_SECONDS:
            raise CommandValidationError(
                f"interval must be {INTERVAL_MIN_SECONDS}-{INTERVAL_MAX_SECONDS} seconds"
            )
        cmd.command_type = navamesh_pb2.SET_TELEMETRY_INTERVAL
        cmd.interval_seconds = value

    elif verb == VERB_QUIET:
        if quiet_on is None:
            raise CommandValidationError("quiet requires on or off")
        if quiet_on:
            cmd.command_type = navamesh_pb2.QUIET_MODE_ENTER
            if value is None:
                # 0 means "use the node's own default" (24 h), per the proto. Sending 0
                # rather than repeating 1440 here keeps a single source of truth for the
                # default, and avoids what an earlier version did: defaulting to
                # QUIET_MAX_MINUTES, so an unqualified "pause this node" silenced it for
                # the full 3-day ceiling instead of a day.
                cmd.duration_minutes = 0
            else:
                if not QUIET_MIN_MINUTES <= value <= QUIET_MAX_MINUTES:
                    raise CommandValidationError(
                        f"quiet duration must be {QUIET_MIN_MINUTES}-{QUIET_MAX_MINUTES} minutes"
                    )
                cmd.duration_minutes = value
        else:
            cmd.command_type = navamesh_pb2.QUIET_MODE_EXIT

    else:
        raise CommandValidationError(f"unknown verb {verb!r}")

    return cmd.SerializeToString()


def is_command_ack(packet: dict) -> bool:
    """Quick check: is this a PortNum 259 packet we should try to decode?"""
    decoded = packet.get("decoded") or {}
    return _portnum_matches(decoded.get("portnum"), ACK_PORTNUM)


def extract_command_ack(packet: dict) -> Optional[dict]:
    """
    Parse a navamesh.NavameshAck out of a PortNum 259 packet.

    Returns a dict with keys command_id, command_type, ok, applied_value, or None if
    this is not a decodable ack. A decode failure must never raise.

    command_id == 0 marks an UNSOLICITED ack: the node's quiet mode self-expired and it
    resumed on its own without anyone sending an exit command. Callers should treat that
    as a state notification rather than trying to correlate it to a pending request.
    """
    if not is_command_ack(packet):
        return None

    decoded = packet.get("decoded") or {}
    raw = _payload_bytes(decoded.get("payload"))
    if raw is None:
        return None

    # Unlike PortNum 256, an empty payload here is not ambiguous -- 259 is ours alone.
    # But an all-default ack still tells us nothing actionable (command_id 0, ok False),
    # so there is no reason to admit it.
    if raw == b"":
        return None

    ack = navamesh_pb2.NavameshAck()
    try:
        ack.ParseFromString(raw)
    except (DecodeError, ValueError):
        return None

    return {
        "command_id": int(ack.command_id),
        "command_type": int(ack.command_type),
        "command_type_name": navamesh_pb2.NavameshCommandType.Name(ack.command_type)
        if ack.command_type in navamesh_pb2.NavameshCommandType.values()
        else str(ack.command_type),
        "ok": bool(ack.ok),
        "applied_value": int(ack.applied_value),
        "unsolicited": int(ack.command_id) == 0,
    }
