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
VERB_SETLOC = "setloc"
# Asks a node what firmware it runs and changes nothing else. Unlike every other verb here
# it is a probe, not a write -- but it still travels the write path (command bus, command_id,
# ack correlation) because that is what carries a reply back, so it is gated the same way.
VERB_FWINFO = "fwinfo"

# Verbs that may never go to ^all. A broadcast setloc would hand every node in the field the
# same coordinates in one unrecoverable transmission, and nobody has ever meant that.
#
# Lives here, beside the verbs themselves, because both gates that enforce it need it:
# reticulum_bridge (so the operator gets a readable refusal) and _bridge (the last gate
# before RF, which must hold even for a command published by something else).
UNICAST_ONLY_VERBS = (VERB_SETLOC,)

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

# Fixed position. The firmware refuses anything outside these, and refuses 0/0, rather than
# clamping -- a clamped coordinate is a different place, so moving a node to the edge of the
# valid range would be worse than not moving it. Mirrored here for a readable rejection.
LATITUDE_MIN_DEGREES = -90.0
LATITUDE_MAX_DEGREES = 90.0
LONGITUDE_MIN_DEGREES = -180.0
LONGITUDE_MAX_DEGREES = 180.0

# Degrees -> the integer scaling meshtastic.Position uses for latitude_i/longitude_i.
_DEGREES_TO_I = 1e7


# Lives here rather than in reticulum_bridge so that navamesh-cmd -- the operator's
# surface -- can accept the same units without importing the RNS/LXMF stack.
# Suffixes accepted on an interval value, and what each is worth in seconds.
#
# A bare number stays seconds, so every caller that predates this -- the app, navamesh-cmd,
# anything scripted -- keeps working untouched. The suffixes exist because seconds is the
# protocol's unit, not a person's: nobody standing in a field thinks "10800", they think
# "3 hours", and asking them to do the arithmetic is how 3600 gets sent when 36000 was meant.
_INTERVAL_UNITS = {
    "s": 1, "sec": 1, "secs": 1, "second": 1, "seconds": 1,
    "m": 60, "min": 60, "mins": 60, "minute": 60, "minutes": 60,
    "h": 3600, "hr": 3600, "hrs": 3600, "hour": 3600, "hours": 3600,
}


def parse_interval_value(arg: str):
    """Parse an interval as seconds, accepting "1800", "30m", "3 h", "2hours".

    Returns (seconds, error_message); exactly one is None.

    The unit is applied before the range check, so "30m" is rejected against 60-86400 s the
    same way 1800 is. Rejecting rather than clamping: an operator who typed "25h" meant
    something this system cannot do, and silently giving them 24 h would look like it worked.
    """
    text = str(arg).strip().lower().replace(" ", "")
    if not text:
        return None, "no interval given."

    digits = text
    unit = ""
    for i, ch in enumerate(text):
        if not (ch.isdigit() or ch == "."):
            digits, unit = text[:i], text[i:]
            break

    if not digits:
        return None, f"'{arg}' is not a number."
    try:
        # float, not int: "1.5h" is a natural thing to type and rounds to a whole second.
        magnitude = float(digits)
    except ValueError:
        return None, f"'{arg}' is not a number."

    if unit and unit not in _INTERVAL_UNITS:
        return None, (
            f"'{unit}' is not a unit I know. Use seconds (1800), minutes (30m) or hours (2h)."
        )

    seconds = int(round(magnitude * _INTERVAL_UNITS.get(unit, 1)))
    if not INTERVAL_MIN_SECONDS <= seconds <= INTERVAL_MAX_SECONDS:
        # Echo what they typed as well as what it came to. "Interval must be 60-86400
        # seconds" against an input of "25h" asks the reader to convert twice to see why.
        return None, (
            f"Interval must be {INTERVAL_MIN_SECONDS}-{INTERVAL_MAX_SECONDS} seconds "
            f"({INTERVAL_MIN_SECONDS // 60} min to 24 h). "
            f"'{arg}' is {seconds} seconds."
        )
    return seconds, None


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
    lat: Optional[float] = None,
    lon: Optional[float] = None,
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

    elif verb == VERB_FWINFO:
        # No arguments by design. The node reports its version on every ack, so this
        # command's only job is to elicit one without altering the node it is asking about.
        cmd.command_type = navamesh_pb2.GET_FIRMWARE_INFO

    elif verb == VERB_SETLOC:
        if lat is None or lon is None:
            raise CommandValidationError("setloc requires a latitude and a longitude")
        if not LATITUDE_MIN_DEGREES <= lat <= LATITUDE_MAX_DEGREES:
            raise CommandValidationError(
                f"latitude must be {LATITUDE_MIN_DEGREES} to {LATITUDE_MAX_DEGREES}"
            )
        if not LONGITUDE_MIN_DEGREES <= lon <= LONGITUDE_MAX_DEGREES:
            raise CommandValidationError(
                f"longitude must be {LONGITUDE_MIN_DEGREES} to {LONGITUDE_MAX_DEGREES}"
            )

        latitude_i = int(round(lat * _DEGREES_TO_I))
        longitude_i = int(round(lon * _DEGREES_TO_I))

        # Checked after scaling, not before: a fix of 1e-9 degrees is not zero as a float but
        # rounds to 0/0 on the wire, and the node would reject it. Fail here with a reason
        # instead of letting it become an opaque nak two radio hops away.
        if latitude_i == 0 and longitude_i == 0:
            raise CommandValidationError(
                "refusing to set 0, 0 -- that usually means the sender had no GPS fix"
            )

        cmd.command_type = navamesh_pb2.SET_LOCATION
        cmd.latitude_i = latitude_i
        cmd.longitude_i = longitude_i

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

    A SET_LOCATION ack additionally carries applied_lat/applied_lon: the coordinates the
    node actually stored, in degrees. They are None for every other command type, and also
    for a node running firmware that predates SET_LOCATION, whose acks simply omit the
    fields and decode to 0/0.

    Every ack also carries firmware_version, the node's Meshtastic APP_VERSION string
    ("2.7.20.200289a"). It is "" from a node running a build older than the field, which is
    itself the answer to "has this one been flashed yet?" -- so an empty string is reported
    as None rather than passed through as a falsy version.

    command_id == 0 marks an UNSOLICITED ack, of which there are now two kinds, told apart
    by command_type: QUIET_MODE_EXIT means the node's quiet mode self-expired and it resumed
    on its own, and GET_FIRMWARE_INFO is the announce every node sends once at boot. Neither
    correlates to a pending request; callers should treat both as state notifications.
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

    applied_lat = applied_lon = None
    if ack.command_type == navamesh_pb2.SET_LOCATION and not (
        ack.applied_latitude_i == 0 and ack.applied_longitude_i == 0
    ):
        applied_lat = ack.applied_latitude_i / _DEGREES_TO_I
        applied_lon = ack.applied_longitude_i / _DEGREES_TO_I

    return {
        "command_id": int(ack.command_id),
        "command_type": int(ack.command_type),
        "command_type_name": navamesh_pb2.NavameshCommandType.Name(ack.command_type)
        if ack.command_type in navamesh_pb2.NavameshCommandType.values()
        else str(ack.command_type),
        "ok": bool(ack.ok),
        "applied_value": int(ack.applied_value),
        "applied_lat": applied_lat,
        "applied_lon": applied_lon,
        "firmware_version": (ack.firmware_version.strip() or None),
        "unsolicited": int(ack.command_id) == 0,
    }
