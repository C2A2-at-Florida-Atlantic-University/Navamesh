"""Two features that both exist because a number on the wire is not what a person means.

A node's firmware version had no carrier at all -- `meshtastic_User` has no version field
and `DeviceMetadata` never leaves the local serial/BLE link -- so "which build is this
node running?" was answered by elimination, twice, on 2026-08-21. It now rides every ack
and is announced unprompted at boot.

And the reporting interval was seconds-only, which is the protocol's unit rather than a
farmer's: nobody thinks "10800", they think "3 hours", and asking them to convert is how
3600 gets sent when 36000 was meant.
"""
import pytest

from navamesh.processors.command_proto import (
    parse_interval_value,
    encode_command,
    extract_command_ack,
    INTERVAL_MIN_SECONDS,
    INTERVAL_MAX_SECONDS,
    VERB_FWINFO,
    VERB_INTERVAL,
    ACK_PORTNUM,
)
from navamesh.proto import navamesh_pb2


# ── Interval units ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("text,expected", [
    ("1800", 1800),      # bare number stays seconds -- every existing caller is unchanged
    ("60", 60),
    ("30m", 1800),
    ("30min", 1800),
    ("30 minutes", 1800),
    ("2h", 7200),
    ("2hr", 7200),
    ("2 hours", 7200),
    ("1.5h", 5400),      # a natural thing to type, and it lands on a whole second
    ("300s", 300),
    ("24h", 86400),      # exactly the ceiling
    ("1m", 60),          # exactly the floor
])
def test_units_convert_to_seconds(text, expected):
    seconds, error = parse_interval_value(text)
    assert error is None, error
    assert seconds == expected


def test_a_bare_number_is_still_seconds():
    """The app, navamesh-cmd and anything scripted all send seconds. Changing what an
    unsuffixed number means would silently retune the fleet by a factor of 60."""
    assert parse_interval_value("28800")[0] == 28800


@pytest.mark.parametrize("text", ["25h", "0m", "10s", "2d", "", "abc", "h"])
def test_out_of_range_or_unparseable_is_refused(text):
    seconds, error = parse_interval_value(text)
    assert seconds is None
    assert error


def test_range_error_shows_the_converted_value():
    """'Interval must be 60-86400 seconds' against an input of '25h' asks the reader to
    convert twice to see why it was refused."""
    _, error = parse_interval_value("25h")
    assert "90000" in error


def test_units_are_bounded_exactly_as_bare_seconds_are():
    """The unit is applied before the range check, so a suffix cannot smuggle a value
    past the bound that the equivalent number in seconds would fail."""
    assert parse_interval_value(f"{INTERVAL_MAX_SECONDS + 1}")[0] is None
    assert parse_interval_value("1441m")[0] is None   # 86460 s, one minute over
    assert parse_interval_value("1440m")[0] == INTERVAL_MAX_SECONDS


def test_an_unknown_unit_names_itself():
    _, error = parse_interval_value("30x")
    assert "'x'" in error


# ── fwinfo encodes as a real command ────────────────────────────────────────────

def test_fwinfo_encodes_with_no_value():
    """The only verb taking no argument: it asks a question rather than setting one."""
    raw = encode_command(VERB_FWINFO, command_id=42)
    cmd = navamesh_pb2.NavameshCommand()
    cmd.ParseFromString(raw)
    assert cmd.command_type == navamesh_pb2.GET_FIRMWARE_INFO
    assert cmd.command_id == 42
    # Nothing else set: a probe that carried a stray interval would apply it.
    assert cmd.interval_seconds == 0
    assert cmd.duration_minutes == 0
    assert cmd.latitude_i == 0 and cmd.longitude_i == 0


def test_interval_still_requires_seconds_at_the_encoder():
    """The encoder is the last gate before RF and speaks seconds only -- unit parsing
    happens above it, so a raw '30m' must never reach here and be silently accepted."""
    from navamesh.processors.command_proto import CommandValidationError
    with pytest.raises(CommandValidationError):
        encode_command(VERB_INTERVAL, 1, value=INTERVAL_MIN_SECONDS - 1)


# ── The version comes back on the ack ───────────────────────────────────────────

def _ack_packet(**fields) -> dict:
    ack = navamesh_pb2.NavameshAck(**fields)
    return {"decoded": {"portnum": ACK_PORTNUM, "payload": ack.SerializeToString()}}


def test_every_ack_carries_the_version_not_just_a_fwinfo_one():
    """The version is most wanted about a command that just failed: an ok=False from a
    node too old to have the handler is indistinguishable from a rejected value."""
    pkt = _ack_packet(command_id=7, command_type=navamesh_pb2.SET_LOCATION,
                      ok=False, firmware_version="2.7.20.200289a")
    ack = extract_command_ack(pkt)
    assert ack["firmware_version"] == "2.7.20.200289a"
    assert ack["ok"] is False


def test_a_node_too_old_to_report_a_version_reads_as_None_not_empty_string():
    """Empty means 'firmware predates this field', which is a real answer. Passing ""
    through would have it render as a version in any f-string that does not test it."""
    pkt = _ack_packet(command_id=7, command_type=navamesh_pb2.BLE_WINDOW, ok=True)
    assert extract_command_ack(pkt)["firmware_version"] is None


def test_the_boot_announce_decodes_as_unsolicited():
    """command_id 0 with GET_FIRMWARE_INFO is a node saying what it runs, unprompted,
    once per boot. There is no request row to correlate it to."""
    pkt = _ack_packet(command_id=0, command_type=navamesh_pb2.GET_FIRMWARE_INFO,
                      ok=True, firmware_version="2.7.20.abc1234")
    ack = extract_command_ack(pkt)
    assert ack["unsolicited"] is True
    assert ack["command_type_name"] == "GET_FIRMWARE_INFO"
    assert ack["firmware_version"] == "2.7.20.abc1234"


def test_quiet_self_expiry_is_still_told_apart_from_a_boot_announce():
    """Both are unsolicited. Reporting one as the other would invent a quiet period
    nobody asked for, or hide a reboot."""
    pkt = _ack_packet(command_id=0, command_type=navamesh_pb2.QUIET_MODE_EXIT, ok=True)
    ack = extract_command_ack(pkt)
    assert ack["unsolicited"] is True
    assert ack["command_type_name"] == "QUIET_MODE_EXIT"


# ── The operator's census ───────────────────────────────────────────────────────

def test_firmware_census_takes_no_target():
    """It reports the whole fleet, so a target is a misunderstanding worth naming --
    most likely someone reaching for `fwinfo <id>`."""
    from navamesh.cmd_cli import main
    assert main(["firmware", "!0b9aed49"]) == 2


def test_every_other_verb_still_requires_a_target():
    """Making target optional for `firmware` must not let a bare `ble` through to a
    place where the missing target reads as some other error."""
    from navamesh.cmd_cli import main
    for verb in ("ble", "interval", "quiet", "setloc", "fwinfo"):
        assert main([verb]) == 2, f"{verb} accepted a missing target"


def test_the_census_is_reachable_without_the_rns_stack():
    """navamesh-cmd runs inside the bridge container, which has no RNS/LXMF. Importing
    the gateway's fmt_firmware would have been the obvious reuse and is not available
    there -- which is why this lives in cmd_cli."""
    import navamesh.cmd_cli as cli
    assert hasattr(cli, "_print_firmware_census")
    import inspect
    assert "reticulum_bridge" not in inspect.getsource(cli)


@pytest.mark.parametrize("seconds,expected", [
    (30, "30s ago"), (300, "5m ago"), (7200, "2h ago"), (300000, "3d ago"),
])
def test_age_column_is_scannable(seconds, expected):
    from navamesh.cmd_cli import _ago
    assert _ago(seconds) == expected
