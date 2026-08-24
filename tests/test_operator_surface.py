"""The firmware version is an operator's concern, and must stay off the farmer's screen.

A farmer needs DRY/DAMP/WET. A build hash in the app's help or its buttons would
re-introduce exactly the protocol-facing surface that cd60737 and the HELP_TEXT rewrite
took out -- so these tests pin the separation rather than the wording, because the
wording is the part that will legitimately change.

Same collection guard as the other bridge modules.
"""
import pytest

try:
    from navamesh.reticulum_bridge import (
        NodeSnapshot,
        HELP_TEXT,
        OPERATOR_HELP_TEXT,
        VERB_LABELS,
        OPERATOR_VERB_LABELS,
        WRITE_VERBS,
        _verb_label,
        _parse_write_args,
        fmt_firmware,
        handle_command,
        ReticulumBridgeConfig,
    )
except (ImportError, SystemExit) as exc:  # rns/lxmf/staticmap/dotenv not installed
    pytest.skip(f"reticulum_bridge unavailable: {exc}", allow_module_level=True)


# ── The separation itself ───────────────────────────────────────────────────────

def test_the_farmer_help_never_mentions_firmware():
    """The one screen a farmer opens because they do not know what the commands do is
    the last place to put a build hash."""
    lowered = HELP_TEXT.lower()
    for word in ("firmware", "fwinfo", "version", "build"):
        assert word not in lowered, f"farmer help now says {word!r}"


def test_operator_verbs_are_not_in_the_farmer_vocabulary():
    """VERB_LABELS is pinned by test_farmer_wording to both HELP_TEXT and the app's
    buttons, so anything added there is a promise to show it to a farmer."""
    for verb in OPERATOR_VERB_LABELS:
        assert verb not in VERB_LABELS


def test_operator_verbs_still_get_a_readable_name():
    """The operator reads the same outcome messages, and '✅ fwinfo confirmed' is the
    protocol talking."""
    assert _verb_label("fwinfo") == "Firmware version"


def test_operator_help_documents_what_the_farmer_help_does_not():
    assert "fwinfo" in OPERATOR_HELP_TEXT
    assert "firmware" in OPERATOR_HELP_TEXT


def test_fwinfo_is_gated_like_anything_that_transmits():
    """It changes nothing on a node, but broadcasting it makes every node in range
    transmit -- so it is gated like a write, not like a database query."""
    assert "fwinfo" in WRITE_VERBS


# ── fwinfo takes no argument ────────────────────────────────────────────────────

def test_fwinfo_parses_with_a_target_and_no_value():
    node, value, quiet_on, coords, error = _parse_write_args("fwinfo", "!0b9aed49")
    assert error is None
    assert node == "!0b9aed49"
    assert value is None


def test_fwinfo_rejects_a_stray_value_instead_of_ignoring_it():
    """Silently dropping it would leave an operator believing they had scoped the probe."""
    _, _, _, _, error = _parse_write_args("fwinfo", "!0b9aed49 30")
    assert error and "no value" in error


def test_fwinfo_may_be_broadcast():
    """Unlike setloc: the reply is the only effect, and asking the whole fleet at once
    is the question this verb exists for."""
    node, _, _, _, error = _parse_write_args("fwinfo", "^all")
    assert error is None and node == "^all"


def test_a_verb_with_no_argument_does_not_produce_a_trailing_space_example():
    _, _, _, _, error = _parse_write_args("fwinfo", None)
    assert "fwinfo ^all" in error
    assert "  " not in error


# ── Interval units reach the gateway ────────────────────────────────────────────

def test_the_gateway_accepts_units_on_an_interval():
    node, value, _, _, error = _parse_write_args("interval", "!0b9aed49 45m")
    assert error is None
    assert value == 2700


def test_the_gateway_still_accepts_bare_seconds():
    _, value, _, _, error = _parse_write_args("interval", "!0b9aed49 1800")
    assert error is None and value == 1800


# ── The operator's read view ────────────────────────────────────────────────────

def _snap(node_id, version=None):
    return NodeSnapshot(node_id=node_id, ts=1_756_000_000, firmware_version=version)


def test_firmware_view_groups_by_version_so_the_odd_one_out_is_visible():
    """During a rollout the question is 'which ones still need doing', and eighteen
    identical lines hide the two that differ."""
    nodes = {
        "!aaa": _snap("!aaa", "2.7.20.200289a"),
        "!bbb": _snap("!bbb", "2.7.20.200289a"),
        "!ccc": _snap("!ccc", "2.7.20.459b09e"),
    }
    out = fmt_firmware(nodes)
    assert "2.7.20.200289a — 2 sensors" in out
    assert "2.7.20.459b09e — 1 sensor" in out


def test_a_node_that_has_not_reported_is_not_called_unflashed():
    """Different questions: soil_raw answers 'has it been flashed at all', this answers
    'which build'. A node simply may not have acked or booted since the Pi last saw it."""
    out = fmt_firmware({"!aaa": _snap("!aaa")})
    assert "Not reported yet" in out
    assert "unflashed" not in out.lower()


def test_firmware_view_is_a_database_read_and_needs_no_command_bus():
    """No dispatch_write, no authorization: nothing goes on the air."""
    cfg = ReticulumBridgeConfig.from_env() if hasattr(ReticulumBridgeConfig, "from_env") else None
    reply, attachment = handle_command("firmware", {"!aaa": _snap("!aaa", "2.7.20.abc")}, cfg)
    assert attachment is None
    assert "2.7.20.abc" in reply


# ── Command id monotonicity ─────────────────────────────────────────────────────
#
# The firmware drops any command whose id is at or below the last one a node accepted,
# and does it SILENTLY -- no ack, no error. A retry carries a similarly low id, so the
# state is unrecoverable by retrying and looks exactly like a dead radio from the Pi.
#
# A bare timestamp is monotonic only while the clock is. A Pi has no guaranteed RTC and a
# farm gateway may have no NTP, so one that loses power for a week returns believing it is
# a week ago -- and every command it issues from then on is rejected by every node it has
# ever commanded.

import threading
import types


def _gateway_stub(highest_issued=0):
    """Enough of LxmfGateway to exercise _next_cmd_id without standing up RNS."""
    from navamesh.reticulum_bridge import LxmfGateway

    stub = types.SimpleNamespace(
        _cmd_seq_lock=threading.Lock(),
        _cfg=types.SimpleNamespace(pg_dsn=""),   # so the DB read short-circuits to 0
    )
    stub._highest_issued_cmd_id = lambda: highest_issued
    stub._next_cmd_id = LxmfGateway._next_cmd_id.__get__(stub)
    return stub


def test_ids_increase_even_when_the_clock_jumps_backwards():
    """The failure this exists to prevent: a gateway that comes back believing it is a
    week ago, and is thereafter ignored by every node it has ever talked to."""
    from navamesh import reticulum_bridge as rb

    gw = _gateway_stub()
    real_time = rb.time.time
    try:
        rb.time.time = lambda: 1_800_000_000.0
        first = gw._next_cmd_id()
        # Power cut; the clock comes back a week earlier.
        rb.time.time = lambda: 1_800_000_000.0 - 604_800
        second = gw._next_cmd_id()
    finally:
        rb.time.time = real_time

    assert second > first, "a backwards clock produced a non-increasing command id"


def test_the_floor_comes_from_what_was_actually_issued():
    """The in-memory counter is lost on restart, so the guard has to be reseeded from the
    audit table or a restart plus a wrong clock reopens the hole."""
    from navamesh import reticulum_bridge as rb

    gw = _gateway_stub(highest_issued=1_900_000_000)
    real_time = rb.time.time
    try:
        rb.time.time = lambda: 1_800_000_000.0   # clock is behind what we have issued
        issued = gw._next_cmd_id()
    finally:
        rb.time.time = real_time

    assert issued > 1_900_000_000


def test_two_commands_in_the_same_second_do_not_collide():
    """The original reason the counter existed; must survive the new seeding."""
    from navamesh import reticulum_bridge as rb

    gw = _gateway_stub()
    real_time = rb.time.time
    try:
        rb.time.time = lambda: 1_800_000_000.0
        ids = [gw._next_cmd_id() for _ in range(5)]
    finally:
        rb.time.time = real_time

    assert ids == sorted(ids) and len(set(ids)) == 5


def test_the_floor_query_casts_because_cmd_id_is_text():
    """max() on a TEXT column is lexicographic: "999999999" sorts above "1787612909"
    while being numerically smaller -- i.e. it fails precisely when the clock has gone
    backwards, which is the only time this code matters."""
    import inspect
    from navamesh.reticulum_bridge import LxmfGateway
    src = inspect.getsource(LxmfGateway._highest_issued_cmd_id)
    assert "cmd_id::bigint" in src
    assert "^[0-9]+$" in src, "a malformed row would break command dispatch entirely"
