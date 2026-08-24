"""Tests for what the farmer actually reads.

Everything here is presentation, but presentation is where two classes of bug hid for a
long time: a soil percentage the probe could not support, shown only sometimes so it read
as the precise answer; and confirmations written in wire verbs, so understanding your own
sensor required knowing the protocol.

Same collection guard as test_map_labels.py -- the bridge module pulls in rns/lxmf at
import time, so skip the module where that stack is not installed.
"""
import pytest

try:
    from navamesh.reticulum_bridge import (
        NodeSnapshot,
        HELP_TEXT,
        VERB_LABELS,
        _verb_label,
        _friendly_seconds,
        percent_to_band,
        _fmt_soil_reading,
        _soil_band_short,
    )
except (ImportError, SystemExit) as exc:  # rns/lxmf/staticmap/dotenv not installed
    pytest.skip(f"reticulum_bridge unavailable: {exc}", allow_module_level=True)

from navamesh.calibration import DRY, DAMP, WET


# ── Durations ───────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("seconds,expected", [
    (60, "1 minute"),
    (300, "5 minutes"),
    (900, "15 minutes"),
    (1800, "30 minutes"),
    (3600, "1 hour"),
    (7200, "2 hours"),
    (28800, "8 hours"),      # the SENSOR-role default
    (86400, "1 day"),
    (172800, "2 days"),
])
def test_friendly_seconds_reads_as_english(seconds, expected):
    assert _friendly_seconds(seconds) == expected


def test_friendly_seconds_keeps_awkward_values_exact():
    """A value that does not divide cleanly is shown as-is rather than rounded.

    Rounding would quietly misreport a cadence the operator chose deliberately.
    """
    assert _friendly_seconds(90) == "90 seconds"
    assert _friendly_seconds(45) == "45 seconds"


def test_friendly_seconds_survives_junk():
    """Never raise while formatting a status message -- the reply matters more than
    the tidiness of one field."""
    assert _friendly_seconds(None) == "None"
    assert _friendly_seconds("x") == "x"


# ── Verb labels ─────────────────────────────────────────────────────────────────

def test_every_write_verb_has_a_farmer_label():
    """A verb without an entry falls through to the raw wire word, which is the bug
    this table exists to prevent. Keep it in step with the app's buttons."""
    for verb in ("ble", "interval", "quiet", "setloc"):
        assert verb in VERB_LABELS
        assert _verb_label(verb) != verb


def test_verb_labels_carry_no_protocol_words():
    for label in VERB_LABELS.values():
        lowered = label.lower()
        for jargon in ("setloc", "quiet mode", "telemetry", "portnum", "node"):
            assert jargon not in lowered, f"{label!r} still speaks protocol"


def test_unknown_verb_falls_back_to_itself():
    """Better a raw verb than a crash or an empty string in an outcome message."""
    assert _verb_label("something_new") == "something_new"


# ── Soil is a band, never a percentage ──────────────────────────────────────────

@pytest.mark.parametrize("pct,band", [(0.0, DRY), (12.0, DRY), (29.9, DRY),
                                      (30.0, DAMP), (45.4, DAMP), (69.9, DAMP),
                                      (70.0, WET), (100.0, WET)])
def test_percent_to_band_covers_the_range(pct, band):
    assert percent_to_band(pct) == band


def test_percent_to_band_survives_junk():
    assert percent_to_band(None) == "UNKNOWN"
    assert percent_to_band("wet") == "UNKNOWN"


def test_soil_reading_never_shows_a_percentage():
    """Not even inside DAMP, where adc_to_percent() can resolve one.

    A figure present on some readings and absent on others reads as the precise answer
    with the rest as approximations, when the band is the part this probe supports.
    """
    for raw in (4095.0, 800.0, 300.0, 709.0):
        out = _fmt_soil_reading(NodeSnapshot(node_id="!abcd1234", soil_raw=raw))
        assert "%" not in out, f"raw {raw} produced {out!r}"
        assert out.split()[0] in (DRY, DAMP, WET)


def test_soil_reading_keeps_the_raw_count_for_diagnostics():
    """The ADC stays: it is plainly not a moisture figure, and it is what we ask for
    when a node looks wrong."""
    out = _fmt_soil_reading(NodeSnapshot(node_id="!abcd1234", soil_raw=709.0))
    assert "ADC 709" in out


def test_legacy_percentage_is_reported_as_a_band_and_flagged():
    """Legacy rows carry a percentage soil_text.py marks as not authoritative -- a
    bone-dry node at raw 4095 was reporting "10.0%". Show a word, and say it is
    uncalibrated rather than implying it is comparable to a real reading."""
    out = _fmt_soil_reading(NodeSnapshot(node_id="!abcd1234", soil_percent=45.0))
    assert "%" not in out
    assert DAMP in out
    assert "uncalibrated" in out


def test_no_reading_is_none_not_a_guess():
    assert _fmt_soil_reading(NodeSnapshot(node_id="!abcd1234")) is None


# ── Map pin short form ──────────────────────────────────────────────────────────

def test_soil_band_short_prefers_raw_over_disagreeing_legacy_percentage():
    """The two genuinely disagree in the DB; raw is authoritative."""
    snap = NodeSnapshot(node_id="!abcd1234", soil_raw=4095.0, soil_percent=45.0)
    assert _soil_band_short(snap) == DRY


def test_soil_band_short_is_one_word_for_the_pin():
    """The pin has room for a word, not a sentence."""
    for raw, band in ((4095.0, DRY), (800.0, DAMP), (300.0, WET)):
        out = _soil_band_short(NodeSnapshot(node_id="!abcd1234", soil_raw=raw))
        assert out == band
        assert " " not in out


def test_soil_band_short_unknown_is_a_question_mark():
    assert _soil_band_short(NodeSnapshot(node_id="!abcd1234")) == "?"


# ── Map popup label (generate_map) ──────────────────────────────────────────────

def test_map_label_shows_a_band_and_never_a_percentage():
    from navamesh.generate_map import moisture_label
    for band in ("DRY", "DAMP", "WET"):
        # A value is passed alongside the band to prove it is not appended.
        assert "%" not in moisture_label(17.7, band)


def test_map_label_maps_legacy_percentages_onto_band_words():
    """Rows predating the band must not be the only percentage on the map."""
    from navamesh.generate_map import moisture_label, BAND_LABELS
    assert moisture_label(12.0, None) == BAND_LABELS["DRY"]
    assert moisture_label(45.0, None) == BAND_LABELS["DAMP"]
    assert moisture_label(80.0, None) == BAND_LABELS["WET"]


def test_map_label_no_data_is_explicit():
    from navamesh.generate_map import moisture_label
    assert moisture_label(None, None) == "No reading"


# ── help text ────────────────────────────────────────────────────────────────

def test_help_names_each_control_command_the_way_the_app_does():
    """help is the one screen a farmer opens *because* they do not already know
    what the commands do, so it is the last place that should assume they do.

    Pinned to VERB_LABELS rather than to literal strings: the app's buttons, the
    command confirmations and this help text are three renderings of the same
    four verbs, and the failure being prevented is one of them being reworded
    alone -- which is what happened when cd60737 rewrote the confirmations and
    left the help text speaking protocol.
    """
    for label in VERB_LABELS.values():
        assert label in HELP_TEXT, f"help text does not name {label!r} as the app does"


def test_help_states_the_quiet_auto_resume_the_node_actually_uses():
    """The app sends no duration, so the node applies NAVAMESH_QUIET_DEFAULT_MINUTES
    = 1440, i.e. one day. 4320 (three days) is only the clamp ceiling.

    Caught on the bench: the ack came back `applied=1440` while this text promised
    "within 3 days". Not false, but wrong in the direction that matters -- a farmer
    who pauses a sensor and expects up to three days of quiet gets it back after
    one, and one who wants it back sooner thinks they must wait three.
    """
    assert "after a day" in HELP_TEXT
    assert "3 days" not in HELP_TEXT


def test_help_does_not_describe_commands_in_protocol_terms():
    """Each of these appeared in the help text while the app said something a
    person could act on."""
    for jargon in ("telemetry interval", "RSSI", "SNR", "auto-close", "transmitting"):
        assert jargon not in HELP_TEXT, f"help text still says {jargon!r}"


def test_help_still_documents_the_wire_syntax():
    """The farmer taps buttons, but the operator drives the same gateway by
    typing, and this is the only place the syntax is written down."""
    for verb in VERB_LABELS:
        assert f"{verb} <" in HELP_TEXT, f"help text no longer shows how to type {verb!r}"


# ── Line width ──────────────────────────────────────────────────────────────────

# The app renders gateway replies in a monospace Label on a phone, which wraps at about
# 44 columns. Measured from a screenshot on a moto g play, 2026-08-24: the help text's
# 76-char control lines broke mid-sentence, and because those lines already carry a
# 6-space hanging indent for their continuations, the wrapped remainder landed at column
# 0 *beside* deliberate column-6 text. The result was an alternating left edge that read
# as corruption rather than as wrapping.
#
# 43 rather than 44, so a line that is exactly at the limit still has somewhere to go if
# the font metrics differ slightly on another handset.
HELP_MAX_COLUMNS = 43


def test_help_text_fits_the_phone_without_wrapping():
    """Anything wider than the phone's Label wraps to column 0, which collides with the
    indented continuation lines and looks like a rendering fault."""
    too_wide = [(len(line), line) for line in HELP_TEXT.split("\n")
                if len(line) > HELP_MAX_COLUMNS]
    assert not too_wide, (
        f"help lines exceed {HELP_MAX_COLUMNS} columns and will wrap on a phone: {too_wide}"
    )


def test_every_reply_the_gateway_composes_by_hand_fits_too():
    """The section rules are 30 columns wide, which is the width this whole surface was
    designed around -- a formatter that drifts past the wrap point looks broken in the
    same way the help text did."""
    from navamesh.reticulum_bridge import _header
    for line in _header("🌱 Something").split("\n"):
        assert len(line) <= HELP_MAX_COLUMNS
