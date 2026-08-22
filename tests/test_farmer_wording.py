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
