"""
test_handle_write_command.py — Control-verb parsing, authorization, and dispatch.

These verbs change deployed field hardware, so the authorization and bounds tests here
are the guardrails, not decoration.
"""

import pytest

from navamesh.reticulum_bridge import (
    WRITE_VERBS,
    is_lxmf_destination,
    NodeSnapshot,
    ReticulumBridgeConfig,
    handle_command,
    is_sender_authorized,
)

KNOWN_NODE = "!abc12345"


@pytest.fixture
def nodes():
    return {KNOWN_NODE: NodeSnapshot(node_id=KNOWN_NODE)}


@pytest.fixture
def cfg():
    # Every field after pg_dsn is defaulted, which is itself the point: a config built
    # without control settings must deny control commands.
    return ReticulumBridgeConfig(
        rns_config_dir="/tmp", lxmf_storage_dir="/tmp", display_name="test",
        announce_interval=180, map_tile_url="", map_tile_fallback="",
        map_max_dimension=400, map_jpeg_quality=60, map_max_bytes=8000,
        soil_wet_threshold=60.0, soil_dry_threshold=30.0,
        map_outlier_guard_enabled=True, map_mesh_neighbor_radius_m=800.0,
        map_outlier_min_nodes=3, map_separate_farm_distance_m=3000.0,
        cache_lat_min=None, cache_lat_max=None, cache_lon_min=None,
        cache_lon_max=None, cache_zoom_min=None, cache_zoom_max=None, pg_dsn="",
    )


class Recorder:
    """Stands in for LxmfGateway._dispatch_write."""

    def __init__(self, raises=None):
        self.calls = []
        self._raises = raises

    def __call__(self, **kwargs):
        if self._raises:
            raise self._raises
        self.calls.append(kwargs)
        return 12345


def _run(cmd, nodes, cfg, dispatch=None, authorized=True):
    return handle_command(
        cmd, nodes, cfg,
        dispatch_write=dispatch if dispatch is not None else Recorder(),
        source_hash="deadbeef",
        authorized=authorized,
    )[0]


# ── authorization ────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("verb", WRITE_VERBS)
def test_write_verbs_are_refused_for_unauthorized_senders(verb, nodes, cfg):
    rec = Recorder()
    reply = _run(f"{verb} {KNOWN_NODE} 15", nodes, cfg, dispatch=rec, authorized=False)
    assert "unauthorized" in reply.lower()
    assert rec.calls == [], "an unauthorized sender must not reach the transmit path"


def test_handle_command_parameter_defaults_closed(nodes, cfg):
    """
    handle_command's own `authorized` parameter defaults to False, so a caller that
    forgets to pass it gets a refusal rather than an open door.

    Note this is separate from the *policy* in is_sender_authorized(), which currently
    returns True for everyone when the allow-list is empty. The parameter default is the
    last line of defence against a wiring mistake, and should stay closed regardless of
    what the deployed policy is.
    """
    reply = handle_command(f"ble {KNOWN_NODE} 15", nodes, cfg,
                           dispatch_write=Recorder())[0]
    assert "unauthorized" in reply.lower()


# ── authorization policy (currently open by design; see TODO.md) ──────────────────

def test_empty_allow_list_permits_everyone():
    """
    Deliberate for testing and first deployment: any device that can reach the gateway may
    issue control commands. If this ever needs to flip, TODO.md describes the change.
    """
    assert is_sender_authorized("deadbeef", ()) is True
    assert is_sender_authorized("anything", []) is True


def test_populated_allow_list_restricts_to_its_members():
    allowed = ("aabbcc",)
    assert is_sender_authorized("aabbcc", allowed) is True
    assert is_sender_authorized("ffffff", allowed) is False


def test_allow_list_matching_is_case_and_separator_insensitive():
    """RNS prints hashes both delimited and bare; an operator may paste either."""
    allowed = ("aabbccdd",)
    assert is_sender_authorized("AABBCCDD", allowed) is True
    assert is_sender_authorized("aa:bb:cc:dd", allowed) is True
    assert is_sender_authorized("  aabbccdd  ", allowed) is True


def test_read_only_verbs_still_work_without_any_write_wiring(nodes, cfg):
    """The nine original verbs must be unaffected by the new keyword-only params."""
    assert "Navamesh" in handle_command("help", nodes, cfg)[0]
    assert KNOWN_NODE in handle_command("nodes", nodes, cfg)[0]


def test_missing_command_bus_is_reported_not_silently_dropped(nodes, cfg):
    reply = handle_command(f"ble {KNOWN_NODE} 15", nodes, cfg,
                           dispatch_write=None, source_hash="x", authorized=True)[0]
    assert "not available" in reply.lower()


# ── dispatch ─────────────────────────────────────────────────────────────────────

def test_ble_window_dispatches_with_parsed_value(nodes, cfg):
    rec = Recorder()
    reply = _run(f"ble {KNOWN_NODE} 30", nodes, cfg, dispatch=rec)
    assert rec.calls[0] == {
        "verb": "ble", "target": KNOWN_NODE, "value": 30,
        "quiet_on": None, "requested_by": "deadbeef",
    }
    assert "12345" in reply


def test_interval_dispatches_seconds(nodes, cfg):
    rec = Recorder()
    _run(f"interval {KNOWN_NODE} 1800", nodes, cfg, dispatch=rec)
    assert rec.calls[0]["verb"] == "interval"
    assert rec.calls[0]["value"] == 1800


@pytest.mark.parametrize("arg,expected", [("on", True), ("off", False)])
def test_quiet_on_off(arg, expected, nodes, cfg):
    rec = Recorder()
    _run(f"quiet {KNOWN_NODE} {arg}", nodes, cfg, dispatch=rec)
    assert rec.calls[0]["quiet_on"] is expected


@pytest.mark.parametrize("target", ["^all", "all"])
def test_broadcast_targets_are_normalised(target, nodes, cfg):
    rec = Recorder()
    reply = _run(f"ble {target} 15", nodes, cfg, dispatch=rec)
    assert rec.calls[0]["target"] == "^all"
    assert "ALL" in reply


def test_broadcast_does_not_require_a_known_node(cfg):
    """A broadcast must work even with an empty node table."""
    rec = Recorder()
    _run("ble ^all 15", {}, cfg, dispatch=rec)
    assert rec.calls[0]["target"] == "^all"


# ── validation ───────────────────────────────────────────────────────────────────

def test_unknown_node_is_rejected_before_dispatch(nodes, cfg):
    rec = Recorder()
    reply = _run("ble !nosuchnode 15", nodes, cfg, dispatch=rec)
    assert "not found" in reply.lower()
    assert rec.calls == []


@pytest.mark.parametrize("bad", [
    f"ble {KNOWN_NODE} 0",
    f"ble {KNOWN_NODE} 999",
    f"interval {KNOWN_NODE} 10",
    f"interval {KNOWN_NODE} 999999",
])
def test_out_of_range_values_are_rejected_before_dispatch(bad, nodes, cfg):
    rec = Recorder()
    reply = _run(bad, nodes, cfg, dispatch=rec)
    assert "must be" in reply.lower()
    assert rec.calls == []


@pytest.mark.parametrize("bad", [
    "ble",
    f"ble {KNOWN_NODE}",
    f"ble {KNOWN_NODE} abc",
    f"quiet {KNOWN_NODE}",
    f"quiet {KNOWN_NODE} maybe",
])
def test_malformed_arguments_are_rejected_with_guidance(bad, nodes, cfg):
    rec = Recorder()
    reply = _run(bad, nodes, cfg, dispatch=rec)
    assert reply.startswith("⚠️")
    assert rec.calls == []


def test_dispatch_failure_is_surfaced_to_the_operator(nodes, cfg):
    """
    If the broker is down the operator must be told. Reporting success here would leave
    them believing a node was reconfigured when nothing was ever transmitted.
    """
    rec = Recorder(raises=RuntimeError("broker unreachable"))
    reply = _run(f"ble {KNOWN_NODE} 15", nodes, cfg, dispatch=rec)
    assert "could not queue" in reply.lower()
    assert "broker unreachable" in reply


# ── outcome delivery targets ─────────────────────────────────────────────────────

@pytest.mark.parametrize("requested_by", ["navamesh-cmd", "bridge", "", None])
def test_non_lxmf_requesters_are_not_reply_targets(requested_by):
    """
    Regression guard. Commands from the CLI or the bridge stamp requested_by with a
    name, not an RNS hash. Treating those as destinations raised ValueError from
    bytes.fromhex, and because the row was then never marked notified it retried every
    poll cycle forever -- observed spamming the reticulum log once per 5s.
    """
    assert is_lxmf_destination(requested_by) is False


@pytest.mark.parametrize("h", ["a" * 32, ("b" * 32).upper(), "aa:" + "c" * 30, "  " + "d" * 32 + "  "])
def test_real_rns_hashes_are_reply_targets(h):
    """RNS prints hashes 16 bytes wide, in either delimited or bare form."""
    assert is_lxmf_destination(h) is True


@pytest.mark.parametrize("h", ["a" * 31, "a" * 33, "z" * 32])
def test_wrong_length_or_non_hex_is_rejected(h):
    assert is_lxmf_destination(h) is False
