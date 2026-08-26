"""Tests for the Pi map-label rendering pieces: the soil/battery value strings
and the primitive-drawn droplet/battery icons that replaced the `S:`/`B:` text
prefixes (the default Pillow font on the Pi has no emoji glyphs).

Same collection guard as test_map_bounds.py — the bridge module pulls in
rns/lxmf at import time, so skip the module where that stack isn't installed.
"""
import pytest

try:
    from navamesh.reticulum_bridge import (
        ReticulumBridgeConfig,
        NodeSnapshot,
        MAP_AVAILABLE,
        handle_command,
        _label_values,
        _draw_droplet_icon,
        _draw_battery_icon,
        _node_label,
        _map_pin_label,
        _MAP_LABEL_MAX,
    )
except (ImportError, SystemExit) as exc:  # rns/lxmf/staticmap/dotenv not installed
    pytest.skip(f"reticulum_bridge unavailable: {exc}", allow_module_level=True)


# ── _label_values ─────────────────────────────────────────────────────────────

def test_label_values_soil_is_a_band_not_a_percentage():
    """The pin says DRY/DAMP/WET. Battery stays a percentage -- it really is one.

    A soil figure implied precision this probe does not have: it is blind below ~9.5%
    moisture and saturated above 20%, so any number outside that window described the rail
    the probe was pinned to rather than the soil.
    """
    snap = NodeSnapshot(node_id="!abcd1234", soil_raw=800.0, battery_level=82.0)
    soil, bat = _label_values(snap)
    assert soil in ("DRY", "DAMP", "WET")
    assert "%" not in soil
    assert bat == "82%"


def test_label_values_legacy_percentage_maps_onto_a_band():
    """Rows predating the band carry only a percentage; it must still render as a word,
    so the map never mixes vocabularies."""
    dry = NodeSnapshot(node_id="!abcd1234", soil_percent=12.0)
    damp = NodeSnapshot(node_id="!abcd1234", soil_percent=45.4)
    wet = NodeSnapshot(node_id="!abcd1234", soil_percent=80.0)
    assert _label_values(dry)[0] == "DRY"
    assert _label_values(damp)[0] == "DAMP"
    assert _label_values(wet)[0] == "WET"


def test_label_values_raw_wins_over_legacy_percentage():
    """The raw ADC is authoritative: the DB still holds legacy percentages that disagree
    with the probe (a bone-dry node at raw 4095 was reporting "10.0%")."""
    snap = NodeSnapshot(node_id="!abcd1234", soil_raw=4095.0, soil_percent=45.0)
    assert _label_values(snap)[0] == "DRY"


def test_label_values_unknown_are_question_marks():
    snap = NodeSnapshot(node_id="!abcd1234")
    assert _label_values(snap) == ("?", "?")


def test_label_values_usb_wins_over_battery_level():
    snap = NodeSnapshot(
        node_id="!abcd1234", soil_raw=800.0, battery_level=99.0, battery_usb=True
    )
    soil, bat = _label_values(snap)
    assert soil in ("DRY", "DAMP", "WET")
    assert bat == "USB"


# ── node display labels (Meshtastic NODEINFO_APP names) ──────────────────────

def test_text_takes_the_long_name_and_the_pin_takes_the_short_one():
    """The two labels disagree on purpose, so keep them pinned together.

    A pin is drawn beside a dot with a whole map to share and takes the shortest
    thing that identifies the node. A line of text has room for the name the
    farmer actually chose, and showing them "NF01" there wasted it -- every
    status/position/battery/link reply said "Node 1234" or a four-character code
    long after the app rename had arrived and was sitting in the database."""
    snap = NodeSnapshot(node_id="!abcd1234", short_name="NF01", long_name="North Field")
    assert _node_label("!abcd1234", snap) == "North Field"
    assert _map_pin_label("!abcd1234", snap) == "NF01"


def test_reticulum_node_label_prefers_display_name_over_short():
    snap = NodeSnapshot(
        node_id="!abcd1234", display_name="Resolved", short_name="NF01"
    )
    assert _node_label("!abcd1234", snap) == "Resolved"


def test_reticulum_node_label_falls_back_to_last4(monkeypatch):
    monkeypatch.delenv("NODE_LABEL_ALIASES", raising=False)
    assert _node_label("!abcd1234") == "Node 1234"
    assert _node_label("!abcd1234", NodeSnapshot(node_id="!abcd1234")) == "Node 1234"
    # blank names don't count as names
    snap = NodeSnapshot(node_id="!abcd1234", short_name="  ", long_name="")
    assert _node_label("!abcd1234", snap) == "Node 1234"
    assert _map_pin_label("!abcd1234", snap) == "1234"


def test_reticulum_node_label_env_alias(monkeypatch):
    monkeypatch.setenv("NODE_LABEL_ALIASES", "!abcd1234=Node A, !def67890=Node B")
    assert _node_label("!abcd1234") == "Node A"
    assert _node_label("!def67890") == "Node B"
    # Meshtastic names still win over the alias
    snap = NodeSnapshot(node_id="!abcd1234", short_name="NF01")
    assert _node_label("!abcd1234", snap) == "NF01"


def test_map_pin_label_truncates_long_names_for_image_only():
    long = "A Very Long Meshtastic Node Name"
    snap = NodeSnapshot(node_id="!abcd1234", long_name=long)
    assert _map_pin_label("!abcd1234", snap) == long[:_MAP_LABEL_MAX].rstrip()
    assert len(_map_pin_label("!abcd1234", snap)) <= _MAP_LABEL_MAX
    assert _node_label("!abcd1234", snap) == long  # text replies keep the full name
    assert snap.long_name == long                  # stored value untouched


def _cfg_no_cache() -> ReticulumBridgeConfig:
    return ReticulumBridgeConfig(
        rns_config_dir="/tmp/rns", lxmf_storage_dir="/tmp/lxmf", display_name="t",
        announce_interval=180, map_tile_url="", map_tile_fallback="",
        map_max_dimension=640, map_jpeg_quality=75, map_max_bytes=120_000,
        soil_wet_threshold=60.0, soil_dry_threshold=30.0,
        map_outlier_guard_enabled=True, map_mesh_neighbor_radius_m=800.0,
        map_outlier_min_nodes=3, map_separate_farm_distance_m=3000.0,
        cache_lat_min=None, cache_lat_max=None, cache_lon_min=None,
        cache_lon_max=None, cache_zoom_min=None, cache_zoom_max=None, pg_dsn="",
    )


def test_map_text_fallback_uses_display_name_and_keeps_raw_id(monkeypatch):
    monkeypatch.delenv("NODE_LABEL_ALIASES", raising=False)
    # No GPS -> render_map can't produce an image -> text fallback lists nodes.
    nodes = {
        "!abcd1234": NodeSnapshot(
            node_id="!abcd1234", soil_percent=45.0, short_name="NF01"
        ),
        "!def67890": NodeSnapshot(node_id="!def67890"),  # no name -> legacy label
    }
    text, image = handle_command("map !abcd1234", nodes, _cfg_no_cache())
    assert image is None
    assert "NF01 (!abcd1234)" in text

    text, image = handle_command("map !def67890", nodes, _cfg_no_cache())
    assert image is None
    assert "Node 7890 (!def67890)" in text


# ── icon primitives ───────────────────────────────────────────────────────────

@pytest.mark.skipif(not MAP_AVAILABLE, reason="staticmap/pillow not installed")
@pytest.mark.parametrize("size", [8, 20, 40])
@pytest.mark.parametrize("icon", [_draw_droplet_icon, _draw_battery_icon])
def test_icons_draw_within_their_cell(icon, size):
    """Icons must produce visible pixels and stay inside their size×size cell
    (plus the icon_gap slack the label layout leaves after each icon), so the
    label collision boxes computed in render_map stay honest."""
    from PIL import Image, ImageDraw, ImageOps

    pad = 10
    img = Image.new("RGB", (size + 2 * pad, size + 2 * pad), "white")
    icon(ImageDraw.Draw(img), pad, pad, size, "black")

    # Invert so drawn (dark) pixels are the non-zero ones getbbox looks for.
    bbox = ImageOps.invert(img.convert("L")).getbbox()  # None → nothing drawn
    assert bbox is not None
    left, top, right, bottom = bbox  # right/bottom are exclusive
    assert left >= pad and top >= pad
    assert right <= pad + size + 1 and bottom <= pad + size + 1
