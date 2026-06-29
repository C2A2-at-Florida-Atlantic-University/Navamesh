"""Tests for the offline-cache bounds + tile-range guard in the Reticulum map
renderer.

The pure helpers here (bounds + Web-Mercator tile math) decide whether a map may
be rendered at all, so they are tested directly — no real tiles or Pillow needed.
The bridge module pulls in rns/lxmf at import time (and SystemExits without them),
so collection is guarded: where that stack isn't installed the module is skipped
rather than erroring out, matching the rest of the suite which avoids importing it.
"""
import math

import pytest

try:
    from navamesh.reticulum_bridge import (
        ReticulumBridgeConfig,
        NodeSnapshot,
        handle_command,
        MAP_AVAILABLE,
        _cache_bounds,
        _within_bounds,
        _deg2tile,
        _tile_range_for_bounds,
        _tile_range_for_view,
        _tile_range_contains,
        _cached_view_choice,
    )
except (ImportError, SystemExit) as exc:  # rns/lxmf/staticmap/dotenv not installed
    pytest.skip(f"reticulum_bridge unavailable: {exc}", allow_module_level=True)


def _cache_tiles_range(lat_min, lat_max, lon_min, lon_max, zoom):
    """Reference oracle: the exact tile range cache_tiles.py would download for
    these CACHE_* bounds at this zoom (copied from cache_tiles.py:54-96)."""
    def deg2tile(lat, lon, z):
        lat_r = math.radians(lat)
        n = 2 ** z
        x = int((lon + 180.0) / 360.0 * n)
        y = int((1.0 - math.log(math.tan(lat_r) + 1.0 / math.cos(lat_r)) / math.pi) / 2.0 * n)
        return x, y
    x_min, y_max = deg2tile(lat_min, lon_min, zoom)
    x_max, y_min = deg2tile(lat_max, lon_max, zoom)
    return x_min, x_max, y_min, y_max


# A bounding box around the FAU farm, and the node coordinate from the reported
# failure (`map !cbf78a20`) whose zoom-17 tiles 404'd at /tiles/17/36371/55571.png.
FARM_BOUNDS = (26.20, 26.40, -80.20, -80.00)   # lat_min, lat_max, lon_min, lon_max
NODE_LAT, NODE_LON = 26.384, -80.103           # lands on z17 tile (36371, 55571)


def _cfg(**overrides) -> ReticulumBridgeConfig:
    base = dict(
        rns_config_dir="/tmp/rns",
        lxmf_storage_dir="/tmp/lxmf",
        display_name="test",
        announce_interval=180,
        map_tile_url="http://127.0.0.1:8080/tiles/{z}/{x}/{y}.png",
        map_tile_fallback="https://tile.openstreetmap.org/{z}/{x}/{y}.png",
        map_max_dimension=640,
        map_jpeg_quality=75,
        map_max_bytes=120_000,
        soil_wet_threshold=60.0,
        soil_dry_threshold=30.0,
        map_outlier_guard_enabled=True,
        map_mesh_neighbor_radius_m=800.0,
        map_outlier_min_nodes=3,
        map_separate_farm_distance_m=3000.0,
        cache_lat_min=None,
        cache_lat_max=None,
        cache_lon_min=None,
        cache_lon_max=None,
        cache_zoom_min=14,
        cache_zoom_max=19,
        pg_dsn="",
    )
    base.update(overrides)
    return ReticulumBridgeConfig(**base)


def _cfg_with_bounds(**overrides) -> ReticulumBridgeConfig:
    return _cfg(
        cache_lat_min=FARM_BOUNDS[0],
        cache_lat_max=FARM_BOUNDS[1],
        cache_lon_min=FARM_BOUNDS[2],
        cache_lon_max=FARM_BOUNDS[3],
        **overrides,
    )


# ── _cache_bounds ─────────────────────────────────────────────────────────────

def test_cache_bounds_returns_none_when_not_configured():
    assert _cache_bounds(_cfg()) is None


def test_cache_bounds_returns_none_when_any_bound_missing():
    cfg = _cfg(cache_lat_min=26.2, cache_lat_max=26.4, cache_lon_min=-80.2)  # no lon_max
    assert _cache_bounds(cfg) is None


def test_cache_bounds_normalizes_swapped_min_max():
    # Swap min/max on input; helper must return them ordered.
    cfg = _cfg(cache_lat_min=26.4, cache_lat_max=26.2,
               cache_lon_min=-80.0, cache_lon_max=-80.2)
    assert _cache_bounds(cfg) == (26.2, 26.4, -80.2, -80.0)


# ── _within_bounds ──────────────────────────────────────────────────────────

def test_within_bounds_inside():
    assert _within_bounds(NODE_LAT, NODE_LON, FARM_BOUNDS) is True


def test_within_bounds_outside_lon_and_lat():
    assert _within_bounds(26.30, -79.50, FARM_BOUNDS) is False   # lon east of box
    assert _within_bounds(27.50, -80.10, FARM_BOUNDS) is False   # lat north of box


def test_within_bounds_handles_missing_inputs():
    assert _within_bounds(None, NODE_LON, FARM_BOUNDS) is False
    assert _within_bounds(NODE_LAT, None, FARM_BOUNDS) is False
    assert _within_bounds(NODE_LAT, NODE_LON, None) is False


# ── tile math ─────────────────────────────────────────────────────────────────

def test_deg2tile_matches_reported_failure_tile():
    x, y = _deg2tile(NODE_LAT, NODE_LON, 17)
    assert (int(x), int(y)) == (36371, 55571)


def test_tile_range_for_bounds_is_ordered_and_y_inverted():
    x0, x1, y0, y1 = _tile_range_for_bounds(FARM_BOUNDS, 17)
    assert x0 <= x1 and y0 <= y1
    # Larger latitude maps to the smaller (top) y tile.
    _, y_top = _deg2tile(FARM_BOUNDS[1], FARM_BOUNDS[2], 17)
    _, y_bot = _deg2tile(FARM_BOUNDS[0], FARM_BOUNDS[2], 17)
    assert y_top < y_bot


@pytest.mark.parametrize("zoom", range(14, 20))
def test_tile_range_for_bounds_matches_cache_tiles(zoom):
    # The CACHE_* example from cache_tiles.py's docstring.
    lat_min, lat_max, lon_min, lon_max = 26.3720, 26.3785, -80.1000, -80.0920
    expected = _cache_tiles_range(lat_min, lat_max, lon_min, lon_max, zoom)
    got = _tile_range_for_bounds((lat_min, lat_max, lon_min, lon_max), zoom)
    assert got == expected


def test_tile_range_for_view_is_edge_conservative():
    # render_size chosen so the half-viewport is non-integer → edges are
    # fractional, so floor (min) and ceil (max) genuinely differ.
    lat, lon, z, size = NODE_LAT, NODE_LON, 17, 600
    fx, fy = _deg2tile(lat, lon, z)
    half = (size / 2.0) / 256
    x0, x1, y0, y1 = _tile_range_for_view(lat, lon, z, size)
    assert x0 == math.floor(fx - half)
    assert x1 == math.ceil(fx + half) - 1
    assert y0 == math.floor(fy - half)
    assert y1 == math.ceil(fy + half) - 1
    # ceil()-1 includes a partially entered edge tile without adding the tile
    # beyond an exact tile boundary.
    assert x1 >= math.floor(fx + half)
    assert y1 >= math.floor(fy + half)


def test_tile_range_for_view_does_not_include_tile_past_exact_boundary():
    # With a 512px viewport the half-width is exactly 1 tile. Centering on an
    # integer tile coordinate should request one tile on each side, not a third
    # tile just beyond the exact boundary.
    x, y, z = 1000, 2000, 17
    n = 2 ** z
    lon = x / n * 360.0 - 180.0
    lat = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    assert _tile_range_for_view(lat, lon, z, 512) == (999, 1000, 1999, 2000)


def test_tile_range_contains_true_when_view_inside_cache():
    cache = _tile_range_for_bounds(FARM_BOUNDS, 17)
    view = _tile_range_for_view(26.30, -80.10, 17, 512)
    assert _tile_range_contains(cache, view) is True


def test_tile_range_contains_false_when_square_viewport_spills():
    # A tiny (~1 tile) cache box with a normal square viewport: the viewport
    # needs neighbouring tiles the cache doesn't have. This is exactly the
    # square-aspect / padding spill that comparing lat/lon alone would miss.
    tiny = (26.384, 26.385, -80.103, -80.102)
    cache = _tile_range_for_bounds(tiny, 17)
    view = _tile_range_for_view(26.3845, -80.1025, 17, 512)
    assert _tile_range_contains(cache, view) is False


def test_cached_view_choice_tries_higher_cached_zoom_before_failing():
    # Mirrors the production failure shape: z16 selected from the bounds spills
    # outside a small cache rectangle, but z17 can fit because the cached bounds
    # cover more integer tiles at that zoom.
    bounds = (26.3720, 26.3785, -80.1000, -80.0920)
    cfg = _cfg(
        cache_lat_min=bounds[0], cache_lat_max=bounds[1],
        cache_lon_min=bounds[2], cache_lon_max=bounds[3],
        cache_zoom_min=14, cache_zoom_max=19,
    )
    choice = _cached_view_choice(bounds, cfg, desired_zoom=16, desired_render_size=1280)
    assert choice is not None
    zoom, render_size, view_range, cache_range = choice
    assert zoom >= 16
    assert render_size >= 480
    assert _tile_range_contains(cache_range, view_range) is True


# ── handle_command: map <id> outside coverage ────────────────────────────────

def test_map_id_outside_bounds_returns_text_and_no_image():
    nodes = {"!cbf78a20": NodeSnapshot(node_id="!cbf78a20", lat=27.50, lon=-80.10)}
    text, image = handle_command("map !cbf78a20", nodes, _cfg_with_bounds())
    assert image is None
    assert "outside offline map coverage" in text
    assert "27.50000" in text and "-80.10000" in text   # GPS reported


def test_map_id_outside_bounds_does_not_recenter_or_render():
    # Even if rendering were available, an out-of-coverage node must return before
    # render_map is ever called, so no tiles are requested.
    nodes = {"!faraway1": NodeSnapshot(node_id="!faraway1", lat=40.0, lon=-74.0)}
    text, image = handle_command("map !faraway1", nodes, _cfg_with_bounds())
    assert image is None
    assert "outside offline map coverage" in text


def test_plain_map_with_no_in_bounds_nodes_returns_clear_text():
    # Cache configured, but every GPS node falls outside coverage.
    nodes = {
        "!a1": NodeSnapshot(node_id="!a1", lat=40.0, lon=-74.0),
        "!b2": NodeSnapshot(node_id="!b2", lat=41.0, lon=-75.0),
    }
    text, image = handle_command("map", nodes, _cfg_with_bounds())
    assert image is None
    assert text == (
        "No GPS nodes are inside offline map coverage. 2 GPS node(s) outside coverage."
    )


@pytest.mark.skipif(not MAP_AVAILABLE, reason="staticmap/pillow not installed")
def test_map_id_in_bounds_but_render_fails_gives_specific_text():
    # Tiny cache box that contains the node, but a square render viewport spills
    # past it → the containment gate refuses to render (no tile server needed).
    cfg = _cfg(
        cache_lat_min=26.384, cache_lat_max=26.385,
        cache_lon_min=-80.103, cache_lon_max=-80.102,
    )
    nodes = {"!cbf78a20": NodeSnapshot(node_id="!cbf78a20", lat=26.3845, lon=-80.1025)}
    text, image = handle_command("map !cbf78a20", nodes, cfg)
    assert image is None
    assert text.startswith(
        "Map image unavailable because required offline tiles are missing "
        "or outside cache coverage."
    )
    assert "26.38450" in text and "-80.10250" in text   # node GPS, not a node summary
    assert "Soil:" not in text                            # not the generic summary
