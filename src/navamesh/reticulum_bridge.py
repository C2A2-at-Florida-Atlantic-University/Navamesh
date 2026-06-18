"""
reticulum_bridge.py — Navamesh LXMF command/response gateway (v4)
==================================================================

Listens for incoming LXMF messages from the farmer's Sideband app and
replies with live sensor data pulled from Postgres.

Supported commands (case-insensitive, send from Sideband):
  status       — full plain-text summary of all nodes
  soil         — soil moisture readings for all nodes
  battery      — battery levels for all nodes
  position     — GPS coordinates for all nodes
  link         — RSSI/SNR link quality for all nodes
  map          — PNG map image for all nodes (Pi-rendered, sent via FIELD_IMAGE)
  map <id>     — PNG map image for one specific node
  nodes        — list all known node IDs
  help         — list available commands

IMAGE SIZING
------------
  Choose a link profile in .env:

      MAP_LINK_PROFILE=lora    (default) — 200 px, quality 35, ~8 KB cap
      MAP_LINK_PROFILE=halow             — 640 px, quality 72, ~120 KB cap
      MAP_LINK_PROFILE=wifi              — 900 px, quality 78, ~220 KB cap

  You can further override individual settings:
      MAP_MAX_DIMENSION, MAP_JPEG_QUALITY, MAP_MAX_BYTES

Required env vars (add to your .env):
  RNS_CONFIG_DIR          — path to Reticulum config dir (default: ~/.reticulum)
  LXMF_STORAGE_DIR        — where to store LXMF identity (default: ~/.navamesh_lxmf)
  LXMF_DISPLAY_NAME       — display name shown in Sideband (default: "Navamesh Gateway")
  PG_DSN                  — Postgres connection string for node snapshots

Optional env vars:
  LXMF_ANNOUNCE_INTERVAL  — seconds between RNS announces (default: 180)
  IGNORED_NODES           — comma-separated node IDs to ignore
  LOG_LEVEL               — logging level (default: INFO)

  # Map rendering (only needed for 'map' command)
  MAP_LINK_PROFILE        — lora | halow | wifi  (default: lora)
  MAP_TILE_URL            — local tile server URL
  MAP_TILE_FALLBACK       — fallback tile URL (default: OSM)
  MAP_MAX_DIMENSION       — override longest side of map image in pixels
  MAP_JPEG_QUALITY        — override JPEG quality 1-95
  MAP_MAX_BYTES           — override max compressed size in bytes
  SOIL_WET_THRESHOLD      — soil % for blue pin (default: 60)
  SOIL_DRY_THRESHOLD      — soil % for red pin (default: 30)

  # Map outlier guardrail (keeps a far-away cluster from zooming out the farm map)
  MAP_OUTLIER_GUARD_ENABLED        — true/false (default: true)
  MAP_MESH_NEIGHBOR_RADIUS_METERS  — link two nodes if within this (default: 800)
  MAP_OUTLIER_MIN_NODES            — min main-mesh size to enable filtering (default: 3)
  MAP_SEPARATE_FARM_DISTANCE_METERS— omit clusters at least this far away (default: 3000)

Dependencies:
  pip install rns lxmf paho-mqtt python-dotenv psycopg
  pip install staticmap pillow    # optional — only needed for map rendering
"""

from __future__ import annotations

import io
import logging
import math
import os
import signal
import sys
import threading
import time
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(usecwd=True))

try:
    import psycopg as _psycopg
except ImportError:
    _psycopg = None

try:
    import RNS
    import LXMF
except ImportError as exc:
    raise SystemExit(
        "Reticulum and LXMF are required:\n  pip install rns lxmf"
    ) from exc

try:
    from staticmap import StaticMap, CircleMarker
    from PIL import ImageDraw, ImageFont, Image
    MAP_AVAILABLE = True
except ImportError:
    MAP_AVAILABLE = False

from navamesh.config import load_config

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [reticulum_bridge] %(message)s",
)
logger = logging.getLogger("reticulum_bridge")

if not MAP_AVAILABLE:
    logger.warning(
        "staticmap/pillow not installed — 'map' command will return text summary only. "
        "Run: pip install staticmap pillow --break-system-packages"
    )


# ── Image transport profiles ──────────────────────────────────────────────────
IMAGE_PROFILES = {
    "lora": {
        "max_dimension": 200,
        "quality":       35,
        "max_bytes":     12_000,
    },
    "halow": {
        "max_dimension": 640,
        "quality":       72,
        "max_bytes":     120_000,
    },
    "wifi": {
        "max_dimension": 900,
        "quality":       78,
        "max_bytes":     220_000,
    },
}

_FIELD_TYPE = "jpg"
_MIME_TYPE  = "image/jpeg"


def _image_profile() -> dict:
    name    = os.getenv("MAP_LINK_PROFILE", "lora").strip().lower()
    profile = dict(IMAGE_PROFILES.get(name, IMAGE_PROFILES["lora"]))
    if os.getenv("MAP_MAX_DIMENSION"):
        profile["max_dimension"] = int(os.getenv("MAP_MAX_DIMENSION"))
    if os.getenv("MAP_JPEG_QUALITY"):
        profile["quality"] = int(os.getenv("MAP_JPEG_QUALITY"))
    if os.getenv("MAP_MAX_BYTES"):
        profile["max_bytes"] = int(os.getenv("MAP_MAX_BYTES"))
    return profile


# ── Config ────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ReticulumBridgeConfig:
    rns_config_dir: str
    lxmf_storage_dir: str
    display_name: str
    announce_interval: int
    map_tile_url: str
    map_tile_fallback: str
    map_max_dimension: int
    map_jpeg_quality: int
    map_max_bytes: int
    soil_wet_threshold: float
    soil_dry_threshold: float
    map_outlier_guard_enabled: bool
    map_mesh_neighbor_radius_m: float
    map_outlier_min_nodes: int
    map_separate_farm_distance_m: float
    pg_dsn: str


def load_rns_config() -> ReticulumBridgeConfig:
    profile = _image_profile()

    def _int(name: str, default: int) -> int:
        v = os.getenv(name)
        return int(v) if v else default

    def _float(name: str, default: float) -> float:
        v = os.getenv(name)
        return float(v) if v else default

    def _bool(name: str, default: bool) -> bool:
        v = os.getenv(name)
        if v is None or v == "":
            return default
        return v.strip().lower() in {"1", "true", "yes", "on"}

    return ReticulumBridgeConfig(
        rns_config_dir=os.getenv("RNS_CONFIG_DIR", os.path.expanduser("~/.reticulum")),
        lxmf_storage_dir=os.path.expanduser(
            os.getenv("LXMF_STORAGE_DIR", "~/.navamesh_lxmf")
        ),
        display_name=os.getenv("LXMF_DISPLAY_NAME", "Navamesh Gateway"),
        announce_interval=_int("LXMF_ANNOUNCE_INTERVAL", 180),
        map_tile_url=os.getenv(
            "MAP_TILE_URL",
            "http://127.0.0.1:8080/data/florida/{z}/{x}/{y}.png",
        ),
        map_tile_fallback=os.getenv(
            "MAP_TILE_FALLBACK",
            "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
        ),
        # Resolved once at startup from the active profile; per-key .env
        # overrides (MAP_MAX_DIMENSION etc.) are applied inside _image_profile().
        map_max_dimension=profile["max_dimension"],
        map_jpeg_quality=profile["quality"],
        map_max_bytes=profile["max_bytes"],
        soil_wet_threshold=_float("SOIL_WET_THRESHOLD", 60.0),
        soil_dry_threshold=_float("SOIL_DRY_THRESHOLD", 30.0),
        map_outlier_guard_enabled=_bool("MAP_OUTLIER_GUARD_ENABLED", True),
        map_mesh_neighbor_radius_m=_float("MAP_MESH_NEIGHBOR_RADIUS_METERS", 800.0),
        map_outlier_min_nodes=_int("MAP_OUTLIER_MIN_NODES", 3),
        map_separate_farm_distance_m=_float("MAP_SEPARATE_FARM_DISTANCE_METERS", 3000.0),
        pg_dsn=os.getenv("PG_DSN", ""),
    )


# ── Node snapshot + DB reader ─────────────────────────────────────────────────

@dataclass
class NodeSnapshot:
    node_id: str
    ts: Optional[int] = None
    soil_raw: Optional[float] = None
    soil_percent: Optional[float] = None
    battery_level: Optional[float] = None
    battery_usb: Optional[bool] = None
    voltage: Optional[float] = None
    uptime_seconds: Optional[int] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    alt: Optional[float] = None
    rx_rssi: Optional[float] = None
    rx_snr: Optional[float] = None


@dataclass
class MeshSelection:
    """Result of splitting GPS nodes into a main mesh + separated outlier clusters."""
    plotted: Dict[str, NodeSnapshot]
    omitted: List[Dict[str, NodeSnapshot]]
    nearest_omitted_m: Optional[float] = None
    # (node_id, distance from that node's component to the main mesh, in meters)
    omitted_detail: List[Tuple[str, float]] = None  # type: ignore[assignment]

    def __post_init__(self):
        if self.omitted_detail is None:
            self.omitted_detail = []


def _nodes_from_postgres(dsn: str) -> Dict[str, NodeSnapshot]:
    if not dsn or _psycopg is None:
        if _psycopg is None:
            logger.warning("psycopg not installed — install it to enable DB reads")
        return {}
    try:
        with _psycopg.connect(dsn, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT node_id, EXTRACT(EPOCH FROM last_seen)::bigint, lat, lon, metadata "
                    "FROM mesh_nodes ORDER BY last_seen DESC"
                )
                rows = cur.fetchall()
    except Exception as exc:
        logger.warning("DB query for node snapshots failed: %s", exc)
        return {}

    snapshots: Dict[str, NodeSnapshot] = {}
    for node_id, ts, lat, lon, meta in rows:
        if meta is None:
            meta = {}
        snapshots[node_id] = NodeSnapshot(
            node_id=node_id,
            ts=int(ts) if ts is not None else None,
            soil_percent=meta.get("soil_percent"),
            battery_level=meta.get("battery_level"),
            battery_usb=meta.get("battery_usb"),
            voltage=meta.get("voltage"),
            uptime_seconds=meta.get("uptime_seconds"),
            lat=lat,
            lon=lon,
            rx_rssi=meta.get("rx_rssi"),
            rx_snr=meta.get("rx_snr"),
        )
    logger.debug("Loaded %d node(s) from Postgres.", len(snapshots))
    return snapshots


# ── Formatters ────────────────────────────────────────────────────────────────

def _fmt_node(node_id: str) -> str:
    return f"Node {node_id[-4:]}" if node_id.startswith("!") else node_id

def _fmt_ts(ts: Optional[int]) -> str:
    if ts is None:
        return "never"
    try:
        import datetime
        return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts)

def _fmt_uptime(seconds: Optional[int]) -> str:
    if seconds is None:
        return "N/A"
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    if h: return f"{h}h {m}m"
    if m: return f"{m}m {s}s"
    return f"{s}s"

def _header(title: str) -> str:
    return f"{'─'*30}\n{title}\n{'─'*30}\n"

def fmt_status(nodes: Dict[str, NodeSnapshot]) -> str:
    if not nodes:
        return "No node data in database yet. Are field nodes transmitting?"
    lines = [_header("🌱 Navamesh Status")]
    for node_id, snap in sorted(nodes.items()):
        lines.append(f"[ {_fmt_node(node_id)} ]  {node_id}")
        lines.append(f"  Last seen:  {_fmt_ts(snap.ts)}")
        if snap.soil_percent is not None:
            lines.append(f"  Soil:       {snap.soil_percent:.1f}%")
        elif snap.soil_raw is not None:
            lines.append(f"  Soil ADC:   {snap.soil_raw}")
        if snap.battery_usb:
            lines.append(f"  Battery:    USB (charging)")
        elif snap.battery_level is not None:
            lines.append(f"  Battery:    {snap.battery_level:.0f}%")
        if snap.voltage is not None:
            lines.append(f"  Voltage:    {snap.voltage:.2f}V")
        if snap.rx_rssi is not None:
            lines.append(f"  RSSI/SNR:   {snap.rx_rssi} dBm / {snap.rx_snr} dB")
        if snap.lat is not None:
            lines.append(f"  Position:   {snap.lat:.6f}, {snap.lon:.6f}")
        lines.append("")
    return "\n".join(lines)

def fmt_soil(nodes: Dict[str, NodeSnapshot]) -> str:
    if not nodes: return "No soil data in database yet."
    lines = [_header("🌱 Soil Moisture")]
    for node_id, snap in sorted(nodes.items()):
        if snap.soil_percent is not None:
            lines.append(f"{_fmt_node(node_id)}: {snap.soil_percent:.1f}%  ({_fmt_ts(snap.ts)})")
        elif snap.soil_raw is not None:
            lines.append(f"{_fmt_node(node_id)}: ADC={snap.soil_raw}  ({_fmt_ts(snap.ts)})")
        else:
            lines.append(f"{_fmt_node(node_id)}: no soil data yet")
    return "\n".join(lines)

def fmt_battery(nodes: Dict[str, NodeSnapshot]) -> str:
    if not nodes: return "No battery data in database yet."
    lines = [_header("🔋 Battery")]
    for node_id, snap in sorted(nodes.items()):
        bat = "USB (charging)" if snap.battery_usb else (
            f"{snap.battery_level:.0f}%" if snap.battery_level is not None else "no data"
        )
        volt = f"  {snap.voltage:.2f}V" if snap.voltage is not None else ""
        up   = f"  up {_fmt_uptime(snap.uptime_seconds)}" if snap.uptime_seconds else ""
        lines.append(f"{_fmt_node(node_id)}: {bat}{volt}{up}  ({_fmt_ts(snap.ts)})")
    return "\n".join(lines)

def fmt_position(nodes: Dict[str, NodeSnapshot]) -> str:
    if not nodes: return "No position data in database yet."
    lines = [_header("📍 Position")]
    for node_id, snap in sorted(nodes.items()):
        if snap.lat is not None:
            alt = f"  alt={snap.alt}m" if snap.alt is not None else ""
            lines.append(f"{_fmt_node(node_id)}: {snap.lat:.6f}, {snap.lon:.6f}{alt}  ({_fmt_ts(snap.ts)})")
        else:
            lines.append(f"{_fmt_node(node_id)}: no GPS fix yet")
    return "\n".join(lines)

def fmt_link(nodes: Dict[str, NodeSnapshot]) -> str:
    if not nodes: return "No link data in database yet."
    lines = [_header("📡 Link Quality")]
    for node_id, snap in sorted(nodes.items()):
        if snap.rx_rssi is not None:
            lines.append(f"{_fmt_node(node_id)}: RSSI={snap.rx_rssi} dBm  SNR={snap.rx_snr} dB  ({_fmt_ts(snap.ts)})")
        else:
            lines.append(f"{_fmt_node(node_id)}: no link data yet")
    return "\n".join(lines)

HELP_TEXT = """🌱 Navamesh Gateway — Commands

  status       — full summary of all nodes
  soil         — soil moisture readings
  battery      — battery levels & uptime
  position     — GPS coordinates
  link         — RSSI/SNR link quality
  map          — rendered map image (all nodes)
  map <id>     — rendered map image (one node)
  nodes        — list all known node IDs
  help         — this message"""


# ── Map renderer ──────────────────────────────────────────────────────────────

def _resolve_tile_url(cfg: ReticulumBridgeConfig, geo_nodes: dict) -> Optional[str]:
    """
    Probe the local tile server using a tile derived from actual node coordinates.
    Falls back to OSM if unreachable. Fully location-agnostic — no hardcoded coords.
    """
    import math

    if geo_nodes:
        # Pick any node with a GPS fix to derive a real tile coordinate
        snap = next(iter(geo_nodes.values()))
        lat, lon = snap.lat, snap.lon
    else:
        # No nodes with GPS yet — can't probe, go straight to fallback
        if cfg.map_tile_fallback:
            logger.warning("No GPS nodes available to probe tile server — falling back to OSM")
            return cfg.map_tile_fallback
        return None

    z = 14
    n = 2 ** z
    x = int((lon + 180.0) / 360.0 * n)
    lat_r = math.radians(lat)
    y = int((1.0 - math.log(math.tan(lat_r) + 1.0 / math.cos(lat_r)) / math.pi) / 2.0 * n)
    test_url = cfg.map_tile_url.format(z=z, x=x, y=y)

    try:
        urllib.request.urlopen(test_url, timeout=2)
        return cfg.map_tile_url
    except Exception:
        if cfg.map_tile_fallback:
            logger.warning("Local tile server unreachable — falling back to OSM")
            return cfg.map_tile_fallback
        return None

def _pin_color(snap: NodeSnapshot, cfg: ReticulumBridgeConfig) -> str:
    soil = snap.soil_percent
    if soil is None:                        return "#22cc44"
    if soil >= cfg.soil_wet_threshold:      return "#2255ff"
    if soil <= cfg.soil_dry_threshold:      return "#ff3322"
    return "#22cc44"

def _lonlat_to_pixel(lon, lat, center_lon, center_lat, zoom, w, h):
    def to_tile(lat_d, lon_d, z):
        r = math.radians(lat_d)
        n = 2 ** z
        return (lon_d + 180.0) / 360.0 * n, (1.0 - math.asinh(math.tan(r)) / math.pi) / 2.0 * n
    cx, cy = to_tile(center_lat, center_lon, zoom)
    px, py = to_tile(lat, lon, zoom)
    return int((px - cx) * 256 + w / 2), int((py - cy) * 256 + h / 2)


def _best_zoom(lats, lons, px: int) -> int:
    if len(lats) < 2:
        return 17
    pad = 0.25
    def _merc_y(lat_d):
        r = math.radians(lat_d)
        return (1.0 - math.asinh(math.tan(r)) / math.pi) / 2.0
    dlon = (max(lons) - min(lons)) * (1 + pad) or 0.001
    dy   = abs(_merc_y(max(lats)) - _merc_y(min(lats))) * (1 + pad) or 0.001
    tiles = px / 256
    for z in range(18, 13, -1):
        n = 2 ** z
        if dlon / 360 * n <= tiles and dy * n <= tiles:
            return z
    return 14


# ── Connected-cluster outlier guardrail ──────────────────────────────────────

def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two lat/lon points, in meters."""
    R = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * R * math.asin(min(1.0, math.sqrt(a)))


def _connected_components(
    geo_nodes: Dict[str, NodeSnapshot],
    radius_m: float,
) -> List[Dict[str, NodeSnapshot]]:
    """
    Group GPS nodes into connected components. Two nodes are connected if their
    haversine distance is <= radius_m. Connectivity is transitive, so a long
    A-B-C-D chain stays a single component even if A and D are far apart.
    Returns components as node-id->snapshot dicts, sorted largest-first.
    """
    ids = list(geo_nodes.keys())
    visited: set = set()
    components: List[Dict[str, NodeSnapshot]] = []

    for start in ids:
        if start in visited:
            continue
        # BFS over neighbours within radius
        comp_ids = []
        queue = [start]
        visited.add(start)
        while queue:
            cur = queue.pop()
            comp_ids.append(cur)
            cs = geo_nodes[cur]
            for other in ids:
                if other in visited:
                    continue
                os_ = geo_nodes[other]
                if _haversine_m(cs.lat, cs.lon, os_.lat, os_.lon) <= radius_m:
                    visited.add(other)
                    queue.append(other)
        components.append({nid: geo_nodes[nid] for nid in comp_ids})

    components.sort(key=len, reverse=True)
    return components


def _select_main_mesh(
    geo_nodes: Dict[str, NodeSnapshot],
    cfg: ReticulumBridgeConfig,
) -> MeshSelection:
    """
    Split GPS nodes into a rendered set (main farm mesh + nearby stragglers) and
    clearly-separated outlier clusters that should be omitted from the normal map.

    Guard is skipped (everything plotted) when disabled, when there are fewer than
    map_outlier_min_nodes GPS nodes, or when the largest component itself is smaller
    than map_outlier_min_nodes (no real mesh — never hide most nodes).
    """
    if (
        not cfg.map_outlier_guard_enabled
        or len(geo_nodes) < cfg.map_outlier_min_nodes
    ):
        return MeshSelection(plotted=dict(geo_nodes), omitted=[])

    components = _connected_components(geo_nodes, cfg.map_mesh_neighbor_radius_m)
    main = components[0]
    if len(main) < cfg.map_outlier_min_nodes:
        return MeshSelection(plotted=dict(geo_nodes), omitted=[])

    plotted: Dict[str, NodeSnapshot] = dict(main)
    omitted: List[Dict[str, NodeSnapshot]] = []
    omitted_detail: List[Tuple[str, float]] = []

    for comp in components[1:]:
        # Min distance from this component to the current main mesh.
        comp_dist = min(
            _haversine_m(cs.lat, cs.lon, ms.lat, ms.lon)
            for cs in comp.values()
            for ms in main.values()
        )
        if comp_dist >= cfg.map_separate_farm_distance_m:
            omitted.append(comp)
            for nid in comp:
                omitted_detail.append((nid, comp_dist))
        else:
            # Nearby straggler — fold into the plotted set.
            plotted.update(comp)

    nearest_omitted_m: Optional[float] = None
    if omitted:
        nearest_omitted_m = min(
            _haversine_m(os_.lat, os_.lon, ps.lat, ps.lon)
            for comp in omitted
            for os_ in comp.values()
            for ps in plotted.values()
        )

    return MeshSelection(
        plotted=plotted,
        omitted=omitted,
        nearest_omitted_m=nearest_omitted_m,
        omitted_detail=omitted_detail,
    )


def render_map(
    nodes: Dict[str, NodeSnapshot],
    cfg: ReticulumBridgeConfig,
) -> Optional[Tuple[str, bytes, str]]:
    if not MAP_AVAILABLE:
        return None

    geo_nodes = {nid: s for nid, s in nodes.items() if s.lat is not None and s.lon is not None}
    if not geo_nodes:
        return None

    tile_url = _resolve_tile_url(cfg, geo_nodes)
    if not tile_url:
        return None

    max_dimension = cfg.map_max_dimension
    quality       = cfg.map_jpeg_quality
    max_bytes     = cfg.map_max_bytes

    lons_list  = [s.lon for s in geo_nodes.values()]
    lats_list  = [s.lat for s in geo_nodes.values()]
    center_lon = (min(lons_list) + max(lons_list)) / 2
    center_lat = (min(lats_list) + max(lats_list)) / 2

    render_size = max(max_dimension * 2, 480)
    zoom        = _best_zoom(lats_list, lons_list, render_size)

    smap = StaticMap(render_size, render_size, url_template=tile_url)
    for snap in geo_nodes.values():
        smap.add_marker(CircleMarker((snap.lon, snap.lat), _pin_color(snap, cfg), 18))

    image = smap.render(zoom=zoom)
    draw  = ImageDraw.Draw(image)
    font_size = max(28, render_size // 40)
    try:
        font = ImageFont.load_default(size=font_size)
    except TypeError:
        font = ImageFont.load_default()

    pin_r  = 18
    margin = max(14, render_size // 80)

    # Pixel center + collision box for every pin, so labels can avoid covering
    # pins other than the one they belong to.
    pin_centers: Dict[str, Tuple[int, int]] = {}
    pin_boxes: Dict[str, Tuple[int, int, int, int]] = {}
    for node_id, snap in geo_nodes.items():
        px, py = _lonlat_to_pixel(
            snap.lon, snap.lat, center_lon, center_lat,
            zoom, render_size, render_size,
        )
        pin_centers[node_id] = (px, py)
        pin_boxes[node_id] = (px - pin_r, py - pin_r, px + pin_r, py + pin_r)

    def _overlap_area(a, b) -> float:
        dx = min(a[2], b[2]) - max(a[0], b[0])
        dy = min(a[3], b[3]) - max(a[1], b[1])
        return dx * dy if dx > 0 and dy > 0 else 0.0

    def _out_of_bounds(box) -> float:
        """Area of the box falling outside the image rectangle."""
        total = (box[2] - box[0]) * (box[3] - box[1])
        inside = _overlap_area(box, (0, 0, render_size, render_size))
        return max(0.0, total - inside)

    # 8 compass directions, tried at increasing distances.
    DIRECTIONS = [(1, 0), (1, 1), (0, 1), (-1, 1), (-1, 0), (-1, -1), (0, -1), (1, -1)]
    placed: List[Tuple[int, int, int, int]] = []

    for node_id, snap in geo_nodes.items():
        px, py = pin_centers[node_id]
        soil_str = f"{snap.soil_percent:.0f}%" if snap.soil_percent is not None else "?"
        bat_str  = "USB" if snap.battery_usb else (
            f"{snap.battery_level:.0f}%" if snap.battery_level is not None else "?"
        )
        label = f"{node_id[-4:]}\nS:{soil_str} B:{bat_str}"

        b0 = draw.textbbox((0, 0), label, font=font)
        lw = b0[2] - b0[0]
        lh = b0[3] - b0[1]
        other_pins = [box for nid, box in pin_boxes.items() if nid != node_id]

        def _anchor_for(dirx, diry, dist):
            if dirx > 0:   ax = px + dist
            elif dirx < 0: ax = px - dist - lw
            else:          ax = px - lw / 2
            if diry > 0:   ay = py + dist
            elif diry < 0: ay = py - dist - lh
            else:          ay = py - lh / 2
            # Clamp inside the image so labels are never clipped at the edge.
            # (4 px keeps the box's padding inside; if the label is wider/taller
            # than the image the range collapses and _out_of_bounds catches it.)
            max_x = render_size - lw - 4
            max_y = render_size - lh - 4
            if max_x >= 4: ax = min(max(ax, 4), max_x)
            if max_y >= 4: ay = min(max(ay, 4), max_y)
            return int(ax), int(ay)

        def _box_for(ax, ay):
            return (ax - 4, ay - 4, ax + lw + 4, ay + lh + 4)

        chosen = None
        candidates = []
        for mult in (1, 2, 3):
            dist = mult * (pin_r + margin)
            for dirx, diry in DIRECTIONS:
                ax, ay = _anchor_for(dirx, diry, dist)
                box = _box_for(ax, ay)
                label_ov = sum(_overlap_area(box, p) for p in placed)
                pin_ov   = sum(_overlap_area(box, p) for p in other_pins)
                oob      = _out_of_bounds(box)
                candidates.append((ax, ay, box, label_ov, pin_ov, oob))
                if label_ov == 0 and pin_ov == 0 and oob == 0:
                    chosen = (ax, ay, box)
                    break
            if chosen:
                break

        if chosen is None:
            # No clean spot — pick the least-bad candidate. Area/penalty terms are
            # weighted to dominate the small distance-from-pin tie-breaker.
            def _score(c):
                ax, ay, box, label_ov, pin_ov, oob = c
                cx = (box[0] + box[2]) / 2
                cy = (box[1] + box[3]) / 2
                dist = math.hypot(cx - px, cy - py)
                return 10.0 * (label_ov + pin_ov + oob) + dist
            best = min(candidates, key=_score)
            chosen = (best[0], best[1], best[2])

        chosen_lx, chosen_ly, chosen_box = chosen
        placed.append(chosen_box)
        draw.rectangle(chosen_box, fill=(0, 0, 0, 200))
        draw.text((chosen_lx, chosen_ly), label, fill="white", font=font)
        cx_l = (chosen_box[0] + chosen_box[2]) // 2
        cy_l = (chosen_box[1] + chosen_box[3]) // 2
        draw.line([(px, py), (cx_l, cy_l)], fill=(255, 255, 255, 160), width=3)

    if image.mode != "RGB":
        image = image.convert("RGB")

    image.thumbnail((max_dimension, max_dimension))

    while quality >= 20:
        buf = io.BytesIO()
        image.save(buf, format="JPEG", quality=quality, optimize=True)
        if buf.tell() <= max_bytes:
            img_bytes = buf.getvalue()
            logger.info(
                "Map image ready: %d bytes  %dx%d px  quality=%d  profile=%s  nodes=%d",
                len(img_bytes), image.width, image.height, quality,
                os.getenv("MAP_LINK_PROFILE", "lora"), len(geo_nodes),
            )
            return _FIELD_TYPE, img_bytes, _MIME_TYPE
        quality -= 8

    logger.error(
        "Map image could not be compressed below %d bytes — not sending image",
        max_bytes,
    )
    return None


# ── Command handler ───────────────────────────────────────────────────────────

def handle_command(
    cmd: str,
    nodes: Dict[str, NodeSnapshot],
    cfg: ReticulumBridgeConfig,
) -> Tuple[str, Optional[Tuple[str, bytes, str]]]:
    parts   = cmd.strip().lower().split(None, 1)
    command = parts[0] if parts else ""
    target  = parts[1].strip() if len(parts) > 1 else None

    if command == "nodes":
        if not nodes: return "No nodes in database yet.", None
        return "Known field nodes:\n" + "\n".join(f"  {n}" for n in sorted(nodes)), None
    if command == "help":     return HELP_TEXT, None
    if command == "status":   return fmt_status(nodes), None
    if command == "soil":     return fmt_soil(nodes), None
    if command == "battery":  return fmt_battery(nodes), None
    if command == "position": return fmt_position(nodes), None
    if command == "link":     return fmt_link(nodes), None

    if command == "map":
        if not nodes:
            return "No sensor data in database yet — field nodes may not have reported in.", None

        if target:
            if target not in nodes:
                return f"Node '{target}' not found. Send 'nodes' to list all known nodes.", None
            map_nodes = {target: nodes[target]}
        else:
            map_nodes = nodes

        geo = {nid: s for nid, s in map_nodes.items()
               if s.lat is not None and s.lon is not None}
        no_gps = len(map_nodes) - len(geo)

        if target:
            # Explicitly requested node — never filter; render even if far away.
            sel = MeshSelection(plotted=dict(geo), omitted=[])
        else:
            sel = _select_main_mesh(geo, cfg)
            if sel.omitted:
                logger.info(
                    "Map outlier guard omitted %d node(s): %s",
                    len(sel.omitted_detail),
                    ", ".join(
                        f"{nid} ({dist / 1000:.1f} km)" for nid, dist in sel.omitted_detail
                    ),
                )

        plotted       = sel.plotted
        omitted_count = sum(len(c) for c in sel.omitted)
        image_result  = render_map(plotted, cfg)

        def _sep_str(m: float) -> str:
            return f"{m / 1000:.1f} km" if m >= 1000 else f"{m:.0f} m"

        if image_result:
            lines = [f"🗺️ Map: {len(plotted)} GPS node(s) plotted"]
            if sel.omitted and sel.nearest_omitted_m is not None:
                lines.append(
                    f"⚠️ {omitted_count} GPS node(s) omitted as separate cluster, "
                    f"nearest {_sep_str(sel.nearest_omitted_m)} from main mesh"
                )
            if no_gps:
                lines.append(f"⚠️ {no_gps} node(s) missing GPS")
            text_reply = "\n".join(lines)
        else:
            # Text fallback: list only the nodes intended for the image so the reply
            # never implies omitted-cluster nodes were drawn. For 'map <id>' show the
            # requested node even when it has no GPS fix.
            listed = map_nodes if target else plotted
            lines = [_header(f"🗺️  Navamesh Map  ({len(listed)} node(s))")]
            for node_id, snap in sorted(listed.items()):
                soil_str = f"{snap.soil_percent:.1f}%" if snap.soil_percent is not None else "no data"
                bat_str  = "USB" if snap.battery_usb else (
                    f"{snap.battery_level:.0f}%" if snap.battery_level is not None else "no data"
                )
                gps_str  = (
                    f"{snap.lat:.5f}, {snap.lon:.5f}"
                    if snap.lat is not None and snap.lon is not None
                    else "no GPS"
                )
                lines += [
                    f"  {_fmt_node(node_id)} ({node_id})",
                    f"    Soil:    {soil_str}",
                    f"    Battery: {bat_str}",
                    f"    GPS:     {gps_str}", "",
                ]
            if sel.omitted and sel.nearest_omitted_m is not None:
                lines.append(
                    f"⚠️  {omitted_count} GPS node(s) omitted as separate cluster, "
                    f"nearest {_sep_str(sel.nearest_omitted_m)} from main mesh."
                )
            if no_gps:
                lines.append(f"⚠️  {no_gps} node(s) have no GPS fix — omitted from map.")
            if not MAP_AVAILABLE:
                lines.append("ℹ️  Map unavailable — install staticmap+pillow on the Pi.")
            elif len(geo) == 0:
                lines.append("ℹ️  Map unavailable — no nodes have GPS coordinates yet.")
            else:
                lines.append("ℹ️  Map unavailable — tile server unreachable.")
            text_reply = "\n".join(lines)

        return text_reply, image_result

    return f"Unknown command: '{command}'\n\n{HELP_TEXT}", None


# ── LXMF gateway ─────────────────────────────────────────────────────────────

class LxmfGateway:
    def __init__(self, cfg: ReticulumBridgeConfig, navamesh_cfg: Any):
        self._cfg          = cfg
        self._navamesh_cfg = navamesh_cfg
        self._router: Optional[LXMF.LXMRouter] = None
        self._source: Optional[Any] = None
        self._lock = threading.Lock()

    def start(self) -> None:
        os.makedirs(self._cfg.lxmf_storage_dir, exist_ok=True)
        identity_path = os.path.join(self._cfg.lxmf_storage_dir, "identity")
        if os.path.exists(identity_path):
            identity = RNS.Identity.from_file(identity_path)
            logger.info("Loaded RNS identity from %s", identity_path)
        else:
            identity = RNS.Identity()
            identity.to_file(identity_path)
            logger.info("Generated new RNS identity → %s", identity_path)

        RNS.Reticulum(self._cfg.rns_config_dir)
        logger.info("Reticulum started (config: %s)", self._cfg.rns_config_dir)

        self._router = LXMF.LXMRouter(storagepath=self._cfg.lxmf_storage_dir, autopeer=False)
        self._source = self._router.register_delivery_identity(
            identity, display_name=self._cfg.display_name
        )
        self._router.register_delivery_callback(self._on_message)
        logger.info(
            "LXMF gateway ready.  Address: %s  Name: %s",
            RNS.prettyhexrep(self._source.hash), self._cfg.display_name,
        )

    def _on_message(self, message: Any) -> None:
        try:
            sender  = RNS.hexrep(message.source_hash, delimit=False)
            content = message.content.decode("utf-8").strip() if message.content else ""
            title   = message.title.decode("utf-8").strip()   if message.title   else ""
            cmd     = content or title
            logger.info("Command from %s: %r", sender, cmd)

            nodes        = _nodes_from_postgres(self._cfg.pg_dsn)
            text_reply, image_result = handle_command(cmd, nodes, self._cfg)

            if image_result:
                self._send_with_image(message, text_reply, image_result)
            else:
                self._send_text(message, text_reply)

        except Exception as exc:
            logger.error("Error handling message: %s", exc, exc_info=True)

    def _dest(self, original: Any) -> Any:
        identity = RNS.Identity.recall(original.source_hash)
        if identity is None:
            logger.info(
                "Identity not cached for %s — requesting path...",
                RNS.hexrep(original.source_hash, delimit=False),
            )
            RNS.Transport.request_path(original.source_hash)
            deadline = time.time() + 8
            while time.time() < deadline:
                identity = RNS.Identity.recall(original.source_hash)
                if identity is not None:
                    logger.info("Path resolved for %s", RNS.hexrep(original.source_hash, delimit=False))
                    break
                time.sleep(0.2)
        if identity is None:
            raise RuntimeError(
                f"Cannot resolve identity for {RNS.hexrep(original.source_hash)} — path unknown"
            )
        if not RNS.Transport.has_path(original.source_hash):
            logger.warning(
                "No RNS path cached for %s — DIRECT delivery will likely fail",
                RNS.hexrep(original.source_hash, delimit=False),
            )
        return RNS.Destination(
            identity,
            RNS.Destination.OUT, RNS.Destination.SINGLE,
            "lxmf", "delivery",
        )

    def _send_text(self, original: Any, text: str, title: str = "Navamesh") -> None:
        with self._lock:
            try:
                dest = self._dest(original)
            except RuntimeError as exc:
                logger.error("Cannot build destination: %s", exc)
                return
            source_hex = RNS.hexrep(original.source_hash, delimit=False)

            def _on_fail(m: Any) -> None:
                if m.state != LXMF.LXMessage.FAILED:
                    return
                logger.warning(
                    "DIRECT delivery failed for %s — retrying via OPPORTUNISTIC", source_hex
                )
                with self._lock:
                    try:
                        fallback = LXMF.LXMessage(
                            destination=dest,
                            source=self._source,
                            content=text,
                            title=title,
                            desired_method=LXMF.LXMessage.OPPORTUNISTIC,
                        )
                        self._router.handle_outbound(fallback)
                        logger.info("Reply re-queued via OPPORTUNISTIC for %s", source_hex)
                    except Exception as exc2:
                        logger.error("OPPORTUNISTIC fallback also failed: %s", exc2)

            msg = LXMF.LXMessage(
                destination=dest,
                source=self._source,
                content=text,
                title=title,
                desired_method=LXMF.LXMessage.DIRECT,
            )
            msg.register_delivery_callback(_on_fail)
            self._router.handle_outbound(msg)
            logger.info("Reply queued via DIRECT to %s", source_hex)

    def _send_with_image(
        self,
        original: Any,
        text: str,
        image_result: Tuple[str, bytes, str],
    ) -> None:
        img_type, img_bytes, mime_type = image_result
        filename = f"navamesh_map.{img_type}"
        fields = {LXMF.FIELD_IMAGE: [img_type, img_bytes]}
        try:
            fields[LXMF.FIELD_FILE_ATTACHMENTS] = [[filename, img_bytes, mime_type]]
        except AttributeError:
            pass
        with self._lock:
            try:
                dest = self._dest(original)
            except RuntimeError as exc:
                logger.error("Cannot build destination: %s — falling back to text", exc)
                self._send_text(original, text)
                return
            source_hex = RNS.hexrep(original.source_hash, delimit=False)

            def _on_fail(m: Any) -> None:
                if m.state != LXMF.LXMessage.FAILED:
                    return
                logger.warning(
                    "DIRECT map delivery failed for %s — retrying via OPPORTUNISTIC", source_hex
                )
                with self._lock:
                    try:
                        fallback = LXMF.LXMessage(
                            destination=dest,
                            source=self._source,
                            content=text,
                            title="Navamesh Map",
                            desired_method=LXMF.LXMessage.OPPORTUNISTIC,
                            fields=fields,
                        )
                        self._router.handle_outbound(fallback)
                        logger.info("Map re-queued via OPPORTUNISTIC for %s", source_hex)
                    except Exception as exc2:
                        logger.error("OPPORTUNISTIC map fallback also failed: %s", exc2)
                        self._send_text(original, text)

            msg = LXMF.LXMessage(
                destination=dest,
                source=self._source,
                content=text,
                title="Navamesh Map",
                desired_method=LXMF.LXMessage.DIRECT,
                fields=fields,
            )
            msg.register_delivery_callback(_on_fail)
            self._router.handle_outbound(msg)
            logger.info(
                "Map reply queued via DIRECT  image=%d bytes  type=%s  profile=%s",
                len(img_bytes), img_type, os.getenv("MAP_LINK_PROFILE", "lora"),
            )

    def announce(self) -> None:
        if self._router and self._source:
            try:
                self._source.announce()
                logger.info("Announce sent for %s", RNS.prettyhexrep(self._source.hash))
            except Exception as exc:
                logger.warning("Announce failed: %s", exc)

    def stop(self) -> None:
        pass


# ── Main bridge ───────────────────────────────────────────────────────────────

class ReticulumBridge:
    def __init__(self) -> None:
        self.cfg         = load_config()
        self.rns_cfg     = load_rns_config()
        self._stop_event = threading.Event()
        self._gateway    = LxmfGateway(self.rns_cfg, self.cfg)

    def start(self) -> None:
        logger.info(
            "Starting Reticulum LXMF gateway — pg_dsn=%s  profile=%s  "
            "max_dim=%d  quality=%d  max_bytes=%d",
            "set" if self.rns_cfg.pg_dsn else "NOT SET",
            os.getenv("MAP_LINK_PROFILE", "lora"),
            self.rns_cfg.map_max_dimension,
            self.rns_cfg.map_jpeg_quality,
            self.rns_cfg.map_max_bytes,
        )
        self._gateway.start()
        self._gateway.announce()
        threading.Timer(15.0, self._gateway.announce).start()
        threading.Thread(target=self._announce_loop, daemon=True).start()

    def stop(self) -> None:
        self._stop_event.set()
        self._gateway.stop()

    def _announce_loop(self) -> None:
        while not self._stop_event.wait(self.rns_cfg.announce_interval):
            try:
                self._gateway.announce()
            except Exception as exc:
                logger.warning("Announce loop error: %s", exc)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> int:
    bridge = ReticulumBridge()

    def _shutdown(signum: int, frame: Any) -> None:
        logger.info("Shutting down on signal %s ...", signum)
        bridge.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    bridge.start()
    logger.info("Navamesh Reticulum bridge running. Send 'help' from Sideband to start.")

    try:
        while not bridge._stop_event.is_set():
            bridge._stop_event.wait(1.0)
    finally:
        bridge.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
