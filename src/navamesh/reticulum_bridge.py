"""
reticulum_bridge.py — Navamesh LXMF command/response gateway (v3)
==================================================================

Listens for incoming LXMF messages from the farmer's Sideband app and
replies with live sensor data pulled from the local MQTT cache.

All processing happens on the Pi — the farmer's Android only needs
stock Sideband, no plugin required.

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
  Sideband's own "view" plugin defines a "lora" quality preset:
      max dimension: 160 px   JPEG quality: 18
  This produces images under ~3 KB — safe to send as a single LXMF
  FIELD_IMAGE attachment even over LoRa/HaLow links.

  For faster links (Wi-Fi HaLow direct) you can raise MAP_MAX_DIMENSION
  and MAP_JPEG_QUALITY in .env, but 160/18 is the safe default.

Required env vars (add to your .env):
  RNS_CONFIG_DIR          — path to Reticulum config dir (default: ~/.reticulum)
  LXMF_STORAGE_DIR        — where to store LXMF identity (default: ~/.navamesh_lxmf)
  LXMF_DISPLAY_NAME       — display name shown in Sideband (default: "Navamesh Gateway")

Optional env vars:
  LXMF_ANNOUNCE_INTERVAL  — seconds between RNS announces (default: 300)
  IGNORED_NODES           — comma-separated node IDs to ignore
  LOG_LEVEL               — logging level (default: INFO)

  # Map rendering (only needed for 'map' command)
  MAP_TILE_URL            — local tile server URL
  MAP_TILE_FALLBACK       — fallback tile URL (default: OSM)
  MAP_MAX_DIMENSION       — longest side of map image in pixels (default: 160)
                            160 = Sideband lora preset. Raise to 320 on fast links.
  MAP_JPEG_QUALITY        — JPEG quality 1-95 (default: 18)
                            18 = Sideband lora preset.
  SOIL_WET_THRESHOLD      — soil % for blue pin (default: 60)
  SOIL_DRY_THRESHOLD      — soil % for red pin (default: 30)

Dependencies:
  pip install rns lxmf paho-mqtt python-dotenv
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
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

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
    soil_wet_threshold: float
    soil_dry_threshold: float
    pg_dsn: str


def load_rns_config() -> ReticulumBridgeConfig:
    def _int(name: str, default: int) -> int:
        v = os.getenv(name)
        return int(v) if v else default

    def _float(name: str, default: float) -> float:
        v = os.getenv(name)
        return float(v) if v else default

    return ReticulumBridgeConfig(
        rns_config_dir=os.getenv("RNS_CONFIG_DIR", os.path.expanduser("~/.reticulum")),
        lxmf_storage_dir=os.path.expanduser(
            os.getenv("LXMF_STORAGE_DIR", "~/.navamesh_lxmf")
        ),
        display_name=os.getenv("LXMF_DISPLAY_NAME", "Navamesh Gateway"),
        announce_interval=_int("LXMF_ANNOUNCE_INTERVAL", 300),
        map_tile_url=os.getenv(
            "MAP_TILE_URL",
            "http://127.0.0.1:8080/data/florida/{z}/{x}/{y}.png",
        ),
        map_tile_fallback=os.getenv(
            "MAP_TILE_FALLBACK",
            "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
        ),
        map_max_dimension=_int("MAP_MAX_DIMENSION", 160),   # lora preset
        map_jpeg_quality=_int("MAP_JPEG_QUALITY", 18),      # lora preset
        soil_wet_threshold=_float("SOIL_WET_THRESHOLD", 60.0),
        soil_dry_threshold=_float("SOIL_DRY_THRESHOLD", 30.0),
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


def _nodes_from_postgres(dsn: str) -> Dict[str, NodeSnapshot]:
    """Query local Postgres for the latest snapshot of every node."""
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
            soil_raw=None,
            soil_percent=meta.get("soil_percent"),
            battery_level=meta.get("battery_level"),
            battery_usb=meta.get("battery_usb"),
            voltage=meta.get("voltage"),
            uptime_seconds=meta.get("uptime_seconds"),
            lat=lat,
            lon=lon,
            alt=None,
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

def _resolve_tile_url(cfg: ReticulumBridgeConfig) -> Optional[str]:
    try:
        urllib.request.urlopen("http://127.0.0.1:8080/", timeout=2)
        return cfg.map_tile_url
    except Exception:
        if cfg.map_tile_fallback:
            logger.warning("Local tile server unreachable — falling back to OSM")
            return cfg.map_tile_fallback
        return None

def _pin_color(snap: NodeSnapshot, cfg: ReticulumBridgeConfig) -> str:
    soil = snap.soil_percent
    if soil is None:     return "#22cc44"
    if soil >= cfg.soil_wet_threshold: return "#2255ff"
    if soil <= cfg.soil_dry_threshold: return "#ff3322"
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
    """Largest zoom where all nodes fit inside a px×px tile image with 25% padding."""
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


# Hard cap — LXMF over LoRa/HaLow reliably handles up to ~8 KB.
# Above ~10 KB the transport layer can drop or crash the chat.
_LXMF_LORA_MAX_BYTES = 8_000


def render_map(nodes: Dict[str, NodeSnapshot], cfg: ReticulumBridgeConfig) -> Optional[bytes]:
    """
    Render a JPEG map image safe for LXMF FIELD_IMAGE over LoRa/HaLow.

    Strategy (mirrors Sideband's view.py lora preset):
      1. Render at 2× target size so staticmap draws legible pins/labels
      2. thumbnail() to scale down to map_max_dimension on the longest side
      3. Save JPEG at map_jpeg_quality
      4. If still > _LXMF_LORA_MAX_BYTES, iteratively reduce quality until
         it fits — bottom floor is quality=5 to avoid total garbage.

    Safe defaults  →  MAP_MAX_DIMENSION=160  MAP_JPEG_QUALITY=18  (~2-3 KB)
    Bumped defaults →  MAP_MAX_DIMENSION=320  MAP_JPEG_QUALITY=50  (~10-20 KB, risky)
    """
    if not MAP_AVAILABLE:
        return None

    geo_nodes = {nid: s for nid, s in nodes.items() if s.lat is not None and s.lon is not None}
    if not geo_nodes:
        return None

    tile_url = _resolve_tile_url(cfg)
    if not tile_url:
        return None

    lons_list  = [s.lon for s in geo_nodes.values()]
    lats_list  = [s.lat for s in geo_nodes.values()]
    center_lon = (min(lons_list) + max(lons_list)) / 2
    center_lat = (min(lats_list) + max(lats_list)) / 2

    # Render at 2× so pins and text look decent before thumbnail shrink
    render_size = max(cfg.map_max_dimension * 2, 320)
    zoom = _best_zoom(lats_list, lons_list, render_size)

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

    placed = []
    for node_id, snap in geo_nodes.items():
        px, py = _lonlat_to_pixel(
            snap.lon, snap.lat, center_lon, center_lat,
            zoom, render_size, render_size,
        )
        soil_str = f"{snap.soil_percent:.0f}%" if snap.soil_percent is not None else "?"
        bat_str  = "USB" if snap.battery_usb else (
            f"{snap.battery_level:.0f}%" if snap.battery_level is not None else "?"
        )
        label = f"{node_id[-4:]}\nS:{soil_str} B:{bat_str}"
        pin_r = 18
        margin = max(14, render_size // 80)
        chosen_lx, chosen_ly, chosen_box = None, None, None
        for sx, sy in [(1, 1), (-1, 1), (1, -1), (-1, -1)]:
            lx = px + sx * (pin_r + margin)
            ly = py - sy * (pin_r + margin)
            b = draw.textbbox((lx, ly), label, font=font)
            box = (b[0]-4, b[1]-4, b[2]+4, b[3]+4)
            if not any(not(box[2]<p[0] or box[0]>p[2] or box[3]<p[1] or box[1]>p[3]) for p in placed):
                chosen_lx, chosen_ly, chosen_box = lx, ly, box
                break
        if chosen_box is None:
            lx = px + (pin_r + margin)
            ly = py - (pin_r + margin)
            b = draw.textbbox((lx, ly), label, font=font)
            chosen_lx, chosen_ly = lx, ly
            chosen_box = (b[0]-4, b[1]-4, b[2]+4, b[3]+4)
        placed.append(chosen_box)
        draw.rectangle(chosen_box, fill=(0, 0, 0, 200))
        draw.text((chosen_lx, chosen_ly), label, fill="white", font=font)
        cx = (chosen_box[0] + chosen_box[2]) // 2
        cy = (chosen_box[1] + chosen_box[3]) // 2
        draw.line([(px, py), (cx, cy)], fill=(255, 255, 255, 160), width=3)

    # Scale down — exactly as Sideband's view.py lora preset does it
    if image.mode in ("RGBA", "P"):
        image = image.convert("RGB")
    image.thumbnail((cfg.map_max_dimension, cfg.map_max_dimension))

    # First attempt at configured quality
    quality = cfg.map_jpeg_quality
    buf = io.BytesIO()
    image.save(buf, format="WEBP", quality=quality)

    # Safety net: re-compress at lower quality until under the hard cap
    while buf.tell() > _LXMF_LORA_MAX_BYTES and quality > 5:
        quality = max(5, quality - 10)
        buf = io.BytesIO()
        image.save(buf, format="WEBP", quality=quality)
        logger.warning(
            "Image too large for LXMF — re-compressing at quality=%d (%d bytes)",
            quality, buf.tell(),
        )

    final_bytes = buf.tell()
    if final_bytes > _LXMF_LORA_MAX_BYTES:
        logger.error(
            "Map image still %d bytes after minimum quality — not sending to avoid crash",
            final_bytes,
        )
        return None  # caller will fall back to text-only reply

    logger.info(
        "Map JPEG: %d bytes  %dx%d px  quality=%d  nodes=%d",
        final_bytes, image.width, image.height, quality, len(geo_nodes),
    )
    return buf.getvalue()


# ── Command handler ───────────────────────────────────────────────────────────

def handle_command(
    cmd: str,
    nodes: Dict[str, NodeSnapshot],
    cfg: ReticulumBridgeConfig,
) -> Tuple[str, Optional[bytes]]:
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

        geo_count = sum(1 for s in map_nodes.values() if s.lat is not None)
        no_gps    = len(map_nodes) - geo_count

        img_bytes = render_map(map_nodes, cfg)

        if img_bytes:
            # Keep text minimal when image is attached — total LXMF payload must stay small
            lines = [f"🗺️ Map: {geo_count} node(s) plotted"]
            if no_gps:
                lines.append(f"⚠️ {no_gps} node(s) missing GPS")
            text_reply = "\n".join(lines)
        else:
            # No image — send the full summary as text fallback
            lines = [_header(f"🗺️  Navamesh Map  ({len(map_nodes)} node(s))")]
            for node_id, snap in sorted(map_nodes.items()):
                soil_str = f"{snap.soil_percent:.1f}%" if snap.soil_percent is not None else "no data"
                bat_str  = "USB" if snap.battery_usb else (
                    f"{snap.battery_level:.0f}%" if snap.battery_level is not None else "no data"
                )
                gps_str = f"{snap.lat:.5f}, {snap.lon:.5f}" if snap.lat is not None else "no GPS"
                lines += [f"  {_fmt_node(node_id)} ({node_id})",
                          f"    Soil:    {soil_str}",
                          f"    Battery: {bat_str}",
                          f"    GPS:     {gps_str}", ""]
            if no_gps:
                lines.append(f"⚠️  {no_gps} node(s) have no GPS fix — omitted from map.")
            if not MAP_AVAILABLE:
                lines.append("ℹ️  Map unavailable — install staticmap+pillow on the Pi.")
            elif geo_count == 0:
                lines.append("ℹ️  Map unavailable — no nodes have GPS coordinates yet.")
            else:
                lines.append("ℹ️  Map unavailable — tile server unreachable.")
            text_reply = "\n".join(lines)

        return text_reply, img_bytes

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
        self._router.announce(self._source.hash)
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

            nodes = _nodes_from_postgres(self._cfg.pg_dsn)
            text_reply, img_bytes = handle_command(cmd, nodes, self._cfg)

            if img_bytes:
                self._send_with_image(message, text_reply, img_bytes)
            else:
                self._send_text(message, text_reply)

        except Exception as exc:
            logger.error("Error handling message: %s", exc, exc_info=True)

    def _dest(self, original: Any) -> Any:
        return RNS.Destination(
            RNS.Identity.recall(original.source_hash),
            RNS.Destination.OUT, RNS.Destination.SINGLE,
            "lxmf", "delivery",
        )

    def _send_text(self, original: Any, text: str, title: str = "Navamesh") -> None:
        with self._lock:
            try:
                self._router.handle_outbound(LXMF.LXMessage(
                    destination=self._dest(original),
                    source=self._source,
                    content=text,
                    title=title,
                    desired_method=LXMF.LXMessage.DIRECT,
                ))
                logger.info("Text reply queued to %s", RNS.hexrep(original.source_hash, delimit=False))
            except Exception as exc:
                logger.error("Failed to send text reply: %s", exc)

    def _send_with_image(self, original: Any, text: str, img_bytes: bytes) -> None:
        """
        Send text + JPEG image together as a single LXMF FIELD_IMAGE message.
        Image is already sized to the lora preset (≤160px, quality 18, ~2-3 KB).
        """
        with self._lock:
            try:
                self._router.handle_outbound(LXMF.LXMessage(
                    destination=self._dest(original),
                    source=self._source,
                    content=text,
                    title="Navamesh Map",
                    desired_method=LXMF.LXMessage.DIRECT,
                    fields={LXMF.FIELD_IMAGE: ["webp", img_bytes]},
                ))
                logger.info(
                    "Map reply queued to %s  image=%d bytes",
                    RNS.hexrep(original.source_hash, delimit=False), len(img_bytes),
                )
            except Exception as exc:
                logger.error("Failed to send map reply: %s  — falling back to text", exc)
                self._send_text(original, text)

    def announce(self) -> None:
        if self._router and self._source:
            self._router.announce(self._source.hash)

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
        logger.info("Starting Reticulum LXMF gateway (DB-backed, pg_dsn=%s)...",
                    "set" if self.rns_cfg.pg_dsn else "NOT SET")
        self._gateway.start()
        threading.Thread(target=self._announce_loop, daemon=True).start()

    def stop(self) -> None:
        self._stop_event.set()
        self._gateway.stop()

    def _announce_loop(self) -> None:
        while not self._stop_event.wait(self.rns_cfg.announce_interval):
            self._gateway.announce()


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
