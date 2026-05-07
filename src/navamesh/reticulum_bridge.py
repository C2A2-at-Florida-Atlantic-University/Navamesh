"""
reticulum_bridge.py — Navamesh LXMF command/response gateway (v2)
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
  map          — PNG map image + GeoJSON for all nodes (Pi-rendered)
  map <id>     — PNG map image + GeoJSON for one specific node
  nodes        — list all known node IDs
  help         — list available commands

Architecture
------------
  Farmer types command in Sideband (Android)
    → LXMF message over Reticulum (mesh → Wi-Fi HaLow backhaul)
    → THIS SERVICE on house node (Pi)
    → looks up latest data from MQTT cache
    → renders map image + GeoJSON entirely on the Pi
    → replies directly to farmer's Sideband over Wi-Fi HaLow

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
                            (default: http://127.0.0.1:8080/data/florida/{z}/{x}/{y}.png)
  MAP_TILE_FALLBACK       — fallback tile URL if local server unreachable
                            (default: https://tile.openstreetmap.org/{z}/{x}/{y}.png)
                            Set to empty string to disable fallback entirely.
  MAP_WIDTH               — map image width in pixels (default: 800)
  MAP_HEIGHT              — map image height in pixels (default: 600)
  SOIL_WET_THRESHOLD      — soil % at or above which pin is blue (default: 60)
  SOIL_DRY_THRESHOLD      — soil % at or below which pin is red (default: 30)

Dependencies:
  pip install rns lxmf paho-mqtt python-dotenv
  pip install staticmap pillow    # optional — only needed for map rendering
"""

from __future__ import annotations

import io
import json
import logging
import math
import os
import signal
import sys
import threading
import time
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(usecwd=True))

import paho.mqtt.client as mqtt

try:
    import RNS
    import LXMF
except ImportError as exc:
    raise SystemExit(
        "Reticulum and LXMF are required:\n  pip install rns lxmf"
    ) from exc

# Optional map rendering dependencies
try:
    from staticmap import StaticMap, CircleMarker
    from PIL import ImageDraw, ImageFont
    MAP_AVAILABLE = True
except ImportError:
    MAP_AVAILABLE = False

from navamesh.config import load_config

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [reticulum_bridge] %(message)s",
)
logger = logging.getLogger("reticulum_bridge")

if not MAP_AVAILABLE:
    logger.warning(
        "staticmap/pillow not installed — 'map' command will return GeoJSON only. "
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
    map_width: int
    map_height: int
    soil_wet_threshold: float
    soil_dry_threshold: float


def load_rns_config() -> ReticulumBridgeConfig:
    def _int(name: str, default: int) -> int:
        v = os.getenv(name)
        return int(v) if v else default

    def _float(name: str, default: float) -> float:
        v = os.getenv(name)
        return float(v) if v else default

    return ReticulumBridgeConfig(
        rns_config_dir=os.getenv(
            "RNS_CONFIG_DIR", os.path.expanduser("~/.reticulum")
        ),
        lxmf_storage_dir=os.path.expanduser(
            os.getenv("LXMF_STORAGE_DIR", "~/.navamesh_lxmf")
        ),
        display_name=os.getenv("LXMF_DISPLAY_NAME", "Navamesh Gateway"),
        announce_interval=_int("LXMF_ANNOUNCE_INTERVAL", 300),
        map_tile_url=os.getenv(
            "MAP_TILE_URL",
            "http://127.0.0.1:8080/data/florida/{z}/{x}/{y}.png"
        ),
        map_tile_fallback=os.getenv(
            "MAP_TILE_FALLBACK",
            "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
        ),
        map_width=_int("MAP_WIDTH", 800),
        map_height=_int("MAP_HEIGHT", 600),
        soil_wet_threshold=_float("SOIL_WET_THRESHOLD", 60.0),
        soil_dry_threshold=_float("SOIL_DRY_THRESHOLD", 30.0),
    )


# ── Sensor cache ──────────────────────────────────────────────────────────────

@dataclass
class NodeSnapshot:
    """Latest known state for a single field node."""
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


class SensorCache:
    """Thread-safe store of the latest snapshot per node."""

    def __init__(self):
        self._data: Dict[str, NodeSnapshot] = {}
        self._lock = threading.Lock()

    def update(self, node_id: str, **kwargs) -> None:
        with self._lock:
            snap = self._data.setdefault(node_id, NodeSnapshot(node_id=node_id))
            for k, v in kwargs.items():
                if hasattr(snap, k) and v is not None:
                    setattr(snap, k, v)

    def all_nodes(self) -> Dict[str, NodeSnapshot]:
        with self._lock:
            return dict(self._data)

    def node(self, node_id: str) -> Optional[NodeSnapshot]:
        with self._lock:
            return self._data.get(node_id)


# ── Plain-text response formatters (kept from v1) ─────────────────────────────

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
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def _header(title: str) -> str:
    return f"{'─' * 30}\n{title}\n{'─' * 30}\n"


def fmt_status(cache: SensorCache) -> str:
    nodes = cache.all_nodes()
    if not nodes:
        return "No node data received yet. Are field nodes transmitting?"
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


def fmt_soil(cache: SensorCache) -> str:
    nodes = cache.all_nodes()
    if not nodes:
        return "No soil data received yet."
    lines = [_header("🌱 Soil Moisture")]
    for node_id, snap in sorted(nodes.items()):
        if snap.soil_percent is not None:
            lines.append(f"{_fmt_node(node_id)}: {snap.soil_percent:.1f}%  ({_fmt_ts(snap.ts)})")
        elif snap.soil_raw is not None:
            lines.append(f"{_fmt_node(node_id)}: ADC={snap.soil_raw}  ({_fmt_ts(snap.ts)})")
        else:
            lines.append(f"{_fmt_node(node_id)}: no soil data yet")
    return "\n".join(lines)


def fmt_battery(cache: SensorCache) -> str:
    nodes = cache.all_nodes()
    if not nodes:
        return "No battery data received yet."
    lines = [_header("🔋 Battery")]
    for node_id, snap in sorted(nodes.items()):
        if snap.battery_usb:
            bat = "USB (charging)"
        elif snap.battery_level is not None:
            bat = f"{snap.battery_level:.0f}%"
        else:
            bat = "no data"
        volt = f"  {snap.voltage:.2f}V" if snap.voltage is not None else ""
        up = f"  up {_fmt_uptime(snap.uptime_seconds)}" if snap.uptime_seconds else ""
        lines.append(f"{_fmt_node(node_id)}: {bat}{volt}{up}  ({_fmt_ts(snap.ts)})")
    return "\n".join(lines)


def fmt_position(cache: SensorCache) -> str:
    nodes = cache.all_nodes()
    if not nodes:
        return "No position data received yet."
    lines = [_header("📍 Position")]
    for node_id, snap in sorted(nodes.items()):
        if snap.lat is not None:
            alt = f"  alt={snap.alt}m" if snap.alt is not None else ""
            lines.append(
                f"{_fmt_node(node_id)}: {snap.lat:.6f}, {snap.lon:.6f}{alt}  ({_fmt_ts(snap.ts)})"
            )
        else:
            lines.append(f"{_fmt_node(node_id)}: no GPS fix yet")
    return "\n".join(lines)


def fmt_link(cache: SensorCache) -> str:
    nodes = cache.all_nodes()
    if not nodes:
        return "No link data received yet."
    lines = [_header("📡 Link Quality")]
    for node_id, snap in sorted(nodes.items()):
        if snap.rx_rssi is not None:
            lines.append(
                f"{_fmt_node(node_id)}: RSSI={snap.rx_rssi} dBm  SNR={snap.rx_snr} dB  ({_fmt_ts(snap.ts)})"
            )
        else:
            lines.append(f"{_fmt_node(node_id)}: no link data yet")
    return "\n".join(lines)


HELP_TEXT = """🌱 Navamesh Gateway — Commands

  status       — full summary of all nodes
  soil         — soil moisture readings
  battery      — battery levels & uptime
  position     — GPS coordinates
  link         — RSSI/SNR link quality
  map          — rendered map image + GeoJSON (all nodes)
  map <id>     — rendered map image + GeoJSON (one node)
  nodes        — list all known node IDs
  help         — this message

Send any command from Sideband to query live data."""


# ── GeoJSON builder ───────────────────────────────────────────────────────────

def build_geojson(nodes: Dict[str, NodeSnapshot]) -> dict:
    """
    Build a GeoJSON FeatureCollection — one Feature per field node.
    Point geometry uses GPS coordinates (lon, lat per GeoJSON spec).
    All telemetry goes into the properties block.
    Nodes without GPS get null geometry but are still included.
    """
    features = []
    now = int(time.time())

    for node_id, snap in nodes.items():
        geometry = (
            {"type": "Point", "coordinates": [snap.lon, snap.lat]}
            if snap.lat is not None and snap.lon is not None
            else None
        )
        props = {
            "node_id":         node_id,
            "last_seen":       _fmt_ts(snap.ts),
            "last_seen_ago_s": (now - snap.ts) if snap.ts else None,
            "soil_percent":    snap.soil_percent,
            "soil_raw":        snap.soil_raw,
            "battery_level":   snap.battery_level,
            "battery_usb":     snap.battery_usb,
            "voltage":         snap.voltage,
            "uptime_seconds":  snap.uptime_seconds,
            "rx_rssi":         snap.rx_rssi,
            "rx_snr":          snap.rx_snr,
            "altitude":        snap.alt,
        }
        # Strip None values to keep payload lean over the radio link
        props = {k: v for k, v in props.items() if v is not None}

        features.append({
            "type":       "Feature",
            "geometry":   geometry,
            "properties": props,
        })

    return {"type": "FeatureCollection", "features": features}


# ── Map image renderer ────────────────────────────────────────────────────────

def _resolve_tile_url(cfg: ReticulumBridgeConfig) -> Optional[str]:
    """
    Check if the local tile server is reachable.
    Returns the local URL if up, fallback URL if set, or None if neither works.
    """
    try:
        urllib.request.urlopen("http://127.0.0.1:8080/", timeout=2)
        return cfg.map_tile_url
    except Exception:
        if cfg.map_tile_fallback:
            logger.warning(
                "Local tile server unreachable — falling back to: %s",
                cfg.map_tile_fallback,
            )
            return cfg.map_tile_fallback
        logger.error("Local tile server unreachable and MAP_TILE_FALLBACK is not set.")
        return None


def _pin_color(snap: NodeSnapshot, cfg: ReticulumBridgeConfig) -> str:
    """Return hex colour for a node's map pin based on soil moisture."""
    soil = snap.soil_percent
    if soil is None:
        return "#22cc44"   # green — no data
    if soil >= cfg.soil_wet_threshold:
        return "#2255ff"   # blue  — well watered
    if soil <= cfg.soil_dry_threshold:
        return "#ff3322"   # red   — needs water
    return "#22cc44"       # green — ok


def _lonlat_to_pixel(
    lon: float, lat: float,
    center_lon: float, center_lat: float,
    zoom: int, width: int, height: int,
) -> Tuple[int, int]:
    """Convert geographic coordinates to pixel position on the rendered map."""
    def to_tile(lat_d: float, lon_d: float, z: int) -> Tuple[float, float]:
        r = math.radians(lat_d)
        n = 2 ** z
        x = (lon_d + 180.0) / 360.0 * n
        y = (1.0 - math.asinh(math.tan(r)) / math.pi) / 2.0 * n
        return x, y

    ts = 256
    cx, cy = to_tile(center_lat, center_lon, zoom)
    px, py = to_tile(lat, lon, zoom)
    return (
        int((px - cx) * ts + width  / 2),
        int((py - cy) * ts + height / 2),
    )


def render_map(
    nodes: Dict[str, NodeSnapshot],
    cfg: ReticulumBridgeConfig,
) -> Optional[bytes]:
    """
    Render a PNG map image with all nodes plotted as colour-coded pins.

    Pin colours by soil moisture:
      Blue  → soil >= SOIL_WET_THRESHOLD  (well watered)
      Red   → soil <= SOIL_DRY_THRESHOLD  (needs water)
      Green → between thresholds or no data

    Each pin gets a label: node ID (last 4 chars), soil %, battery.

    Returns raw PNG bytes, or None if:
      - staticmap/pillow not installed
      - no nodes have GPS coordinates
      - tile server unreachable and no fallback
    """
    if not MAP_AVAILABLE:
        return None

    geo_nodes = {
        nid: snap for nid, snap in nodes.items()
        if snap.lat is not None and snap.lon is not None
    }
    if not geo_nodes:
        return None

    tile_url = _resolve_tile_url(cfg)
    if not tile_url:
        return None

    smap = StaticMap(cfg.map_width, cfg.map_height, url_template=tile_url)

    for node_id, snap in geo_nodes.items():
        color = _pin_color(snap, cfg)
        smap.add_marker(CircleMarker((snap.lon, snap.lat), color, 14))

    image = smap.render()
    draw  = ImageDraw.Draw(image)

    try:
        font = ImageFont.load_default()
    except Exception:
        font = None

    for node_id, snap in geo_nodes.items():
        px, py = _lonlat_to_pixel(
            snap.lon, snap.lat,
            smap.center[0], smap.center[1],
            smap.zoom, cfg.map_width, cfg.map_height,
        )
        soil_str = f"{snap.soil_percent:.0f}%" if snap.soil_percent is not None else "?"
        if snap.battery_usb:
            bat_str = "USB"
        elif snap.battery_level is not None:
            bat_str = f"{snap.battery_level:.0f}%"
        else:
            bat_str = "?"

        short_id = node_id[-4:] if node_id.startswith("!") else node_id
        label = f"{short_id}\nSoil:{soil_str} Bat:{bat_str}"

        lx, ly = px + 12, py - 24
        bbox = draw.textbbox((lx, ly), label, font=font)
        draw.rectangle(
            [bbox[0] - 2, bbox[1] - 2, bbox[2] + 2, bbox[3] + 2],
            fill=(0, 0, 0, 180),
        )
        draw.text((lx, ly), label, fill="white", font=font)

    buf = io.BytesIO()
    image.save(buf, format="PNG", optimize=True)
    logger.info("Map image rendered (%d bytes, %d nodes)", buf.tell(), len(geo_nodes))
    return buf.getvalue()


# ── Command handler ───────────────────────────────────────────────────────────

def handle_command(
    cmd: str,
    cache: SensorCache,
    cfg: ReticulumBridgeConfig,
) -> Tuple[str, Optional[bytes], Optional[str]]:
    """
    Parse a command string and return a 3-tuple:
      (text_reply, image_bytes_or_None, geojson_str_or_None)

    The caller sends text_reply as an LXMF message, then image and geojson
    as separate file attachments if present.
    """
    parts   = cmd.strip().lower().split(None, 1)
    command = parts[0] if parts else ""
    target  = parts[1].strip() if len(parts) > 1 else None

    # ── nodes ──────────────────────────────────────────────────────────────
    if command == "nodes":
        all_nodes = cache.all_nodes()
        if not all_nodes:
            return "No nodes seen yet.", None, None
        body = "Known field nodes:\n" + "\n".join(f"  {n}" for n in sorted(all_nodes))
        return body, None, None

    # ── help ───────────────────────────────────────────────────────────────
    if command == "help":
        return HELP_TEXT, None, None

    # ── plain-text commands ────────────────────────────────────────────────
    if command == "status":
        return fmt_status(cache), None, None
    if command == "soil":
        return fmt_soil(cache), None, None
    if command == "battery":
        return fmt_battery(cache), None, None
    if command == "position":
        return fmt_position(cache), None, None
    if command == "link":
        return fmt_link(cache), None, None

    # ── map command (Pi-rendered) ──────────────────────────────────────────
    if command == "map":
        all_nodes = cache.all_nodes()
        if not all_nodes:
            return "No sensor data yet — field nodes may not have reported in.", None, None

        # Filter to specific node if requested
        if target:
            if target not in all_nodes:
                return (
                    f"Node '{target}' not found. Send 'nodes' to list all known nodes.",
                    None, None,
                )
            nodes = {target: all_nodes[target]}
        else:
            nodes = all_nodes

        # Build GeoJSON — always available
        geojson     = build_geojson(nodes)
        geojson_str = json.dumps(geojson, indent=2)

        # Render map image on the Pi
        img_bytes = render_map(nodes, cfg)

        geo_nodes_count = sum(
            1 for s in nodes.values() if s.lat is not None
        )
        no_gps_count = len(nodes) - geo_nodes_count

        # Build the text summary that accompanies the image
        lines = [_header(f"🗺️  Navamesh Map  ({len(nodes)} node(s))")]
        for node_id, snap in sorted(nodes.items()):
            soil_str = f"{snap.soil_percent:.1f}%" if snap.soil_percent is not None else "no data"
            if snap.battery_usb:
                bat_str = "USB"
            elif snap.battery_level is not None:
                bat_str = f"{snap.battery_level:.0f}%"
            else:
                bat_str = "no data"
            gps_str = (
                f"{snap.lat:.5f}, {snap.lon:.5f}"
                if snap.lat is not None else "no GPS"
            )
            lines.append(f"  {_fmt_node(node_id)} ({node_id})")
            lines.append(f"    Soil:    {soil_str}")
            lines.append(f"    Battery: {bat_str}")
            lines.append(f"    GPS:     {gps_str}")
            lines.append("")

        if no_gps_count > 0:
            lines.append(f"⚠️  {no_gps_count} node(s) have no GPS fix — omitted from map image.")
            lines.append("")

        if not img_bytes:
            if not MAP_AVAILABLE:
                lines.append("ℹ️  Map image unavailable — install staticmap+pillow on the Pi.")
            elif geo_nodes_count == 0:
                lines.append("ℹ️  Map image unavailable — no nodes have GPS coordinates yet.")
            else:
                lines.append("ℹ️  Map image unavailable — tile server unreachable.")
            lines.append("    GeoJSON payload attached below.")

        text_reply = "\n".join(lines)
        return text_reply, img_bytes, geojson_str

    # ── unknown command ────────────────────────────────────────────────────
    return f"Unknown command: '{command}'\n\n{HELP_TEXT}", None, None


# ── LXMF gateway ─────────────────────────────────────────────────────────────

class LxmfGateway:
    """
    Registers an LXMF delivery identity, announces on Reticulum, and
    handles incoming messages by replying with sensor data.
    Replies include: text summary, optional PNG map image, optional GeoJSON.
    """

    def __init__(
        self,
        cfg: ReticulumBridgeConfig,
        cache: SensorCache,
        navamesh_cfg: Any,
    ):
        self._cfg         = cfg
        self._cache       = cache
        self._navamesh_cfg = navamesh_cfg
        self._router: Optional[LXMF.LXMRouter] = None
        self._source: Optional[Any] = None
        self._lock = threading.Lock()

    def start(self) -> None:
        os.makedirs(self._cfg.lxmf_storage_dir, exist_ok=True)

        # Load or generate persistent identity
        identity_path = os.path.join(self._cfg.lxmf_storage_dir, "identity")
        if os.path.exists(identity_path):
            identity = RNS.Identity.from_file(identity_path)
            logger.info("Loaded RNS identity from %s", identity_path)
        else:
            identity = RNS.Identity()
            identity.to_file(identity_path)
            logger.info("Generated new RNS identity → %s", identity_path)

        # Start Reticulum stack
        RNS.Reticulum(self._cfg.rns_config_dir)
        logger.info("Reticulum started (config: %s)", self._cfg.rns_config_dir)

        # Create LXMF router and register delivery identity
        self._router = LXMF.LXMRouter(
            storagepath=self._cfg.lxmf_storage_dir,
            autopeer=True,
        )
        self._source = self._router.register_delivery_identity(
            identity,
            display_name=self._cfg.display_name,
        )
        self._router.register_delivery_callback(self._on_message)

        # Announce so farmer's Sideband can discover the gateway
        self._router.announce(self._source.hash)
        logger.info(
            "LXMF gateway ready.  Address: %s  Name: %s",
            RNS.prettyhexrep(self._source.hash),
            self._cfg.display_name,
        )
        logger.info("Waiting for commands from the farmer's Sideband app.")

    def _on_message(self, message: Any) -> None:
        """Called by LXMF router when a message arrives from the farmer."""
        try:
            sender = RNS.hexrep(message.source_hash, delimit=False)
            content = message.content.decode("utf-8").strip() if message.content else ""
            title   = message.title.decode("utf-8").strip() if message.title else ""
            cmd     = content or title
            logger.info("Command from %s: %r", sender, cmd)

            text_reply, img_bytes, geojson_str = handle_command(
                cmd, self._cache, self._cfg
            )

            # Always send the text reply first
            self._send_text(message, text_reply)

            # Send map image as a file attachment if available
            if img_bytes:
                self._send_file(message, img_bytes, "navamesh_map.png")

            # Send GeoJSON as a second text message if available
            if geojson_str:
                self._send_text(
                    message,
                    f"GeoJSON:\n```json\n{geojson_str}\n```",
                )

        except Exception as exc:
            logger.error("Error handling message: %s", exc, exc_info=True)

    def _send_text(self, original: Any, text: str) -> None:
        """Send a plain text LXMF reply back to the farmer."""
        with self._lock:
            try:
                reply = LXMF.LXMessage(
                    destination=RNS.Destination(
                        RNS.Identity.recall(original.source_hash),
                        RNS.Destination.OUT,
                        RNS.Destination.SINGLE,
                        "lxmf",
                        "delivery",
                    ),
                    source=self._source,
                    content=text,
                    title="Navamesh",
                    desired_method=LXMF.LXMessage.DIRECT,
                )
                self._router.handle_outbound(reply)
                logger.info(
                    "Text reply queued to %s",
                    RNS.hexrep(original.source_hash, delimit=False),
                )
            except Exception as exc:
                logger.error("Failed to send text reply: %s", exc)

    def _send_file(self, original: Any, data: bytes, filename: str) -> None:
        """Send a binary file (PNG map image) as an LXMF field attachment."""
        with self._lock:
            try:
                reply = LXMF.LXMessage(
                    destination=RNS.Destination(
                        RNS.Identity.recall(original.source_hash),
                        RNS.Destination.OUT,
                        RNS.Destination.SINGLE,
                        "lxmf",
                        "delivery",
                    ),
                    source=self._source,
                    content=f"[Map image: {filename}]",
                    title="Navamesh Map",
                    desired_method=LXMF.LXMessage.DIRECT,
                    fields={
                        LXMF.FIELD_IMAGE: [
                            "image/png",
                            data,
                        ]
                    },
                )
                self._router.handle_outbound(reply)
                logger.info(
                    "Map image (%d bytes) queued to %s",
                    len(data),
                    RNS.hexrep(original.source_hash, delimit=False),
                )
            except Exception as exc:
                logger.error("Failed to send map image: %s", exc)

    def announce(self) -> None:
        if self._router and self._source:
            self._router.announce(self._source.hash)
            logger.debug("Reticulum announce sent.")

    def stop(self) -> None:
        pass  # RNS GC handles cleanup


# ── MQTT subscriber ───────────────────────────────────────────────────────────

class MqttCacheUpdater:
    """
    Subscribes to all clean Navamesh MQTT topics and keeps the SensorCache
    up to date. Mirrors topic classification from mqtt_to_db.py.
    """

    def __init__(self, cfg: Any, cache: SensorCache, ignored_nodes: set):
        self._cfg     = cfg
        self._cache   = cache
        self._ignored = ignored_nodes

        try:
            self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        except AttributeError:
            self._client = mqtt.Client()

        self._client.on_connect    = self._on_connect
        self._client.on_message    = self._on_message
        self._client.on_disconnect = self._on_disconnect

        self._topics = {
            "soil_raw":     f"{cfg.root_sensors}/soil/+/raw",
            "soil_percent": f"{cfg.root_sensors}/soil/+/percent",
            "position":     f"{cfg.root_nodes}/+/position",
            "battery":      f"{cfg.root_nodes}/+/battery",
            "link":         f"{cfg.root_nodes}/+/link",
        }

    def start(self) -> None:
        self._client.connect(self._cfg.mqtt_host, self._cfg.mqtt_port, 60)
        self._client.loop_start()
        logger.info(
            "MQTT cache updater connecting to %s:%s",
            self._cfg.mqtt_host, self._cfg.mqtt_port,
        )

    def stop(self) -> None:
        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception:
            pass

    def _on_connect(self, client, userdata, flags, rc, properties=None) -> None:
        if rc != 0:
            logger.error("MQTT connect failed rc=%s", rc)
            return
        logger.info("MQTT connected — subscribing to sensor topics")
        for name, topic in self._topics.items():
            client.subscribe(topic)
            logger.info("  %s → %s", name, topic)

    def _on_disconnect(self, client, userdata, rc, properties=None) -> None:
        if rc != 0:
            logger.warning("MQTT unexpected disconnect rc=%s", rc)

    def _on_message(self, client, userdata, msg: mqtt.MQTTMessage) -> None:
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except Exception as exc:
            logger.error("JSON decode error on %s: %s", msg.topic, exc)
            return

        kind, node_id = self._classify(msg.topic)
        if kind is None or node_id is None:
            return
        if node_id in self._ignored:
            logger.debug("Ignoring node %s (IGNORED_NODES)", node_id)
            return

        ts = payload.get("ts")
        if kind == "soil_raw":
            self._cache.update(node_id, ts=ts, soil_raw=payload.get("value"))
        elif kind == "soil_percent":
            self._cache.update(node_id, ts=ts, soil_percent=payload.get("value"))
        elif kind == "battery":
            self._cache.update(
                node_id, ts=ts,
                battery_level=payload.get("batteryLevel"),
                battery_usb=payload.get("batteryUsb"),
                voltage=payload.get("voltage"),
                uptime_seconds=payload.get("uptimeSeconds"),
            )
        elif kind == "position":
            self._cache.update(
                node_id, ts=ts,
                lat=payload.get("lat"),
                lon=payload.get("lon"),
                alt=payload.get("alt"),
            )
        elif kind == "link":
            self._cache.update(
                node_id, ts=ts,
                rx_rssi=payload.get("rxRssi"),
                rx_snr=payload.get("rxSnr"),
            )

    def _classify(self, topic: str) -> Tuple[Optional[str], Optional[str]]:
        soil_pfx = f"{self._cfg.root_sensors}/soil/"
        node_pfx = f"{self._cfg.root_nodes}/"

        if topic.startswith(soil_pfx):
            parts = topic[len(soil_pfx):].split("/")
            if len(parts) != 2:
                return None, None
            node_id, metric = parts
            return ("soil_raw" if metric == "raw" else "soil_percent"), node_id

        if topic.startswith(node_pfx):
            parts = topic[len(node_pfx):].split("/")
            if len(parts) != 2:
                return None, None
            node_id, metric = parts
            if metric in {"position", "battery", "link"}:
                return metric, node_id

        return None, None


# ── Main bridge ───────────────────────────────────────────────────────────────

class ReticulumBridge:
    def __init__(self) -> None:
        self.cfg      = load_config()
        self.rns_cfg  = load_rns_config()
        self.ignored_nodes = set(
            filter(None, os.getenv("IGNORED_NODES", "").split(","))
        )
        self._stop_event = threading.Event()

        self._cache   = SensorCache()
        self._gateway = LxmfGateway(self.rns_cfg, self._cache, self.cfg)
        self._mqtt    = MqttCacheUpdater(self.cfg, self._cache, self.ignored_nodes)

    def start(self) -> None:
        logger.info("Starting Reticulum LXMF gateway...")
        self._gateway.start()

        logger.info("Starting MQTT cache updater...")
        self._mqtt.start()

        t = threading.Thread(target=self._announce_loop, daemon=True)
        t.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._mqtt.stop()
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
    logger.info(
        "Navamesh Reticulum bridge running. "
        "Farmer can send 'help' from Sideband to get started. "
        "Press Ctrl+C to stop."
    )

    try:
        while not bridge._stop_event.is_set():
            bridge._stop_event.wait(1.0)
    finally:
        bridge.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
