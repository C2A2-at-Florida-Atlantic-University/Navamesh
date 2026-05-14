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
    )


# ── Sensor cache ──────────────────────────────────────────────────────────────

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


class SensorCache:
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
    if not nodes: return "No soil data received yet."
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
    if not nodes: return "No battery data received yet."
    lines = [_header("🔋 Battery")]
    for node_id, snap in sorted(nodes.items()):
        bat = "USB (charging)" if snap.battery_usb else (
            f"{snap.battery_level:.0f}%" if snap.battery_level is not None else "no data"
        )
        volt = f"  {snap.voltage:.2f}V" if snap.voltage is not None else ""
        up   = f"  up {_fmt_uptime(snap.uptime_seconds)}" if snap.uptime_seconds else ""
        lines.append(f"{_fmt_node(node_id)}: {bat}{volt}{up}  ({_fmt_ts(snap.ts)})")
    return "\n".join(lines)

def fmt_position(cache: SensorCache) -> str:
    nodes = cache.all_nodes()
    if not nodes: return "No position data received yet."
    lines = [_header("📍 Position")]
    for node_id, snap in sorted(nodes.items()):
        if snap.lat is not None:
            alt = f"  alt={snap.alt}m" if snap.alt is not None else ""
            lines.append(f"{_fmt_node(node_id)}: {snap.lat:.6f}, {snap.lon:.6f}{alt}  ({_fmt_ts(snap.ts)})")
        else:
            lines.append(f"{_fmt_node(node_id)}: no GPS fix yet")
    return "\n".join(lines)

def fmt_link(cache: SensorCache) -> str:
    nodes = cache.all_nodes()
    if not nodes: return "No link data received yet."
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


def render_map(nodes: Dict[str, NodeSnapshot], cfg: ReticulumBridgeConfig) -> Optional[bytes]:
    """
    Render a JPEG map image sized to Sideband's lora quality preset.

    Strategy (mirrors Sideband's view.py example plugin):
      1. Render at 2x target size so staticmap draws legible pins
      2. Call image.thumbnail((max_dim, max_dim)) to scale down
      3. Save as JPEG at cfg.map_jpeg_quality

    Default: 160px / quality 18 → ~2-3 KB  (Sideband lora preset)
    """
    if not MAP_AVAILABLE:
        return None

    geo_nodes = {nid: s for nid, s in nodes.items() if s.lat is not None and s.lon is not None}
    if not geo_nodes:
        return None

    tile_url = _resolve_tile_url(cfg)
    if not tile_url:
        return None

    render_size = max(cfg.map_max_dimension * 2, 320)
    smap = StaticMap(render_size, render_size, url_template=tile_url)
    for node_id, snap in geo_nodes.items():
        smap.add_marker(CircleMarker((snap.lon, snap.lat), _pin_color(snap, cfg), 12))

    image = smap.render()
    draw  = ImageDraw.Draw(image)
    try:
        font = ImageFont.load_default()
    except Exception:
        font = None

    center_lon = sum(s.lon for s in geo_nodes.values()) / len(geo_nodes)
    center_lat = sum(s.lat for s in geo_nodes.values()) / len(geo_nodes)
    zoom = getattr(smap, '_zoom', 17)

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
        lx, ly = px + 10, py - 20
        bbox = draw.textbbox((lx, ly), label, font=font)
        draw.rectangle([bbox[0]-2, bbox[1]-2, bbox[2]+2, bbox[3]+2], fill=(0, 0, 0, 180))
        draw.text((lx, ly), label, fill="white", font=font)

    # Scale down — exactly as Sideband's view.py does it
    if image.mode in ("RGBA", "P"):
        image = image.convert("RGB")
    image.thumbnail((cfg.map_max_dimension, cfg.map_max_dimension))

    buf = io.BytesIO()
    image.save(buf, format="JPEG", quality=cfg.map_jpeg_quality, optimize=True)
    logger.info(
        "Map JPEG: %d bytes  %dx%d px  quality=%d  nodes=%d",
        buf.tell(), image.width, image.height, cfg.map_jpeg_quality, len(geo_nodes),
    )
    return buf.getvalue()


# ── Command handler ───────────────────────────────────────────────────────────

def handle_command(
    cmd: str,
    cache: SensorCache,
    cfg: ReticulumBridgeConfig,
) -> Tuple[str, Optional[bytes]]:
    parts   = cmd.strip().lower().split(None, 1)
    command = parts[0] if parts else ""
    target  = parts[1].strip() if len(parts) > 1 else None

    if command == "nodes":
        all_nodes = cache.all_nodes()
        if not all_nodes: return "No nodes seen yet.", None
        return "Known field nodes:\n" + "\n".join(f"  {n}" for n in sorted(all_nodes)), None
    if command == "help":     return HELP_TEXT, None
    if command == "status":   return fmt_status(cache), None
    if command == "soil":     return fmt_soil(cache), None
    if command == "battery":  return fmt_battery(cache), None
    if command == "position": return fmt_position(cache), None
    if command == "link":     return fmt_link(cache), None

    if command == "map":
        all_nodes = cache.all_nodes()
        if not all_nodes:
            return "No sensor data yet — field nodes may not have reported in.", None

        if target:
            if target not in all_nodes:
                return f"Node '{target}' not found. Send 'nodes' to list all known nodes.", None
            nodes = {target: all_nodes[target]}
        else:
            nodes = all_nodes

        geo_count  = sum(1 for s in nodes.values() if s.lat is not None)
        no_gps     = len(nodes) - geo_count

        lines = [_header(f"🗺️  Navamesh Map  ({len(nodes)} node(s))")]
        for node_id, snap in sorted(nodes.items()):
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

        img_bytes = render_map(nodes, cfg)
        if not img_bytes:
            if not MAP_AVAILABLE:
                lines.append("ℹ️  Map unavailable — install staticmap+pillow on the Pi.")
            elif geo_count == 0:
                lines.append("ℹ️  Map unavailable — no nodes have GPS coordinates yet.")
            else:
                lines.append("ℹ️  Map unavailable — tile server unreachable.")

        return "\n".join(lines), img_bytes

    return f"Unknown command: '{command}'\n\n{HELP_TEXT}", None


# ── LXMF gateway ─────────────────────────────────────────────────────────────

class LxmfGateway:
    def __init__(self, cfg: ReticulumBridgeConfig, cache: SensorCache, navamesh_cfg: Any):
        self._cfg          = cfg
        self._cache        = cache
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

            text_reply, img_bytes = handle_command(cmd, self._cache, self._cfg)

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
                    fields={LXMF.FIELD_IMAGE: ["image/jpeg", img_bytes]},
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


# ── MQTT subscriber ───────────────────────────────────────────────────────────

class MqttCacheUpdater:
    def __init__(self, cfg: Any, cache: SensorCache, ignored_nodes: set):
        self._cfg, self._cache, self._ignored = cfg, cache, ignored_nodes
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

    def stop(self) -> None:
        try: self._client.loop_stop(); self._client.disconnect()
        except Exception: pass

    def _on_connect(self, client, userdata, flags, rc, properties=None) -> None:
        if rc != 0: logger.error("MQTT connect failed rc=%s", rc); return
        logger.info("MQTT connected")
        for name, topic in self._topics.items():
            client.subscribe(topic)
            logger.info("  %s → %s", name, topic)

    def _on_disconnect(self, client, userdata, rc, properties=None) -> None:
        if rc != 0: logger.warning("MQTT unexpected disconnect rc=%s", rc)

    def _on_message(self, client, userdata, msg: mqtt.MQTTMessage) -> None:
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except Exception as exc:
            logger.error("JSON decode error on %s: %s", msg.topic, exc); return

        kind, node_id = self._classify(msg.topic)
        if not kind or not node_id or node_id in self._ignored:
            return

        ts = payload.get("ts")
        if kind == "soil_raw":
            self._cache.update(node_id, ts=ts, soil_raw=payload.get("value"))
        elif kind == "soil_percent":
            self._cache.update(node_id, ts=ts, soil_percent=payload.get("value"))
        elif kind == "battery":
            self._cache.update(node_id, ts=ts,
                battery_level=payload.get("batteryLevel"),
                battery_usb=payload.get("batteryUsb"),
                voltage=payload.get("voltage"),
                uptime_seconds=payload.get("uptimeSeconds"),
            )
        elif kind == "position":
            self._cache.update(node_id, ts=ts,
                lat=payload.get("lat"), lon=payload.get("lon"), alt=payload.get("alt"),
            )
        elif kind == "link":
            self._cache.update(node_id, ts=ts,
                rx_rssi=payload.get("rxRssi"), rx_snr=payload.get("rxSnr"),
            )

    def _classify(self, topic: str) -> Tuple[Optional[str], Optional[str]]:
        soil_pfx = f"{self._cfg.root_sensors}/soil/"
        node_pfx = f"{self._cfg.root_nodes}/"
        if topic.startswith(soil_pfx):
            parts = topic[len(soil_pfx):].split("/")
            if len(parts) != 2: return None, None
            node_id, metric = parts
            return ("soil_raw" if metric == "raw" else "soil_percent"), node_id
        if topic.startswith(node_pfx):
            parts = topic[len(node_pfx):].split("/")
            if len(parts) != 2: return None, None
            node_id, metric = parts
            if metric in {"position", "battery", "link"}: return metric, node_id
        return None, None


# ── Main bridge ───────────────────────────────────────────────────────────────

class ReticulumBridge:
    def __init__(self) -> None:
        self.cfg           = load_config()
        self.rns_cfg       = load_rns_config()
        self.ignored_nodes = set(filter(None, os.getenv("IGNORED_NODES", "").split(",")))
        self._stop_event   = threading.Event()
        self._cache        = SensorCache()
        self._gateway      = LxmfGateway(self.rns_cfg, self._cache, self.cfg)
        self._mqtt         = MqttCacheUpdater(self.cfg, self._cache, self.ignored_nodes)

    def start(self) -> None:
        logger.info("Starting Reticulum LXMF gateway...")
        self._gateway.start()
        logger.info("Starting MQTT cache updater...")
        self._mqtt.start()
        threading.Thread(target=self._announce_loop, daemon=True).start()

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
    logger.info("Navamesh Reticulum bridge running. Send 'help' from Sideband to start.")

    try:
        while not bridge._stop_event.is_set():
            bridge._stop_event.wait(1.0)
    finally:
        bridge.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
