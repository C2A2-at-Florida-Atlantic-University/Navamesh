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
  NODE_LABEL_ALIASES      — manual node labels used when no Meshtastic name has
                            been received, e.g. "!abc12345=Node A,!def67890=Node B"

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
from navamesh.calibration import DAMP, DRY, WET, adc_to_band
from navamesh.processors.command_proto import UNICAST_ONLY_VERBS

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
    # Offline tile-cache bounds (shared with cache_tiles.py). When all four
    # lat/lon bounds are set, the map renderer constrains itself to the cached
    # farm extent instead of recentering on individual nodes. Optional — None
    # when unset, in which case the legacy node-centered behavior is preserved.
    cache_lat_min: Optional[float]
    cache_lat_max: Optional[float]
    cache_lon_min: Optional[float]
    cache_lon_max: Optional[float]
    cache_zoom_min: Optional[int]
    cache_zoom_max: Optional[int]
    pg_dsn: str
    # RNS/LXMF identity hashes permitted to issue control commands (WRITE_VERBS).
    # EMPTY MEANS EVERYONE, which is the current intent for testing and first deployment:
    # any device that can reach the gateway may command the mesh. Populate it to restrict.
    # See is_sender_authorized() and TODO.md.
    authorized_farmer_hashes: Tuple[str, ...] = ()
    # How long to wait for a node's ack before reporting a timeout to the operator.
    cmd_ack_timeout_seconds: int = 120


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

    def _opt_float(name: str) -> Optional[float]:
        v = os.getenv(name)
        if v in (None, ""):
            return None
        try:
            return float(v)
        except ValueError:
            return None

    def _opt_int(name: str) -> Optional[int]:
        v = os.getenv(name)
        if v in (None, ""):
            return None
        try:
            return int(v)
        except ValueError:
            return None

    def _hash_list(name: str) -> Tuple[str, ...]:
        """
        Parse a comma-separated list of RNS identity hashes.

        Normalised to lowercase with any ':' separators stripped, so an operator can paste
        a hash in either the delimited or bare form that RNS prints.
        """
        v = os.getenv(name, "")
        parsed = tuple(
            h.strip().lower().replace(":", "")
            for h in v.split(",")
            if h.strip()
        )
        if not parsed:
            logger.warning(
                "%s is unset — control commands are OPEN to any sender that can reach this "
                "gateway. Fine for testing; set it before this is a trusted deployment.", name
            )
        return parsed

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
        cache_lat_min=_opt_float("CACHE_LAT_MIN"),
        cache_lat_max=_opt_float("CACHE_LAT_MAX"),
        cache_lon_min=_opt_float("CACHE_LON_MIN"),
        cache_lon_max=_opt_float("CACHE_LON_MAX"),
        cache_zoom_min=_opt_int("CACHE_ZOOM_MIN"),
        cache_zoom_max=_opt_int("CACHE_ZOOM_MAX"),
        pg_dsn=os.getenv("PG_DSN", ""),
        authorized_farmer_hashes=_hash_list("AUTHORIZED_FARMER_HASHES"),
        cmd_ack_timeout_seconds=_int("CMD_ACK_TIMEOUT_SECONDS", 120),
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
    # Meshtastic NODEINFO_APP owner names, when the Pi has received them
    display_name: Optional[str] = None
    long_name: Optional[str] = None
    short_name: Optional[str] = None


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
            # soil_raw is the authoritative reading -- the DRY/DAMP/WET band is
            # derived from it. Without this the formatter silently falls back to
            # the legacy (node-computed, uncalibrated) percentage.
            soil_raw=meta.get("soil_raw"),
            soil_percent=meta.get("soil_percent"),
            battery_level=meta.get("battery_level"),
            battery_usb=meta.get("battery_usb"),
            voltage=meta.get("voltage"),
            uptime_seconds=meta.get("uptime_seconds"),
            lat=lat,
            lon=lon,
            rx_rssi=meta.get("rx_rssi"),
            rx_snr=meta.get("rx_snr"),
            display_name=meta.get("display_name"),
            long_name=meta.get("long_name"),
            short_name=meta.get("short_name"),
        )
    logger.debug("Loaded %d node(s) from Postgres.", len(snapshots))
    return snapshots


# ── Formatters ────────────────────────────────────────────────────────────────

def _fmt_node(node_id: str) -> str:
    return f"Node {node_id[-4:]}" if node_id.startswith("!") else node_id


def _node_aliases() -> Dict[str, str]:
    """Optional manual labels from the environment, e.g.
    NODE_LABEL_ALIASES="!abc12345=Node A,!def67890=Node B"."""
    aliases: Dict[str, str] = {}
    for pair in os.getenv("NODE_LABEL_ALIASES", "").split(","):
        if "=" not in pair:
            continue
        nid, name = pair.split("=", 1)
        nid, name = nid.strip(), name.strip()
        if nid and name:
            aliases[nid] = name
    return aliases


def _label_candidates(node_id: str, snap: Optional[NodeSnapshot], order: Tuple[str, ...]):
    for field in order:
        cand = getattr(snap, field, None) if snap is not None else None
        if isinstance(cand, str) and cand.strip():
            yield cand.strip()
    alias = _node_aliases().get(node_id)
    if alias:
        yield alias


def _node_label(node_id: str, snap: Optional[NodeSnapshot] = None) -> str:
    """Human label for text replies: Meshtastic display/short/long name, then a
    configured alias, then the legacy 'Node <last4>' fallback."""
    for cand in _label_candidates(
        node_id, snap, ("display_name", "short_name", "long_name")
    ):
        return cand
    return _fmt_node(node_id)


# Image labels stay compact so they don't crowd the map or spill off the frame.
_MAP_LABEL_MAX = 12


def _map_pin_label(node_id: str, snap: Optional[NodeSnapshot] = None) -> str:
    """Short label for map-image pins: prefers short_name, truncates anything
    longer than _MAP_LABEL_MAX (image only — stored values are untouched), and
    falls back to the legacy last-4-chars of the node ID."""
    for cand in _label_candidates(
        node_id, snap, ("short_name", "display_name", "long_name")
    ):
        return cand[:_MAP_LABEL_MAX].rstrip()
    return node_id[-4:]

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

def _fmt_soil_reading(snap) -> Optional[str]:
    """Render a soil reading as a DRY/DAMP/WET band, or None if there is no data.

    The band comes from the RAW ADC via calibration.adc_to_band(), which is the
    single source of truth -- do not re-derive thresholds anywhere else.

    No percentage is shown at all, anywhere. It used to appear inside the DAMP band,
    where adc_to_percent() can resolve one -- but a figure present on some readings and
    absent on others reads as the precise answer with the rest as approximations, when
    the band is the part this probe actually supports. Outside DAMP the probe is pinned
    to a rail and has no resolution, so a percentage there was invented precision. The
    raw count is kept in parentheses for diagnostics; it is plainly not a moisture figure.

    snap.soil_percent is deliberately NOT trusted when a raw count exists: the
    DB still holds percentages parsed from legacy firmware status strings (see
    processors/soil_text.py, which states they are not authoritative), and those
    disagree with the probe -- a node at raw_adc=4095 (bone dry) was reporting
    "10.0%". It is used only as a last resort for nodes that never send raw.
    """
    if snap.soil_raw is not None:
        raw = float(snap.soil_raw)
        # Band only, no percentage -- not even inside DAMP, where one is derivable. A single
        # figure shown on some readings and not others reads as the precise answer and the
        # rest as approximations, when in fact the band is the trustworthy part on this
        # probe. The raw count stays for diagnostics; it is plainly not a moisture figure.
        return f"{adc_to_band(raw)} (ADC {raw:.0f})"
    if snap.soil_percent is not None:
        # Legacy rows: a percentage parsed from the old firmware's status strings, which
        # soil_text.py itself marks as not authoritative -- a bone-dry node at raw 4095 was
        # reporting "10.0%". Map it to a band word so the farmer never sees two vocabularies,
        # and mark it uncalibrated rather than implying it is comparable to a real reading.
        return f"{percent_to_band(snap.soil_percent)} (uncalibrated)"
    return None


def percent_to_band(pct) -> str:
    """A legacy percentage as one of the three band words.

    Thresholds mirror generate_map.py's MOISTURE_* defaults so the map and the text
    replies cannot disagree about the same node.
    """
    try:
        v = float(pct)
    except (TypeError, ValueError):
        return "UNKNOWN"
    if v < 30:
        return DRY
    if v < 70:
        return DAMP
    return WET


def _header(title: str) -> str:
    return f"{'─'*30}\n{title}\n{'─'*30}\n"

def fmt_status(nodes: Dict[str, NodeSnapshot]) -> str:
    if not nodes:
        return "No node data in database yet. Are field nodes transmitting?"
    lines = [_header("🌱 Navamesh Status")]
    for node_id, snap in sorted(nodes.items()):
        lines.append(f"[ {_fmt_node(node_id)} ]  {node_id}")
        lines.append(f"  Last seen:  {_fmt_ts(snap.ts)}")
        soil = _fmt_soil_reading(snap)
        if soil is not None:
            lines.append(f"  Soil:       {soil}")
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
        soil = _fmt_soil_reading(snap)
        if soil is None:
            lines.append(f"{_fmt_node(node_id)}: no soil data yet")
        else:
            lines.append(f"{_fmt_node(node_id)}: {soil}  ({_fmt_ts(snap.ts)})")
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

# The control half of this used to describe itself the way the protocol does --
# "set telemetry interval (live, no reboot)", "stop / resume transmitting" -- which
# asked a farmer to know what telemetry is before they could find out how often
# their sensor reports. The app's buttons were rewritten in the farmer's words in
# cd60737 and this was missed, so the one screen a farmer opens *because* they do
# not already know what the commands do was the one still written for us.
#
# Each control entry now leads with the same name the app's button carries (see
# VERB_LABELS, and command_registry.py in the app repo) and says what happens to
# the sensor. The wire syntax stays underneath: the farmer never types it, but the
# operator drives this same gateway by typing, and dropping it would take away the
# only place it is written down. test_farmer_wording.py pins the labels to
# VERB_LABELS so the two cannot drift again.
HELP_TEXT = """🌱 Navamesh Gateway — Commands

  status       — how every sensor is doing
  soil         — how wet the soil is
  battery      — battery level and how long each sensor has been up
  position     — where each sensor is
  link         — how strong each sensor's signal is
  map          — a map picture of every sensor
  map <id>     — a map picture of one sensor
  nodes        — list every sensor the gateway knows
  help         — this message

Change a sensor — the app asks you to confirm before any of these are sent:

  Bluetooth on — turns Bluetooth on for a while, then off again by itself, so
      you can connect to the sensor while the window is open
      ble <id|^all> <minutes>
  Reporting interval — how often the sensor reports. Shorter gives finer data
      and uses more battery. Takes effect right away
      interval <id|^all> <seconds>
  Messaging pause — the sensor stops sending but keeps listening, so you can
      resume it whenever. It also resumes by itself within 3 days, or on a reboot
      quiet <id|^all> on|off
  Sensor location — the sensor has no GPS of its own, so this tells it where it
      stands. Stand next to it before sending. One sensor at a time
      setloc <id> <lat> <lon>"""


# Verbs that change deployed field hardware. Gated by AUTHORIZED_FARMER_HASHES; every
# other verb in this gateway is read-only and safe to leave open.
WRITE_VERBS = ("ble", "interval", "quiet", "setloc")

# Which of those may never go to ^all -- see UNICAST_ONLY_VERBS in command_proto, which is
# where it lives because the transmit path enforces the same rule independently.

# How often to check for command acks to report. Tight, because an operator standing in a
# field waiting on a Bluetooth window is actively watching for this.
COMMAND_OUTCOME_POLL_SECONDS = 5


def is_lxmf_destination(requested_by: str) -> bool:
    """
    True if `requested_by` looks like an RNS identity hash we could reply to.

    Commands do not only come from Sideband: navamesh-cmd stamps requested_by with its
    own name, and the bridge uses "bridge". Those requesters get their answer through
    MQTT and have no LXMF address, so trying to build a destination for them raises
    ValueError from bytes.fromhex and -- because the row then never gets marked
    notified -- retries forever, once per poll.
    """
    if not requested_by:
        return False
    h = requested_by.strip().lower().replace(":", "")
    return len(h) == 32 and all(c in "0123456789abcdef" for c in h)


def is_sender_authorized(sender_hash: str, allowed_hashes) -> bool:
    """
    May this sender issue control commands?

    An EMPTY allow-list means everyone may. That is a deliberate choice for testing and
    first deployment: any device that can reach this gateway over Reticulum can command
    the mesh, and the only thing in the way is that the verb list lives in the wrapper app.

    That is obscurity rather than access control, and note the `help` verb prints the
    control commands to anyone who asks for them. Populating AUTHORIZED_FARMER_HASHES
    turns this into a real check with no code change -- see TODO.md.

    Kept as a module-level pure function so the policy is testable without standing up RNS
    or a gateway instance.
    """
    if not allowed_hashes:
        return True
    return sender_hash.strip().lower().replace(":", "") in allowed_hashes

# Bounds duplicated from processors.command_proto so a bad value is rejected here, with a
# readable message, instead of travelling all the way to a node that would silently clamp
# it. command_proto re-validates before transmit; this is the operator-facing copy.
_BLE_MIN_MINUTES, _BLE_MAX_MINUTES = 1, 240
_INTERVAL_MIN_SECONDS, _INTERVAL_MAX_SECONDS = 60, 86400
_QUIET_MIN_MINUTES, _QUIET_MAX_MINUTES = 1, 4320
_LAT_MIN_DEGREES, _LAT_MAX_DEGREES = -90.0, 90.0
_LON_MIN_DEGREES, _LON_MAX_DEGREES = -180.0, 180.0

BROADCAST_TARGET = "^all"

_WRITE_EXAMPLE_ARGS = {
    "ble": "15",
    "interval": "1800",
    "quiet": "on",
    "setloc": "36.0721 -109.0450",
}


def _parse_write_args(command: str, target: Optional[str]):
    """
    Parse "<id|^all> <value>" ("<id|^all> on|off" for quiet, "<id> <lat> <lon>" for setloc).

    Returns (node, value, quiet_on, coords, error_message). `error_message` is non-None on
    any problem, in which case the other fields are meaningless. `coords` is a (lat, lon)
    pair of floats for setloc and None for every other verb.
    """
    if not target:
        example = _WRITE_EXAMPLE_ARGS.get(command, "15")
        who = "!a1b2c3d4" if command in UNICAST_ONLY_VERBS else BROADCAST_TARGET
        return None, None, None, None, (
            f"'{command}' needs a target. Example: {command} {who} {example}"
        )

    bits = target.split()
    node = bits[0]
    arg = bits[1] if len(bits) > 1 else None

    if command == "quiet":
        if arg not in ("on", "off"):
            return None, None, None, None, "quiet needs 'on' or 'off'. Example: quiet ^all on"
        return node, None, arg == "on", None, None

    if command == "setloc":
        if len(bits) < 3:
            return None, None, None, None, (
                f"setloc needs a latitude and a longitude. "
                f"Example: setloc {node} {_WRITE_EXAMPLE_ARGS['setloc']}"
            )
        try:
            lat, lon = float(bits[1]), float(bits[2])
        except ValueError:
            return None, None, None, None, (
                f"'{bits[1]} {bits[2]}' is not a latitude and longitude in decimal degrees."
            )
        if not _LAT_MIN_DEGREES <= lat <= _LAT_MAX_DEGREES:
            return None, None, None, None, (
                f"Latitude must be {_LAT_MIN_DEGREES} to {_LAT_MAX_DEGREES}, got {lat}."
            )
        if not _LON_MIN_DEGREES <= lon <= _LON_MAX_DEGREES:
            return None, None, None, None, (
                f"Longitude must be {_LON_MIN_DEGREES} to {_LON_MAX_DEGREES}, got {lon}."
            )
        return node, None, None, (lat, lon), None

    if arg is None:
        unit = "minutes" if command == "ble" else "seconds"
        return None, None, None, None, f"'{command}' needs a value in {unit}. Example: {command} {node} 15"

    try:
        value = int(arg)
    except ValueError:
        return None, None, None, None, f"'{arg}' is not a number."

    if command == "ble" and not _BLE_MIN_MINUTES <= value <= _BLE_MAX_MINUTES:
        return None, None, None, None, (
            f"BLE window must be {_BLE_MIN_MINUTES}-{_BLE_MAX_MINUTES} minutes."
        )
    if command == "interval" and not _INTERVAL_MIN_SECONDS <= value <= _INTERVAL_MAX_SECONDS:
        return None, None, None, None, (
            f"Interval must be {_INTERVAL_MIN_SECONDS}-{_INTERVAL_MAX_SECONDS} seconds "
            f"({_INTERVAL_MIN_SECONDS // 60} min to 24 h)."
        )

    return node, value, None, None, None


def _handle_write_command(
    command: str,
    target: Optional[str],
    nodes: Dict[str, NodeSnapshot],
    *,
    dispatch_write=None,
    source_hash: Optional[str] = None,
    authorized: bool = False,
) -> str:
    """Validate a control command and hand it to the transmit path."""
    if not authorized:
        # Deliberately terse. This gateway answers any LXMF sender that discovers it via
        # RNS announce, so an unauthorized peer should learn nothing about what exists.
        return "Unauthorized: this gateway does not accept control commands from you."

    if dispatch_write is None:
        return "Control commands are not available on this gateway (no command bus configured)."

    node, value, quiet_on, coords, error = _parse_write_args(command, target)
    if error:
        return f"⚠️  {error}"

    is_broadcast = node in (BROADCAST_TARGET, "all")
    if is_broadcast and command in UNICAST_ONLY_VERBS:
        return (f"⚠️  '{command}' must name one node. Sending it to {BROADCAST_TARGET} would "
                f"give every node the same position.")
    if not is_broadcast and node not in nodes:
        return f"Node '{node}' not found. Send 'nodes' to list all known nodes."

    resolved = BROADCAST_TARGET if is_broadcast else node
    lat, lon = coords if coords else (None, None)
    try:
        cmd_id = dispatch_write(
            verb=command,
            target=resolved,
            value=value,
            quiet_on=quiet_on,
            lat=lat,
            lon=lon,
            requested_by=source_hash or "unknown",
        )
    except Exception as exc:
        # Most likely the MQTT broker is down. Fail loudly: silently swallowing this
        # would leave the operator believing a node had been reconfigured.
        return f"⚠️  Could not queue command: {exc}"

    who = "ALL sensors" if is_broadcast else _node_label(node, nodes.get(node))
    # Farmer-facing wording: name the request in the same words the app's button uses,
    # and say what is being waited for. "Queued: telemetry interval 300 s" described the
    # protocol rather than the action, and "quiet mode" appears nowhere in the UI.
    if command == "ble":
        what = f"Bluetooth on for {value} min"
    elif command == "interval":
        what = f"Reporting interval every {_friendly_seconds(value)}"
    elif command == "setloc":
        what = f"Sensor location {lat:.6f}, {lon:.6f}"
    else:
        what = "Pause messaging" if quiet_on else "Resume messaging"

    return (f"📤 {what} — request sent to {who}\n"
            f"Waiting for the sensor to confirm… (request {cmd_id})")


# ── Map renderer ──────────────────────────────────────────────────────────────

# Farmer-facing names for the wire verbs. The verb is what the protocol carries; these are
# what the app's buttons say, and an outcome message that reads "applied setloc" asks the
# farmer to know the protocol to understand their own sensor.
VERB_LABELS = {
    "ble": "Bluetooth on",
    "interval": "Reporting interval",
    "quiet": "Messaging pause",
    "setloc": "Sensor location",
}


def _verb_label(verb) -> str:
    return VERB_LABELS.get(str(verb), str(verb))


def _friendly_seconds(seconds) -> str:
    """Seconds as a farmer would say them: "5 minutes", "8 hours", "1 day".

    The raw number is what the protocol carries, but "telemetry interval 300 s" asked the
    reader to do the arithmetic. Falls back to the bare number for anything that does not
    divide cleanly, which is better than rounding a value they explicitly chose.
    """
    try:
        s = int(seconds)
    except (TypeError, ValueError):
        return str(seconds)
    if s % 86400 == 0 and s >= 86400:
        n = s // 86400
        return f"{n} day" if n == 1 else f"{n} days"
    if s % 3600 == 0 and s >= 3600:
        n = s // 3600
        return f"{n} hour" if n == 1 else f"{n} hours"
    if s % 60 == 0 and s >= 60:
        n = s // 60
        return f"{n} minute" if n == 1 else f"{n} minutes"
    return f"{s} seconds"


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


# ── Offline tile-cache bounds + tile-range guard ──────────────────────────────
#
# The lat/lon cache box is NOT sufficient on its own: the render viewport is a
# square of render_size px and _best_zoom() adds padding, so the actual tiles
# StaticMap requests can spill a row/column past the configured CACHE_* box. The
# real invariant is "only render if every tile the viewport needs lives inside
# the cached tile range" — the cache is the authority, node placement secondary.

def _cache_bounds(
    cfg: ReticulumBridgeConfig,
) -> Optional[Tuple[float, float, float, float]]:
    """(lat_min, lat_max, lon_min, lon_max) if all four bounds are configured,
    else None. Min/max are normalized so swapped values still work."""
    vals = (cfg.cache_lat_min, cfg.cache_lat_max, cfg.cache_lon_min, cfg.cache_lon_max)
    if any(v is None for v in vals):
        return None
    return (
        min(cfg.cache_lat_min, cfg.cache_lat_max),
        max(cfg.cache_lat_min, cfg.cache_lat_max),
        min(cfg.cache_lon_min, cfg.cache_lon_max),
        max(cfg.cache_lon_min, cfg.cache_lon_max),
    )


def _within_bounds(
    lat: Optional[float],
    lon: Optional[float],
    bounds: Optional[Tuple[float, float, float, float]],
) -> bool:
    if lat is None or lon is None or bounds is None:
        return False
    lat_min, lat_max, lon_min, lon_max = bounds
    return lat_min <= lat <= lat_max and lon_min <= lon <= lon_max


def _deg2tile(lat: float, lon: float, zoom: int) -> Tuple[float, float]:
    """Fractional (x, y) tile coordinate for a lat/lon at zoom (Web Mercator)."""
    n = 2 ** zoom
    x = (lon + 180.0) / 360.0 * n
    lat_r = math.radians(lat)
    y = (1.0 - math.asinh(math.tan(lat_r)) / math.pi) / 2.0 * n
    return x, y


def _tile_range_for_bounds(
    bounds: Tuple[float, float, float, float],
    zoom: int,
) -> Tuple[int, int, int, int]:
    """Integer (x_min, x_max, y_min, y_max) tile range covering a lat/lon box,
    matching exactly the tiles cache_tiles.py downloads for the same CACHE_*
    bounds at this zoom. cache_tiles.py computes
        x_min, y_max = deg2tile(LAT_MIN, LON_MIN); x_max, y_min = deg2tile(LAT_MAX, LON_MAX)
    and iterates the inclusive box; we reproduce that mapping here. (Its deg2tile
    uses log(tan + 1/cos), which is identical to our asinh(tan); both floor via
    int(), which equals math.floor() for the always-positive tile indices in
    valid lat/lon range.) Note: y is inverted — larger lat → smaller y."""
    lat_min, lat_max, lon_min, lon_max = bounds
    n = 2 ** zoom
    def _clamp(v: float) -> int:
        return max(0, min(n - 1, int(math.floor(v))))
    x0, _ = _deg2tile(lat_min, lon_min, zoom)   # west edge  → x_min
    x1, _ = _deg2tile(lat_max, lon_max, zoom)   # east edge  → x_max
    _, y0 = _deg2tile(lat_max, lon_max, zoom)   # top edge (max lat) → y_min
    _, y1 = _deg2tile(lat_min, lon_min, zoom)   # bottom edge (min lat) → y_max
    return _clamp(x0), _clamp(x1), _clamp(y0), _clamp(y1)


def _tile_range_for_view(
    center_lat: float,
    center_lon: float,
    zoom: int,
    render_size: int,
    tile_size: int = 256,
) -> Tuple[int, int, int, int]:
    """Integer tile range the square render viewport will request from the tile
    server, including the half-viewport spill in every direction.

    Conservative at tile edges: floor() for the min indices, ceil()-1 for the max
    indices. A viewport that ends exactly on a tile boundary does not need the
    tile beyond that boundary, while a viewport that pokes even slightly into the
    next tile row/column counts that tile."""
    fx, fy = _deg2tile(center_lat, center_lon, zoom)
    half = (render_size / 2.0) / tile_size          # half-viewport in tiles
    n = 2 ** zoom
    # Absorb float round-trip noise (lat -> Mercator -> lat drifts ~1e-11 tiles)
    # so an edge that lands exactly on a tile boundary is treated as on it,
    # not as poking into the neighboring tile. Sub-micrometer at any zoom.
    eps = 1e-9
    def _lo(v: float) -> int:
        return max(0, min(n - 1, int(math.floor(v + eps))))
    def _hi(v: float) -> int:
        return max(0, min(n - 1, int(math.ceil(v - eps) - 1)))
    return _lo(fx - half), _hi(fx + half), _lo(fy - half), _hi(fy + half)


def _tile_range_contains(
    cache_range: Tuple[int, int, int, int],
    render_range: Tuple[int, int, int, int],
) -> bool:
    """True iff the render tile range is fully inside the cached tile range."""
    cx0, cx1, cy0, cy1 = cache_range
    rx0, rx1, ry0, ry1 = render_range
    return rx0 >= cx0 and rx1 <= cx1 and ry0 >= cy0 and ry1 <= cy1


def _cached_view_choice(
    bounds: Tuple[float, float, float, float],
    cfg: ReticulumBridgeConfig,
    desired_zoom: int,
    desired_render_size: int,
    min_render_size: int = 480,
) -> Optional[Tuple[int, int, Tuple[int, int, int, int], Tuple[int, int, int, int]]]:
    """Pick a zoom/render-size pair whose viewport is fully inside cache tiles.

    Cache bounds often describe a rectangular tile set, while StaticMap renders a
    square viewport. If the first viewport spills outside the cache, try higher
    cached zooms and, if needed, shrink the internal render size instead of
    immediately falling back to text.
    """
    lat_min, lat_max, lon_min, lon_max = bounds
    center_lon = (lon_min + lon_max) / 2
    center_lat = (lat_min + lat_max) / 2
    min_zoom = cfg.cache_zoom_min if cfg.cache_zoom_min is not None else 14
    max_zoom = cfg.cache_zoom_max if cfg.cache_zoom_max is not None else desired_zoom
    min_zoom = min(min_zoom, max_zoom)
    desired_zoom = max(min_zoom, min(desired_zoom, max_zoom))

    zooms = list(range(desired_zoom, max_zoom + 1))
    zooms.extend(range(desired_zoom - 1, min_zoom - 1, -1))

    sizes: List[int] = []
    size = desired_render_size
    while size >= min_render_size:
        sizes.append(size)
        size -= 128
    if min_render_size not in sizes:
        sizes.append(min_render_size)

    for zoom in zooms:
        cache_range = _tile_range_for_bounds(bounds, zoom)
        for render_size in sizes:
            view_range = _tile_range_for_view(center_lat, center_lon, zoom, render_size)
            if _tile_range_contains(cache_range, view_range):
                return zoom, render_size, view_range, cache_range
    return None


def _preflight_tile_urls(
    tile_url_template: str,
    tile_range: Tuple[int, int, int, int],
    zoom: int,
) -> List[Tuple[int, int, int]]:
    """Probe the local tile server for the tiles the render needs. Returns a
    list of missing/unreachable (z, x, y) tiles; short-circuits on the first
    miss so a missing tile costs one request."""
    x0, x1, y0, y1 = tile_range
    missing: List[Tuple[int, int, int]] = []
    for x in range(x0, x1 + 1):
        for y in range(y0, y1 + 1):
            url = tile_url_template.format(z=zoom, x=x, y=y)
            try:
                urllib.request.urlopen(url, timeout=2)
            except Exception:
                missing.append((zoom, x, y))
                return missing          # one miss is enough — bail early
    return missing


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


def _soil_band_short(snap) -> str:
    """DRY / DAMP / WET for a map pin, or "?" when there is no reading.

    Short form on purpose: the pin label has room for a word, not a sentence. Prefers the
    raw ADC (authoritative) and falls back to a legacy percentage mapped onto the same
    three words, so a pin never shows a figure the probe cannot support.
    """
    if getattr(snap, "soil_raw", None) is not None:
        try:
            return adc_to_band(float(snap.soil_raw))
        except (TypeError, ValueError):
            pass
    if getattr(snap, "soil_percent", None) is not None:
        return percent_to_band(snap.soil_percent)
    return "?"


def _label_values(snap: NodeSnapshot) -> Tuple[str, str]:
    """Soil/battery value strings for a map label ("?" when unknown, "USB" when
    on external power).

    Soil is a band, not a percentage: the pin is glanced at while deciding whether to
    water, and DRY/DAMP/WET is that decision. A number invited precision the probe does
    not have (it is blind below ~9.5% and saturated above 20%).
    """
    soil_str = _soil_band_short(snap)
    bat_str = "USB" if snap.battery_usb else (
        f"{snap.battery_level:.0f}%" if snap.battery_level is not None else "?"
    )
    return soil_str, bat_str


def _draw_droplet_icon(draw, x: int, y: int, size: int, color) -> None:
    """Tiny water-droplet glyph drawn with primitives — the default Pillow font
    on the Pi has no emoji coverage, so 💧 can't be rendered as text."""
    w = max(4, int(size * 0.72))
    cx = x + size / 2
    body_top = y + size * 0.30
    draw.ellipse((cx - w / 2, body_top, cx + w / 2, y + size), fill=color)
    draw.polygon(
        [
            (cx, y),
            (cx - w / 2 + 1, body_top + size * 0.20),
            (cx + w / 2 - 1, body_top + size * 0.20),
        ],
        fill=color,
    )


def _draw_battery_icon(draw, x: int, y: int, size: int, color) -> None:
    """Tiny battery glyph (solid body + terminal nub), primitives for the same
    no-emoji-font reason as the droplet."""
    body_h = max(4, int(size * 0.55))
    top = y + (size - body_h) / 2
    body_w = max(6, int(size * 0.82))
    nub_w = max(1, int(size * 0.14))
    draw.rectangle((x, top, x + body_w, top + body_h), fill=color)
    draw.rectangle(
        (x + body_w, top + body_h * 0.28, x + body_w + nub_w, top + body_h * 0.72),
        fill=color,
    )


def render_map(
    nodes: Dict[str, NodeSnapshot],
    cfg: ReticulumBridgeConfig,
    bounds: Optional[Tuple[float, float, float, float]] = None,
    highlight_node_id: Optional[str] = None,
) -> Optional[Tuple[str, bytes, str]]:
    """Render a node map to a JPEG. When ``bounds`` is given the map is locked to
    that fixed cached-farm extent (center and zoom derived from the bounds, not
    the nodes) and is rendered only if every required tile lives inside the
    offline cache — the cache is the authority, node placement secondary. When
    ``bounds`` is None the legacy node-centered behavior is preserved.

    ``highlight_node_id`` draws that node's marker larger so a requested node
    stands out. Returns None (so callers fall back to text) on any failure,
    including missing/uncached tiles."""
    if not MAP_AVAILABLE:
        return None

    geo_nodes = {nid: s for nid, s in nodes.items() if s.lat is not None and s.lon is not None}
    if not geo_nodes:
        return None

    max_dimension = cfg.map_max_dimension
    quality       = cfg.map_jpeg_quality
    max_bytes     = cfg.map_max_bytes

    render_size = max(max_dimension * 2, 480)

    if bounds is not None:
        # Fixed cached-farm extent — center/zoom come from the bounds, never the
        # nodes, so a single requested node can't recenter or over-zoom the map.
        lat_min, lat_max, lon_min, lon_max = bounds
        center_lon = (lon_min + lon_max) / 2
        center_lat = (lat_min + lat_max) / 2
        desired_zoom = _best_zoom([lat_min, lat_max], [lon_min, lon_max], render_size)

        # Tile-range containment gate: refuse to render if the square viewport
        # needs any tile outside the cached tile range (catches the padding /
        # square-aspect spill that lat/lon bounds alone don't cover). If the
        # first choice spills, try cached zooms/smaller internal render sizes
        # before falling back to text.
        choice = _cached_view_choice(bounds, cfg, desired_zoom, render_size)
        if choice is None:
            fallback_zoom = desired_zoom
            if cfg.cache_zoom_max is not None:
                fallback_zoom = min(fallback_zoom, cfg.cache_zoom_max)
            if cfg.cache_zoom_min is not None:
                fallback_zoom = max(fallback_zoom, cfg.cache_zoom_min)
            logger.warning(
                "No cached map viewport fits inside cache bounds at z%d — text fallback",
                fallback_zoom,
            )
            return None
        zoom, render_size, view_range, cache_range = choice
        if zoom != desired_zoom or render_size != max(max_dimension * 2, 480):
            logger.info(
                "Adjusted map viewport to fit cache: z%d size=%d tiles=%s cache=%s",
                zoom, render_size, view_range, cache_range,
            )

        # Use the local/offline tiles directly and preflight the real render
        # tiles: a passing zoom-14 probe never implies the chosen-zoom tiles exist.
        if cfg.map_tile_url:
            missing = _preflight_tile_urls(cfg.map_tile_url, view_range, zoom)
            if missing:
                logger.warning(
                    "Required local tile(s) missing (e.g. %s) — text fallback", missing[0]
                )
                return None
            tile_url = cfg.map_tile_url
        else:
            tile_url = _resolve_tile_url(cfg, geo_nodes)
            if not tile_url:
                return None
    else:
        # Legacy node-centered path — unchanged.
        tile_url = _resolve_tile_url(cfg, geo_nodes)
        if not tile_url:
            return None

        lons_list  = [s.lon for s in geo_nodes.values()]
        lats_list  = [s.lat for s in geo_nodes.values()]
        center_lon = (min(lons_list) + max(lons_list)) / 2
        center_lat = (min(lats_list) + max(lats_list)) / 2
        zoom       = _best_zoom(lats_list, lons_list, render_size)

    smap = StaticMap(render_size, render_size, url_template=tile_url)
    for node_id, snap in geo_nodes.items():
        radius = 26 if node_id == highlight_node_id else 18
        smap.add_marker(CircleMarker((snap.lon, snap.lat), _pin_color(snap, cfg), radius))

    try:
        if bounds is not None:
            image = smap.render(zoom=zoom, center=(center_lon, center_lat))
        else:
            image = smap.render(zoom=zoom)
    except RuntimeError as e:
        # Missing/uncached tiles raise "could not download N tiles" — never let
        # that crash the command handler; fall back to text instead.
        logger.error("Map tile render failed (%s) — falling back to text", e)
        return None
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
        soil_str, bat_str = _label_values(snap)
        id_text = _map_pin_label(node_id, snap)

        # Second line is icon+value pairs (droplet=soil, battery=charge) drawn
        # with primitives, so measure it piecewise instead of as one string.
        line_gap = 4  # Pillow's default multiline spacing, kept from the old label
        b_id   = draw.textbbox((0, 0), id_text, font=font)
        b_soil = draw.textbbox((0, 0), soil_str, font=font)
        b_bat  = draw.textbbox((0, 0), bat_str, font=font)
        id_h     = b_id[3] - b_id[1]
        val_top  = min(b_soil[1], b_bat[1])
        val_h    = max(b_soil[3], b_bat[3]) - val_top
        icon_sz  = max(8, val_h)
        icon_gap = max(2, font_size // 8)
        pair_gap = 3 * icon_gap
        line2_w = (
            icon_sz + icon_gap + (b_soil[2] - b_soil[0])
            + pair_gap
            + icon_sz + icon_gap + (b_bat[2] - b_bat[0])
        )
        lw = max(b_id[2] - b_id[0], line2_w)
        lh = id_h + line_gap + val_top + icon_sz
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
        cx_l = (chosen_box[0] + chosen_box[2]) // 2
        cy_l = (chosen_box[1] + chosen_box[3]) // 2
        # Leader line first so the boxless black text is drawn on top of it.
        draw.line([(px, py), (cx_l, cy_l)], fill=(255, 255, 255, 160), width=3)
        draw.text((chosen_lx, chosen_ly), id_text, fill="black", font=font)
        ly2 = chosen_ly + id_h + line_gap
        ix = chosen_lx
        _draw_droplet_icon(draw, ix, ly2 + val_top, icon_sz, "black")
        ix += icon_sz + icon_gap
        draw.text((ix, ly2), soil_str, fill="black", font=font)
        ix += (b_soil[2] - b_soil[0]) + pair_gap
        _draw_battery_icon(draw, ix, ly2 + val_top, icon_sz, "black")
        ix += icon_sz + icon_gap
        draw.text((ix, ly2), bat_str, fill="black", font=font)

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
    *,
    dispatch_write=None,
    source_hash: Optional[str] = None,
    authorized: bool = False,
) -> Tuple[str, Optional[Tuple[str, bytes, str]]]:
    """
    Interpret one operator command.

    The read-only verbs are pure functions of `nodes` and stay that way. The write verbs
    (see WRITE_VERBS) need to reach the mesh, which this process cannot do -- the bridge
    process owns the serial port -- so they call `dispatch_write` instead.

    The write-path parameters are keyword-only with defaults so every existing caller and
    test that passes just (cmd, nodes, cfg) keeps working unchanged, and a caller that
    forgets to wire up authorization gets "unauthorized" rather than an open door.
    """
    parts   = cmd.strip().lower().split(None, 1)
    command = parts[0] if parts else ""
    target  = parts[1].strip() if len(parts) > 1 else None

    if command in WRITE_VERBS:
        return _handle_write_command(
            command, target, nodes,
            dispatch_write=dispatch_write,
            source_hash=source_hash,
            authorized=authorized,
        ), None

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

        # When the offline tile cache is configured, the cached farm box is the
        # authority: maps render that fixed extent and only plot in-bounds nodes.
        bounds        = _cache_bounds(cfg)
        outside_count = 0            # GPS nodes outside offline cache coverage
        render_bounds: Optional[Tuple[float, float, float, float]] = None
        highlight: Optional[str]    = None

        if target:
            snap = nodes[target]
            # Explicitly requested node — never cluster-filter.
            sel = MeshSelection(plotted=dict(geo), omitted=[])
            if bounds is not None and _within_bounds(snap.lat, snap.lon, bounds):
                # Inside the cached farm box: lock the render to that fixed extent,
                # which keeps a single requested node from recentering or
                # over-zooming the map, and keeps the render entirely offline.
                render_bounds = bounds
                highlight     = target
            # Outside it (or no cache configured), render_bounds stays None and we
            # take the node-centered path, which is allowed to reach the fallback
            # tile server. This used to refuse outright, on the reasoning that
            # rendering would pull uncached tiles -- but plain `map` pulls those
            # same tiles for those same nodes without complaint, so the refusal
            # only ever told the operator that one of two adjacent commands had a
            # rule the other did not. A node the farmer explicitly asked for is a
            # worse thing to withhold than a few tiles are to fetch; where there is
            # genuinely no way to get tiles, the render fails and the text fallback
            # below answers, as it already does for every other tile failure.
        else:
            # Plain `map` keeps the original main-mesh behavior: a node-centered
            # extent, so the cached box constrains which nodes are reported as
            # out-of-coverage but never the extent itself.
            sel = _select_main_mesh(geo, cfg)
            if bounds is not None:
                # Populate the warning that has been in both replies all along.
                # Counting it here rather than leaving it at zero is the whole
                # reason the operator can tell "not drawn" from "not covered".
                outside_count = sum(
                    1 for snap in geo.values()
                    if not _within_bounds(snap.lat, snap.lon, bounds)
                )
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
        image_result  = render_map(
            plotted, cfg, bounds=render_bounds, highlight_node_id=highlight
        )

        if (
            target
            and render_bounds is not None
            and image_result is None
            and MAP_AVAILABLE
            and target in geo
        ):
            # Requested node is inside the cache box, but the render extent still
            # needs tiles the offline cache lacks (or preflight failed). Explain
            # that specifically rather than emitting a normal no-image summary.
            snap = geo[target]
            return (
                f"Map image unavailable because required offline tiles are missing "
                f"or outside cache coverage. Node GPS: {snap.lat:.5f}, {snap.lon:.5f}",
                None,
            )

        def _sep_str(m: float) -> str:
            return f"{m / 1000:.1f} km" if m >= 1000 else f"{m:.0f} m"

        if image_result:
            lines = [f"🗺️ Map: {len(plotted)} GPS node(s) plotted"]
            if sel.omitted and sel.nearest_omitted_m is not None:
                lines.append(
                    f"⚠️ {omitted_count} GPS node(s) omitted as separate cluster, "
                    f"nearest {_sep_str(sel.nearest_omitted_m)} from main mesh"
                )
            if outside_count:
                lines.append(f"⚠️ {outside_count} GPS node(s) outside offline map coverage")
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
                soil_str = _soil_band_short(snap)
                if soil_str == "?":
                    soil_str = "no data"
                bat_str  = "USB" if snap.battery_usb else (
                    f"{snap.battery_level:.0f}%" if snap.battery_level is not None else "no data"
                )
                gps_str  = (
                    f"{snap.lat:.5f}, {snap.lon:.5f}"
                    if snap.lat is not None and snap.lon is not None
                    else "no GPS"
                )
                lines += [
                    f"  {_node_label(node_id, snap)} ({node_id})",
                    f"    Soil:    {soil_str}",
                    f"    Battery: {bat_str}",
                    f"    GPS:     {gps_str}", "",
                ]
            if sel.omitted and sel.nearest_omitted_m is not None:
                lines.append(
                    f"⚠️  {omitted_count} GPS node(s) omitted as separate cluster, "
                    f"nearest {_sep_str(sel.nearest_omitted_m)} from main mesh."
                )
            if outside_count:
                lines.append(
                    f"⚠️  {outside_count} GPS node(s) outside offline map coverage."
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
        # Lazily created on the first control command, so a gateway that never issues one
        # never opens an MQTT connection it does not need.
        self._cmd_mqtt: Optional[Any] = None
        self._cmd_seq_lock = threading.Lock()

    # ── Control-command dispatch ──────────────────────────────────────────────

    def _is_authorized(self, sender_hash: str) -> bool:
        """True if this sender may issue control commands. See is_sender_authorized()."""
        return is_sender_authorized(sender_hash, self._cfg.authorized_farmer_hashes)

    def _next_cmd_id(self) -> int:
        """
        Allocate a command id.

        The firmware uses this as a monotonic replay guard, so it must strictly increase
        per node and survive a gateway restart. A second-resolution unix timestamp does
        both without needing any persisted counter; the lock just prevents two commands
        issued in the same second from colliding.
        """
        with self._cmd_seq_lock:
            candidate = int(time.time())
            last = getattr(self, "_last_cmd_id", 0)
            if candidate <= last:
                candidate = last + 1
            self._last_cmd_id = candidate
            return candidate

    def _dispatch_write(self, verb: str, target: str, value, quiet_on, requested_by: str,
                        lat=None, lon=None) -> int:
        """
        Log a control command and publish it to the bridge process for transmission.

        Raises on failure so handle_command() can tell the operator the command did NOT
        go out. Silently swallowing this would leave them believing a node had been
        reconfigured when nothing was ever sent.
        """
        from navamesh import topics
        from navamesh.mqtt_client import MqttPublisher

        cmd_id = self._next_cmd_id()
        params = {"value": value, "quiet_on": quiet_on, "lat": lat, "lon": lon}

        # Record the intent before transmitting, so a command that is sent but never
        # acknowledged still leaves an audit trail.
        self._log_pending_command(cmd_id, verb, target, params, requested_by)

        if self._cmd_mqtt is None:
            self._cmd_mqtt = MqttPublisher(
                self._navamesh_cfg.mqtt_host, self._navamesh_cfg.mqtt_port
            )

        self._cmd_mqtt.publish(
            topics.cmd_request(self._navamesh_cfg.root_cmd),
            {
                "cmd_id": cmd_id,
                "verb": verb,
                "target": target,
                "value": value,
                "quiet_on": quiet_on,
                "lat": lat,
                "lon": lon,
                "requested_by": requested_by,
                "ts": int(time.time()),
            },
            qos=1,
            # Never retained: the broker would redeliver this on every bridge reconnect
            # and re-command the mesh long after the operator moved on.
            retain=False,
        )
        return cmd_id

    def _log_pending_command(self, cmd_id: int, verb: str, target: str,
                             params: dict, requested_by: str) -> None:
        if _psycopg is None or not self._cfg.pg_dsn:
            return
        try:
            with _psycopg.connect(self._cfg.pg_dsn, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO public.command_log
                            (cmd_id, verb, target, params, requested_by, state)
                        VALUES (%s, %s, %s, %s, %s, 'pending')
                        ON CONFLICT (cmd_id) DO NOTHING
                        """,
                        (str(cmd_id), verb, target, json.dumps(params), requested_by),
                    )
        except Exception as exc:
            # Do not abort the command: losing the audit row is worse than nothing, but
            # far less bad than refusing to open a BLE window for a crew already on site.
            logger.warning("Could not log pending command %s: %s", cmd_id, exc)

    def notify_command_outcomes(self) -> None:
        """
        Report finished or timed-out commands back to whoever asked for them.

        Called on a timer by ReticulumBridge. Commands are fire-and-forget from the
        operator's point of view, so this is what closes the loop: the node's ack arrives
        asynchronously through the bridge and the ingestor, long after the original reply
        was sent.
        """
        if _psycopg is None or not self._cfg.pg_dsn:
            return
        try:
            with _psycopg.connect(self._cfg.pg_dsn, autocommit=True) as conn:
                with conn.cursor() as cur:
                    # Anything still pending/sent past the deadline is a timeout: the node
                    # never answered. Marked before selecting so it gets reported too.
                    cur.execute(
                        """
                        UPDATE public.command_log
                           SET state = 'timeout', updated_at = now(), notified = false
                         WHERE state IN ('pending', 'sent')
                           AND requested_at < now() - (%s * interval '1 second')
                        """,
                        (self._cfg.cmd_ack_timeout_seconds,),
                    )
                    cur.execute(
                        """
                        SELECT cmd_id, verb, target, state, detail, requested_by
                          FROM public.command_log
                         WHERE notified = false
                           AND state NOT IN ('pending', 'sent')
                         ORDER BY requested_at
                         LIMIT 20
                        """
                    )
                    rows = cur.fetchall()

                    for cmd_id, verb, target, state, detail, requested_by in rows:
                        if not is_lxmf_destination(requested_by):
                            # Nothing to deliver to: this command came from the CLI or the
                            # bridge, which already reported the outcome over MQTT. Mark it
                            # notified so it is not re-polled every cycle.
                            cur.execute(
                                "UPDATE public.command_log SET notified = true WHERE cmd_id = %s",
                                (cmd_id,),
                            )
                            continue
                        if self._send_outcome(cmd_id, verb, target, state, detail, requested_by):
                            cur.execute(
                                "UPDATE public.command_log SET notified = true WHERE cmd_id = %s",
                                (cmd_id,),
                            )
        except Exception as exc:
            logger.warning("Command outcome poll failed: %s", exc)

    def _send_outcome(self, cmd_id, verb, target, state, detail, requested_by) -> bool:
        """Send one outcome over LXMF. Returns True if it was handed to the router."""
        icon = {"acked": "✅", "nak": "❌", "timeout": "⏱", "error": "⚠️"}.get(state, "ℹ️")
        who = "ALL sensors" if target in ("^all", "all") else _node_label(target)
        what = _verb_label(verb)

        detail = detail or {}
        applied = detail.get("applied_value") if isinstance(detail, dict) else None
        applied_lat = detail.get("applied_lat") if isinstance(detail, dict) else None
        applied_lon = detail.get("applied_lon") if isinstance(detail, dict) else None
        reason = detail.get("reason") if isinstance(detail, dict) else None

        if state == "acked":
            body = f"{icon} {what} confirmed by {who}"
            # The node echoes back the coordinates it actually stored, so report those rather
            # than the ones we sent -- they are what the node will broadcast from now on.
            if applied_lat is not None and applied_lon is not None:
                body += f" = {applied_lat:.6f}, {applied_lon:.6f}"
            elif applied:
                body += f" = {applied}"
        elif state == "timeout":
            retry = ("" if verb in UNICAST_ONLY_VERBS else
                     " Nodes out of direct gateway range cannot be reached by unicast; "
                     "try ^all instead.")
            body = (f"{icon} {what}: no confirmation from {who} within "
                    f"{self._cfg.cmd_ack_timeout_seconds}s.\n"
                    f"The command may still have been applied — check the next reading."
                    f"{retry}")
        elif state == "nak":
            body = f"{icon} {what} refused by {who} — the value was not accepted."
        else:
            body = f"{icon} {what} to {who} failed: {reason or state}"

        try:
            dest = self._dest_from_hash(requested_by)
            if dest is None:
                return False
            self._send_text_to(dest, f"{body}\n(command {cmd_id})")
            return True
        except Exception as exc:
            logger.warning("Could not deliver outcome for %s: %s", cmd_id, exc)
            return False

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
            text_reply, image_result = handle_command(
                cmd, nodes, self._cfg,
                dispatch_write=self._dispatch_write,
                source_hash=sender,
                authorized=self._is_authorized(sender),
            )

            if image_result:
                self._send_with_image(message, text_reply, image_result)
            else:
                self._send_text(message, text_reply)

        except Exception as exc:
            logger.error("Error handling message: %s", exc, exc_info=True)

    def _dest(self, original: Any) -> Any:
        """Build a reply destination for an inbound message."""
        return self._dest_from_hash(original.source_hash)

    def _dest_from_hash(self, source_hash: Any) -> Any:
        """
        Build a destination from a bare identity hash.

        Split out from _dest() so an asynchronous command outcome can be delivered later,
        when the original LXMessage object is long gone. Accepts bytes or a hex string
        (with or without ':' separators) since the command log stores the hex form.
        """
        if isinstance(source_hash, str):
            source_hash = bytes.fromhex(source_hash.strip().lower().replace(":", ""))

        identity = RNS.Identity.recall(source_hash)
        if identity is None:
            logger.info(
                "Identity not cached for %s — requesting path...",
                RNS.hexrep(source_hash, delimit=False),
            )
            RNS.Transport.request_path(source_hash)
            deadline = time.time() + 8
            while time.time() < deadline:
                identity = RNS.Identity.recall(source_hash)
                if identity is not None:
                    logger.info("Path resolved for %s", RNS.hexrep(source_hash, delimit=False))
                    break
                time.sleep(0.2)
        if identity is None:
            raise RuntimeError(
                f"Cannot resolve identity for {RNS.hexrep(source_hash)} — path unknown"
            )
        if not RNS.Transport.has_path(source_hash):
            logger.warning(
                "No RNS path cached for %s — DIRECT delivery will likely fail",
                RNS.hexrep(source_hash, delimit=False),
            )
        return RNS.Destination(
            identity,
            RNS.Destination.OUT, RNS.Destination.SINGLE,
            "lxmf", "delivery",
        )

    def _send_text_to(self, dest: Any, text: str, title: str = "Navamesh") -> None:
        """
        Send text to an already-resolved destination.

        Used for asynchronous command outcomes. Kept simpler than _send_text(): there is
        no OPPORTUNISTIC fallback because a missed outcome notice is cosmetic -- the
        command itself already succeeded or failed on its own, and the command_log row
        stays as the durable record either way.
        """
        with self._lock:
            msg = LXMF.LXMessage(
                destination=dest,
                source=self._source,
                content=text,
                title=title,
                desired_method=LXMF.LXMessage.DIRECT,
            )
            self._router.handle_outbound(msg)

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
        threading.Thread(target=self._command_outcome_loop, daemon=True).start()

    def stop(self) -> None:
        self._stop_event.set()
        self._gateway.stop()

    def _announce_loop(self) -> None:
        while not self._stop_event.wait(self.rns_cfg.announce_interval):
            try:
                self._gateway.announce()
            except Exception as exc:
                logger.warning("Announce loop error: %s", exc)

    def _command_outcome_loop(self) -> None:
        """
        Deliver command acks and timeouts back to the operator.

        Separate from _announce_loop because the cadences are unrelated: announces are
        every few minutes, whereas someone standing in a field waiting for a Bluetooth
        window wants to know within seconds.
        """
        while not self._stop_event.wait(COMMAND_OUTCOME_POLL_SECONDS):
            try:
                self._gateway.notify_command_outcomes()
            except Exception as exc:
                logger.warning("Command outcome loop error: %s", exc)


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
