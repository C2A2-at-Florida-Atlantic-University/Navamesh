"""
reticulum_bridge.py — Navamesh LXMF command/response gateway

Listens for incoming LXMF messages from the farmer's Sideband app and
replies with live sensor data pulled from the local MQTT cache.

The Pi does NOT need the farmer's address hardcoded — it learns it
automatically from the first incoming message.

Supported commands (case-insensitive, send from Sideband):
  status   — full summary of all nodes (soil, battery, link, position)
  soil     — soil moisture readings for all nodes
  battery  — battery levels for all nodes
  position — GPS coordinates for all nodes
  link     — RSSI/SNR link quality for all nodes
  help     — list available commands

Architecture
------------
  Farmer types command in Sideband
    → LXMF message over Reticulum (LoRa backhaul)
    → THIS SERVICE on house node
    → looks up latest data from MQTT cache
    → replies directly to farmer's Sideband address

Required env vars:
  RNS_CONFIG_DIR      — path to Reticulum config dir (default: ~/.reticulum)
  LXMF_STORAGE_DIR    — where to store LXMF identity (default: ~/.navamesh_lxmf)
  LXMF_DISPLAY_NAME   — display name shown in Sideband (default: "Navamesh Gateway")

Optional env vars:
  LXMF_ANNOUNCE_INTERVAL — seconds between RNS announces (default: 300)
  IGNORED_NODES          — comma-separated node IDs to ignore (same as mqtt_to_db)
  LOG_LEVEL              — logging level (default: INFO)
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
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

from navamesh.config import load_config

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [reticulum_bridge] %(message)s",
)
logger = logging.getLogger("reticulum_bridge")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ReticulumBridgeConfig:
    rns_config_dir: str
    lxmf_storage_dir: str
    display_name: str
    announce_interval: int


def load_rns_config() -> ReticulumBridgeConfig:
    def _int(name: str, default: int) -> int:
        v = os.getenv(name)
        return int(v) if v else default

    return ReticulumBridgeConfig(
        rns_config_dir=os.getenv("RNS_CONFIG_DIR", os.path.expanduser("~/.reticulum")),
        lxmf_storage_dir=os.path.expanduser(
            os.getenv("LXMF_STORAGE_DIR", "~/.navamesh_lxmf")
        ),
        display_name=os.getenv("LXMF_DISPLAY_NAME", "Navamesh Gateway"),
        announce_interval=_int("LXMF_ANNOUNCE_INTERVAL", 300),
    )


# ---------------------------------------------------------------------------
# Sensor cache — updated by MQTT subscriber
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Response formatters
# ---------------------------------------------------------------------------

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

  status   — full summary of all nodes
  soil     — soil moisture readings
  battery  — battery levels & uptime
  position — GPS coordinates
  link     — RSSI/SNR link quality
  help     — this message

Send any command from Sideband to query live data."""


def handle_command(cmd: str, cache: SensorCache) -> str:
    cmd = cmd.strip().lower().split()[0] if cmd.strip() else ""
    if cmd == "status":
        return fmt_status(cache)
    elif cmd == "soil":
        return fmt_soil(cache)
    elif cmd == "battery":
        return fmt_battery(cache)
    elif cmd == "position":
        return fmt_position(cache)
    elif cmd == "link":
        return fmt_link(cache)
    elif cmd == "help":
        return HELP_TEXT
    else:
        return f"Unknown command: '{cmd}'\n\n{HELP_TEXT}"


# ---------------------------------------------------------------------------
# LXMF listener / responder
# ---------------------------------------------------------------------------

class LxmfGateway:
    """
    Registers an LXMF delivery identity, announces on Reticulum, and
    handles incoming messages by replying with sensor data.
    """

    def __init__(self, cfg: ReticulumBridgeConfig, cache: SensorCache):
        self._cfg = cfg
        self._cache = cache
        self._router: Optional[LXMF.LXMRouter] = None
        self._source: Optional[Any] = None
        self._lock = threading.Lock()

    def start(self) -> None:
        os.makedirs(self._cfg.lxmf_storage_dir, exist_ok=True)

        # Load or generate identity
        identity_path = os.path.join(self._cfg.lxmf_storage_dir, "identity")
        if os.path.exists(identity_path):
            identity = RNS.Identity.from_file(identity_path)
            logger.info("Loaded existing RNS identity from %s", identity_path)
        else:
            identity = RNS.Identity()
            identity.to_file(identity_path)
            logger.info("Generated new RNS identity, saved to %s", identity_path)

        # Start Reticulum
        RNS.Reticulum(self._cfg.rns_config_dir)
        logger.info("Reticulum started (config: %s)", self._cfg.rns_config_dir)

        # Create LXMF router
        self._router = LXMF.LXMRouter(
            storagepath=self._cfg.lxmf_storage_dir,
            autopeer=True,
        )

        # Register delivery identity with incoming message handler
        self._source = self._router.register_delivery_identity(
            identity,
            display_name=self._cfg.display_name,
        )
        self._router.register_delivery_callback(self._on_message)

        # Announce so farmer's Sideband can discover the gateway
        self._router.announce(self._source.hash)
        logger.info(
            "LXMF gateway ready. Address: %s  Display name: %s",
            RNS.prettyhexrep(self._source.hash),
            self._cfg.display_name,
        )
        logger.info(
            "Waiting for commands from Sideband. "
            "The farmer can find this gateway by announcing or sending a message."
        )

    def _on_message(self, message: Any) -> None:
        """Called by LXMF router when a message arrives."""
        try:
            sender_hash = RNS.hexrep(message.source_hash, delimit=False)
            content = message.content.decode("utf-8").strip() if message.content else ""
            title = message.title.decode("utf-8").strip() if message.title else ""
            cmd = content or title
            logger.info("Received command from %s: %r", sender_hash, cmd)

            response = handle_command(cmd, self._cache)
            self._reply(message, response)
        except Exception as exc:
            logger.error("Error handling message: %s", exc)

    def _reply(self, original: Any, text: str) -> None:
        """Send a reply LXMF message back to the sender."""
        with self._lock:
            try:
                reply = LXMF.LXMessage(
                    destination=RNS.Destination(
                        original.source,
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
                logger.info("Reply queued to %s", RNS.hexrep(original.source_hash, delimit=False))
            except Exception as exc:
                logger.error("Failed to send reply: %s", exc)

    def announce(self) -> None:
        if self._router and self._source:
            self._router.announce(self._source.hash)
            logger.debug("Announced LXMF identity")

    def stop(self) -> None:
        pass  # RNS GC handles cleanup


# ---------------------------------------------------------------------------
# MQTT subscriber — keeps sensor cache up to date
# ---------------------------------------------------------------------------

class MqttCacheUpdater:
    """
    Subscribes to all clean Navamesh MQTT topics and updates the SensorCache.
    Mirrors the topic classification from mqtt_to_db.py.
    """

    def __init__(self, cfg, cache: SensorCache, ignored_nodes: set):
        self._cfg = cfg
        self._cache = cache
        self._ignored = ignored_nodes

        try:
            self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        except AttributeError:
            self._client = mqtt.Client()

        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
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
        logger.info("MQTT cache updater connecting to %s:%s", self._cfg.mqtt_host, self._cfg.mqtt_port)

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
            logger.info("Subscribed %s → %s", name, topic)

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


# ---------------------------------------------------------------------------
# Main bridge
# ---------------------------------------------------------------------------

class ReticulumBridge:
    def __init__(self) -> None:
        self.cfg = load_config()
        self.rns_cfg = load_rns_config()
        self.ignored_nodes = set(filter(None, os.getenv("IGNORED_NODES", "").split(",")))
        self._stop_event = threading.Event()

        self._cache = SensorCache()
        self._gateway = LxmfGateway(self.rns_cfg, self._cache)
        self._mqtt = MqttCacheUpdater(self.cfg, self._cache, self.ignored_nodes)

    def start(self) -> None:
        logger.info("Starting Reticulum LXMF gateway...")
        self._gateway.start()

        logger.info("Starting MQTT cache updater...")
        self._mqtt.start()

        # Periodic announce thread
        t = threading.Thread(target=self._announce_loop, daemon=True)
        t.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._mqtt.stop()
        self._gateway.stop()

    def _announce_loop(self) -> None:
        while not self._stop_event.wait(self.rns_cfg.announce_interval):
            self._gateway.announce()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

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
        "Send 'help' from Sideband to get started. "
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
