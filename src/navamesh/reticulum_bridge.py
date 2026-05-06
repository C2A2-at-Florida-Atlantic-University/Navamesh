"""
reticulum_bridge.py — Navamesh → Reticulum/LXMF fanout

Subscribes to all clean MQTT topics published by the Navamesh pipeline and
forwards them as LXMF messages to the farmer's Sideband app over Reticulum.

Architecture
------------
  Field nodes
    → Meshtastic LoRa mesh
    → Navamesh bridge (main.py)
    → Mosquitto MQTT (local)
    → THIS SERVICE                 ← new, runs on the house node
    → Reticulum (LoRa backhaul via Field Access Node)
    → Farmer's Sideband app (phone / laptop)

Deployment
----------
  Run alongside the existing bridge and ingestor:

    python -m navamesh.reticulum_bridge
    # or after pip install -e .
    navamesh-reticulum

Required env vars (add to your .env):
  LXMF_PEER_HASH          — Sideband destination hash (hex, from Sideband → Share address)
  RNS_CONFIG_DIR          — path to Reticulum config dir (default: ~/.reticulum)
  LXMF_DISPLAY_NAME       — display name shown in Sideband (default: "Navamesh Gateway")
  LXMF_STORAGE_DIR        — where to store LXMF identity (default: ~/.navamesh_lxmf)

Optional env vars:
  LXMF_SEND_METHOD        — "direct" or "propagated" (default: direct)
  LXMF_TITLE_PREFIX       — prefix for message titles (default: "🌱 Navamesh")
  LXMF_ANNOUNCE_INTERVAL  — seconds between RNS announces (default: 300)
  LXMF_THROTTLE_SECONDS   — min seconds between messages per node/topic (default: 60)
                             prevents flooding when sensors are noisy
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
except ImportError as exc:  # pragma: no cover
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
    peer_hash: str                  # hex destination hash of the Sideband app
    rns_config_dir: str
    lxmf_storage_dir: str
    display_name: str
    send_method: str                # "direct" | "propagated"
    title_prefix: str
    announce_interval: int          # seconds
    throttle_seconds: int           # min gap between messages per (node, topic_kind)


def load_rns_config() -> ReticulumBridgeConfig:
    def _int(name: str, default: int) -> int:
        v = os.getenv(name)
        return int(v) if v else default

    peer_hash = os.getenv("LXMF_PEER_HASH", "").strip()
    if not peer_hash:
        raise SystemExit(
            "LXMF_PEER_HASH is not set.\n"
            "Open Sideband on your phone/laptop → My Address → copy the hex hash.\n"
            "Add it to your .env:  LXMF_PEER_HASH=<hex>"
        )

    return ReticulumBridgeConfig(
        peer_hash=peer_hash,
        rns_config_dir=os.getenv("RNS_CONFIG_DIR", os.path.expanduser("~/.reticulum")),
        lxmf_storage_dir=os.path.expanduser(
            os.getenv("LXMF_STORAGE_DIR", "~/.navamesh_lxmf")
        ),
        display_name=os.getenv("LXMF_DISPLAY_NAME", "Navamesh Gateway"),
        send_method=os.getenv("LXMF_SEND_METHOD", "direct").lower(),
        title_prefix=os.getenv("LXMF_TITLE_PREFIX", "🌱 Navamesh"),
        announce_interval=_int("LXMF_ANNOUNCE_INTERVAL", 300),
        throttle_seconds=_int("LXMF_THROTTLE_SECONDS", 60),
    )


# ---------------------------------------------------------------------------
# Message formatting
# ---------------------------------------------------------------------------

def _fmt_node(from_id: str) -> str:
    """Shorten '!86b0c98d' → 'Node 8d' for compact titles."""
    return f"Node {from_id[-4:]}" if from_id.startswith("!") else from_id


def format_soil(payload: Dict[str, Any], kind: str) -> Tuple[str, str]:
    """Return (title, body) for a soil MQTT message."""
    node = _fmt_node(payload.get("fromId", "unknown"))
    val = payload.get("value", "?")
    if kind == "soil_raw":
        title = f"Soil ADC · {node}"
        body = (
            f"Node:     {payload.get('fromId', '?')}\n"
            f"Raw ADC:  {val}\n"
            f"Time:     {_ts(payload.get('ts'))}"
        )
    else:
        title = f"Soil Moisture · {node}"
        body = (
            f"Node:       {payload.get('fromId', '?')}\n"
            f"Moisture:   {val}%\n"
            f"Time:       {_ts(payload.get('ts'))}"
        )
    return title, body


def format_battery(payload: Dict[str, Any]) -> Tuple[str, str]:
    node = _fmt_node(payload.get("fromId", "unknown"))
    level = payload.get("batteryLevel")
    voltage = payload.get("voltage")
    usb = payload.get("batteryUsb", False)
    uptime = payload.get("uptimeSeconds")

    bat_str = "USB (charging)" if usb else (f"{level}%" if level is not None else "?")
    volt_str = f"{voltage:.2f}V" if voltage is not None else "N/A"
    up_str = _fmt_uptime(uptime) if uptime is not None else "N/A"

    title = f"Battery · {node}"
    body = (
        f"Node:     {payload.get('fromId', '?')}\n"
        f"Battery:  {bat_str}\n"
        f"Voltage:  {volt_str}\n"
        f"Uptime:   {up_str}\n"
        f"Time:     {_ts(payload.get('ts'))}"
    )
    return title, body


def format_position(payload: Dict[str, Any]) -> Tuple[str, str]:
    node = _fmt_node(payload.get("fromId", "unknown"))
    lat = payload.get("lat", "?")
    lon = payload.get("lon", "?")
    alt = payload.get("alt")
    sats = payload.get("sats")

    alt_str = f"{alt}m" if alt is not None else "N/A"
    sats_str = str(sats) if sats is not None else "N/A"

    title = f"Position · {node}"
    body = (
        f"Node:     {payload.get('fromId', '?')}\n"
        f"Lat:      {lat}\n"
        f"Lon:      {lon}\n"
        f"Alt:      {alt_str}\n"
        f"Sats:     {sats_str}\n"
        f"Time:     {_ts(payload.get('ts'))}"
    )
    return title, body


def format_link(payload: Dict[str, Any]) -> Tuple[str, str]:
    node = _fmt_node(payload.get("fromId", "unknown"))
    rssi = payload.get("rxRssi", "?")
    snr = payload.get("rxSnr", "?")
    hops = payload.get("hopStart", "?")

    title = f"Link Quality · {node}"
    body = (
        f"Node:     {payload.get('fromId', '?')}\n"
        f"RSSI:     {rssi} dBm\n"
        f"SNR:      {snr} dB\n"
        f"Hops:     {hops}\n"
        f"Time:     {_ts(payload.get('ts'))}"
    )
    return title, body


def _ts(unix_ts: Any) -> str:
    """Format a unix timestamp as a human-readable local time string."""
    if unix_ts is None:
        return "unknown"
    try:
        import datetime
        return datetime.datetime.fromtimestamp(int(unix_ts)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(unix_ts)


def _fmt_uptime(seconds: int) -> str:
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


# ---------------------------------------------------------------------------
# Reticulum / LXMF sender
# ---------------------------------------------------------------------------

class LxmfSender:
    """
    Wraps Reticulum + LXMF to send messages to a fixed destination hash.
    Thread-safe: send() may be called from any thread.
    """

    def __init__(self, cfg: ReticulumBridgeConfig):
        self._cfg = cfg
        self._router: Optional[LXMF.LXMRouter] = None
        self._source: Optional[Any] = None          # LXMF local delivery identity
        self._destination: Optional[Any] = None     # RNS.Destination for the peer
        self._lock = threading.Lock()
        self._started = False

    def start(self) -> None:
        os.makedirs(self._cfg.lxmf_storage_dir, exist_ok=True)

        # ── Load or generate the gateway identity ──────────────────────────
        identity_path = os.path.join(self._cfg.lxmf_storage_dir, "identity")
        if os.path.exists(identity_path):
            identity = RNS.Identity.from_file(identity_path)
            logger.info("Loaded existing RNS identity from %s", identity_path)
        else:
            identity = RNS.Identity()
            identity.to_file(identity_path)
            logger.info("Generated new RNS identity, saved to %s", identity_path)

        # ── Start Reticulum ────────────────────────────────────────────────
        RNS.Reticulum(self._cfg.rns_config_dir)
        logger.info("Reticulum started (config: %s)", self._cfg.rns_config_dir)

        # ── Create LXMF router and register the source identity ───────────
        self._router = LXMF.LXMRouter(
            storagepath=self._cfg.lxmf_storage_dir,
            autopeer=True,
        )
        self._source = self._router.register_delivery_identity(
            identity,
            display_name=self._cfg.display_name,
        )
        self._router.announce(self._source.hash)
        logger.info(
            "LXMF source registered. Gateway address: %s",
            RNS.prettyhexrep(self._source.hash),
        )

        # ── Resolve peer destination ───────────────────────────────────────
        peer_hash_bytes = bytes.fromhex(self._cfg.peer_hash)
        peer_identity = RNS.Identity.recall(peer_hash_bytes)

        if peer_identity is None:
            # Not in path table yet — request a path and wait briefly
            logger.info(
                "Peer %s not in path table, requesting path...",
                self._cfg.peer_hash,
            )
            RNS.Transport.request_path(peer_hash_bytes)
            deadline = time.time() + 30
            while time.time() < deadline:
                peer_identity = RNS.Identity.recall(peer_hash_bytes)
                if peer_identity is not None:
                    break
                time.sleep(1)

        if peer_identity is None:
            logger.warning(
                "Could not resolve peer %s within 30s. "
                "Messages will be queued and delivered when the path becomes available.",
                self._cfg.peer_hash,
            )

        self._destination = RNS.Destination(
            peer_identity,
            RNS.Destination.OUT,
            RNS.Destination.SINGLE,
            "lxmf",
            "delivery",
        )
        self._destination.set_default_app_data(
            self._cfg.display_name.encode("utf-8")
        )

        self._started = True
        logger.info("LxmfSender ready → peer %s", self._cfg.peer_hash)

    def send(self, title: str, body: str) -> None:
        """Send an LXMF message to the configured peer. Non-blocking."""
        if not self._started:
            logger.warning("LxmfSender.send() called before start()")
            return

        with self._lock:
            try:
                msg = LXMF.LXMessage(
                    destination=self._destination,
                    source=self._source,
                    content=body,
                    title=title,
                    desired_method=(
                        LXMF.LXMessage.PROPAGATED
                        if self._cfg.send_method == "propagated"
                        else LXMF.LXMessage.DIRECT
                    ),
                )
                msg.register_delivery_callback(self._on_delivered)
                msg.register_failed_callback(self._on_failed)
                self._router.handle_outbound(msg)
                logger.info("Queued LXMF message: %s", title)
            except Exception as exc:
                logger.error("Failed to send LXMF message: %s", exc)

    def announce(self) -> None:
        """Emit a Reticulum announce so peers can discover this gateway."""
        if self._router and self._source:
            self._router.announce(self._source.hash)
            logger.debug("Announced LXMF source identity")

    def stop(self) -> None:
        self._started = False
        # RNS does not expose a clean shutdown API; GC handles it

    # ── Callbacks ────────────────────────────────────────────────────────

    @staticmethod
    def _on_delivered(message: Any) -> None:
        logger.info("LXMF delivered: %s", message.title_as_string())

    @staticmethod
    def _on_failed(message: Any) -> None:
        logger.warning("LXMF delivery failed: %s", message.title_as_string())


# ---------------------------------------------------------------------------
# MQTT → LXMF bridge
# ---------------------------------------------------------------------------

class ReticulumBridge:
    """
    Subscribes to all Navamesh clean MQTT topics and forwards each packet as
    an LXMF message to the farmer's Sideband app.

    Throttling: at most one message per (node_id, topic_kind) per
    cfg.throttle_seconds to avoid flooding when sensors are chatty.
    """

    def __init__(self) -> None:
        self.cfg = load_config()
        self.rns_cfg = load_rns_config()
        self._sender = LxmfSender(self.rns_cfg)
        self._stop_event = threading.Event()
        self.ignored_nodes = set(filter(None, os.getenv("IGNORED_NODES", "").split(",")))

        # throttle: (node_id, kind) → last sent timestamp
        self._throttle: Dict[Tuple[str, str], float] = {}
        self._throttle_lock = threading.Lock()

        try:
            self._mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        except AttributeError:
            self._mqtt = mqtt.Client()

        self._mqtt.on_connect = self._on_connect
        self._mqtt.on_message = self._on_message
        self._mqtt.on_disconnect = self._on_disconnect

        # Topics to subscribe — mirrors mqtt_to_db.py clean topic set
        self._topics = {
            "soil_raw":     f"{self.cfg.root_sensors}/soil/+/raw",
            "soil_percent": f"{self.cfg.root_sensors}/soil/+/percent",
            "position":     f"{self.cfg.root_nodes}/+/position",
            "battery":      f"{self.cfg.root_nodes}/+/battery",
            "link":         f"{self.cfg.root_nodes}/+/link",
        }

    # ── Lifecycle ────────────────────────────────────────────────────────

    def start(self) -> None:
        logger.info("Starting LxmfSender (Reticulum)...")
        self._sender.start()

        logger.info(
            "Connecting to MQTT broker at %s:%s ...",
            self.cfg.mqtt_host,
            self.cfg.mqtt_port,
        )
        self._mqtt.connect(self.cfg.mqtt_host, self.cfg.mqtt_port, 60)
        self._mqtt.loop_start()

        # Periodic RNS announce thread
        t = threading.Thread(target=self._announce_loop, daemon=True)
        t.start()

    def stop(self) -> None:
        self._stop_event.set()
        try:
            self._mqtt.loop_stop()
            self._mqtt.disconnect()
        except Exception:
            pass
        self._sender.stop()

    # ── MQTT callbacks ───────────────────────────────────────────────────

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: Any,
        rc: int,
        properties: Any = None,
    ) -> None:
        if rc != 0:
            logger.error("MQTT connect failed rc=%s", rc)
            return
        logger.info("Connected to MQTT broker")
        for name, topic in self._topics.items():
            client.subscribe(topic)
            logger.info("Subscribed %s → %s", name, topic)

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        rc: int,
        properties: Any = None,
    ) -> None:
        if rc != 0:
            logger.warning("Unexpected MQTT disconnect rc=%s", rc)

    def _on_message(
        self,
        client: mqtt.Client,
        userdata: Any,
        msg: mqtt.MQTTMessage,
    ) -> None:
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except Exception as exc:
            logger.error("JSON decode error on %s: %s", msg.topic, exc)
            return

        kind, node_id = self._classify(msg.topic)
        if kind is None or node_id in self.ignored_nodes:
            return

        if not self._should_send(node_id, kind):
            logger.debug("Throttled (%s, %s)", node_id, kind)
            return

        title, body = self._format(kind, payload)
        full_title = f"{self.rns_cfg.title_prefix} · {title}"
        self._sender.send(full_title, body)

    # ── Topic classification (mirrors mqtt_to_db.py) ─────────────────────

    def _classify(self, topic: str) -> Tuple[Optional[str], Optional[str]]:
        soil_pfx = f"{self.cfg.root_sensors}/soil/"
        node_pfx = f"{self.cfg.root_nodes}/"

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

    # ── Throttle ─────────────────────────────────────────────────────────

    def _should_send(self, node_id: Optional[str], kind: Optional[str]) -> bool:
        key = (node_id or "", kind or "")
        now = time.time()
        with self._throttle_lock:
            last = self._throttle.get(key, 0.0)
            if now - last < self.rns_cfg.throttle_seconds:
                return False
            self._throttle[key] = now
            return True

    # ── Formatting ───────────────────────────────────────────────────────

    @staticmethod
    def _format(kind: str, payload: Dict[str, Any]) -> Tuple[str, str]:
        if kind in ("soil_raw", "soil_percent"):
            return format_soil(payload, kind)
        if kind == "battery":
            return format_battery(payload)
        if kind == "position":
            return format_position(payload)
        if kind == "link":
            return format_link(payload)
        return "Unknown", json.dumps(payload)

    # ── Announce loop ────────────────────────────────────────────────────

    def _announce_loop(self) -> None:
        while not self._stop_event.wait(self.rns_cfg.announce_interval):
            self._sender.announce()


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
        "Forwarding clean MQTT topics → Sideband app (%s). "
        "Press Ctrl+C to stop.",
        bridge.rns_cfg.peer_hash,
    )

    try:
        while not bridge._stop_event.is_set():
            bridge._stop_event.wait(1.0)
    finally:
        bridge.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
