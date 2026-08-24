import json
import logging
import os
import re
import signal
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(usecwd=True))

import paho.mqtt.client as mqtt

from navamesh.config import load_config

POSTGRES_TABLES = frozenset({"mesh_nodes", "mesh_nodes_farm1", "mesh_nodes_farm2"})
FARM_CLOUD_TABLES = {
    "farm1": "mesh_nodes_farm1",
    "farm2": "mesh_nodes_farm2",
}

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None

try:
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS
except Exception:  # pragma: no cover
    InfluxDBClient = None
    Point = None
    WritePrecision = None
    SYNCHRONOUS = None


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("mqtt_to_db")

# Gateway node IDs to exclude from DB writes
def _load_ignored_nodes() -> frozenset:
    raw = os.getenv("IGNORED_NODES", "")
    return frozenset(n.strip() for n in raw.split(",") if n.strip())

GATEWAY_NODE_IDS = _load_ignored_nodes()

CLOUD_RETRY_INTERVAL = int(os.getenv("CLOUD_RETRY_INTERVAL", "30"))
# After this many consecutive failed flush attempts, an UNKNOWN (unclassifiable)
# error is dead-lettered so it can't block the head of the queue forever. Known
# connectivity/429/5xx outages are never capped — they stay queued to avoid data loss.
CLOUD_QUEUE_MAX_ATTEMPTS = int(os.getenv("CLOUD_QUEUE_MAX_ATTEMPTS", "50"))
# Max length of an error body stored/logged for a dead-lettered row (defensive cap).
CLOUD_ERROR_BODY_MAX = int(os.getenv("CLOUD_ERROR_BODY_MAX", "2000"))


# --- Cloud-sync failure classification ------------------------------------------------
TRANSIENT = "transient"            # keep queued, retry later, preserve FIFO
DROP_RETENTION = "drop_retention"  # timestamp aged out of retention — drop the row
PERMANENT = "permanent"            # will never succeed — dead-letter with diagnostics
UNKNOWN = "unknown"                # unclassifiable — retry, but cap to avoid blocking

_RETENTION_MARKERS = ("retention period", "outside the retention")

# (pattern, replacement) pairs that scrub secrets from error text before it is
# logged or persisted.
_SECRET_PATTERNS = [
    (re.compile(r"(token=)[^&\s\"']+", re.IGNORECASE), r"\1***"),
    (re.compile(r"(password=)[^&\s\"']+", re.IGNORECASE), r"\1***"),
    # Mask the ENTIRE Authorization header value (e.g. "Bearer SECRET"), up to EOL.
    (re.compile(r"(authorization\s*[:=]\s*)[^\r\n]+", re.IGNORECASE), r"\1***"),
    (re.compile(r"(bearer\s+)\S+", re.IGNORECASE), r"\1***"),
    # Mask the password component of a URI-style DSN: scheme://user:PASSWORD@host
    (re.compile(r"(://[^:/@\s]+:)[^@\s]+(@)"), r"\1***\2"),
]
# Env vars whose literal values must never appear in logs/dead-letter rows.
_SECRET_ENV_VARS = (
    "INFLUX_TOKEN", "INFLUX_CLOUD_TOKEN", "PG_DSN", "PG_CLOUD_DSN",
)


def _redact_text(text: str) -> str:
    """Mask credentials in arbitrary error text before logging or persisting."""
    if not text:
        return ""
    for pat, repl in _SECRET_PATTERNS:
        text = pat.sub(repl, text)
    for var in _SECRET_ENV_VARS:
        val = os.getenv(var, "")
        if val and val in text:
            text = text.replace(val, "***")
    return text


def _redact_and_cap(text: object) -> str:
    """Redact secrets then length-cap a single diagnostic component."""
    out = _redact_text(str(text))
    if len(out) > CLOUD_ERROR_BODY_MAX:
        out = out[:CLOUD_ERROR_BODY_MAX] + "…(truncated)"
    return out


def _redact_error(exc: Exception) -> str:
    """Build a fully-redacted, length-capped one-line diagnostic for an exception.

    Every component (reason, body/message) is independently redacted and capped.
    """
    parts: List[str] = []
    status = getattr(exc, "status", None)
    if isinstance(status, int) and not isinstance(status, bool):
        parts.append(f"HTTP {status}")
    reason = getattr(exc, "reason", None)
    if reason:
        parts.append(_redact_and_cap(reason))
    body = getattr(exc, "body", None) or getattr(exc, "message", None) or str(exc)
    parts.append(f"{type(exc).__name__}: {_redact_and_cap(body)}")
    return " | ".join(parts)


def _mentions_retention(exc: Exception) -> bool:
    text = " ".join(
        str(getattr(exc, attr, "") or "") for attr in ("body", "message")
    )
    text = (text + " " + str(exc)).lower()
    return any(marker in text for marker in _RETENTION_MARKERS)


def _is_network_error(exc: Exception) -> bool:
    if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
        return True
    module = type(exc).__module__ or ""
    name = type(exc).__name__
    if module.startswith("urllib3"):
        return True
    return "Timeout" in name or "Connection" in name


def classify_cloud_failure(exc: Exception, target: str) -> str:
    """Classify a cloud-write failure to decide retry vs drop vs dead-letter.

    Duck-typed on ``.status`` so it works whether or not influxdb-client is
    installed (e.g. an ``ApiException`` exposes ``.status``/``.body``).
    """
    status = getattr(exc, "status", None)
    if isinstance(status, bool):
        status = None
    if isinstance(status, int):
        if status == 429 or 500 <= status < 600:
            return TRANSIENT
        if status == 400 and _mentions_retention(exc):
            return DROP_RETENTION
        if 400 <= status < 500:
            return PERMANENT

    if _is_network_error(exc):
        return TRANSIENT

    if psycopg is not None:
        transient_pg = tuple(
            c for c in (
                getattr(psycopg, "OperationalError", None),
                getattr(psycopg, "InterfaceError", None),
            ) if c is not None
        )
        permanent_pg = tuple(
            c for c in (
                getattr(psycopg, "DataError", None),
                getattr(psycopg, "IntegrityError", None),
                getattr(psycopg, "ProgrammingError", None),
                getattr(psycopg, "NotSupportedError", None),
            ) if c is not None
        )
        if transient_pg and isinstance(exc, transient_pg):
            return TRANSIENT
        if permanent_pg and isinstance(exc, permanent_pg):
            return PERMANENT

    return UNKNOWN


@dataclass(frozen=True)
class DatabaseConfig:
    # Local (primary — always write)
    pg_dsn: str
    influx_url: str
    influx_token: str
    influx_org: str
    influx_bucket: str
    # Cloud (secondary — best-effort, queue on failure)
    pg_cloud_dsn: str
    influx_cloud_url: str
    influx_cloud_token: str
    influx_cloud_org: str
    influx_cloud_bucket: str

    location_name: str
    node_type: str


@dataclass
class NodeState:
    node_id: str
    last_seen_ts: Optional[int] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    alt: Optional[float] = None
    sats: Optional[int] = None
    hdop: Optional[float] = None
    soil_raw: Optional[float] = None
    soil_percent: Optional[float] = None
    # DRY / DAMP / WET -- the authoritative derived value. soil_percent is a
    # coarse convenience and is absent outside DAMP (see navamesh.calibration).
    soil_band: Optional[str] = None
    battery_level: Optional[float] = None
    battery_usb: Optional[bool] = None    # True when RAK4631 reports "Bat: USB"
    voltage: Optional[float] = None
    uptime_seconds: Optional[int] = None  # from "Up: Xh Ym" in status messages
    rx_rssi: Optional[float] = None
    rx_snr: Optional[float] = None
    # Meshtastic NODEINFO_APP owner names (app renames); display_name is the
    # resolved label (short first, then long)
    long_name: Optional[str] = None
    short_name: Optional[str] = None
    display_name: Optional[str] = None
    # Meshtastic APP_VERSION as the node reported it in its last ack, e.g.
    # "2.7.20.200289a". None until a node has acked anything since this Pi last
    # restarted, or from a node whose firmware predates the field. Nothing else in
    # the system records what build a node runs -- NodeInfo has no version field and
    # DeviceMetadata never leaves the local serial/BLE link.
    firmware_version: Optional[str] = None
    # Newest packet timestamp applied per payload kind, so a delayed or replayed
    # packet cannot overwrite a field with an older value. Deliberately per-kind
    # rather than one timestamp for the whole state: on startup the ingestor
    # receives every retained topic for a node at once, in arbitrary order and
    # with each topic's own original timestamp, so a single "newer than anything
    # seen" rule would let the first retained message arriving discard all the
    # others. Ingest bookkeeping only -- not serialized to the cloud queue.
    applied_ts: Dict[str, int] = field(default_factory=dict, repr=False)

    # Payload kinds that are an actual soil reading, as opposed to any packet at
    # all. The distinction is the whole point of soil_last_ts below: a node in the
    # wrong role, or with a dead probe, keeps producing the others indefinitely.
    SOIL_KINDS = ("soil_raw", "soil_percent", "soil_band")

    def soil_last_ts(self) -> Optional[int]:
        """When this node last sent a soil reading, or None if it never has.

        Separate from last_seen_ts, which any packet moves -- NodeInfo, battery,
        position, link. A node left in CLIENT role acks commands, broadcasts
        NodeInfo and holds a good link forever while never reading the probe, so
        the two timestamps drifting apart is the signature of that failure and of
        a dead probe. Nothing else in the system could tell them apart.
        """
        stamps = [self.applied_ts[k] for k in self.SOIL_KINDS if k in self.applied_ts]
        return max(stamps) if stamps else None

    def metadata(self, location_name: str, node_type: str) -> Dict[str, Any]:
        return {
            "location": location_name,
            "type": node_type,
            # Write-time only, and not to be read as current: this row is only
            # rewritten when this node is heard from, so a node that stops
            # reporting keeps whatever was stored the last time it did. Consumers
            # must derive liveness from last_seen and soil_last_ts instead -- see
            # classify_node_health() in reticulum_bridge.py. Kept rather than
            # removed because Navamesh-Cloud still selects it.
            "status": "online",
            "soil_raw": self.soil_raw,
            "soil_percent": self.soil_percent,
            "soil_band": self.soil_band,
            "battery_level": self.battery_level,
            "battery_usb": self.battery_usb,
            "voltage": self.voltage,
            "uptime_seconds": self.uptime_seconds,
            "alt": self.alt,
            "sats": self.sats,
            "hdop": self.hdop,
            "rx_rssi": self.rx_rssi,
            "rx_snr": self.rx_snr,
            "long_name": self.long_name,
            "short_name": self.short_name,
            "display_name": self.display_name,
            "firmware_version": self.firmware_version,
            "last_packet_ts": self.last_seen_ts,
            # The fact that makes "present but never reporting" detectable at all.
            "soil_last_ts": self.soil_last_ts(),
        }


def _state_to_dict(state: NodeState, location_name: str = "", node_type: str = "") -> dict:
    return {
        "node_id": state.node_id,
        "last_seen_ts": state.last_seen_ts,
        "lat": state.lat,
        "lon": state.lon,
        "alt": state.alt,
        "sats": state.sats,
        "hdop": state.hdop,
        "soil_raw": state.soil_raw,
        "soil_percent": state.soil_percent,
        "soil_band": state.soil_band,
        "battery_level": state.battery_level,
        "battery_usb": state.battery_usb,
        "voltage": state.voltage,
        "uptime_seconds": state.uptime_seconds,
        "rx_rssi": state.rx_rssi,
        "rx_snr": state.rx_snr,
        "long_name": state.long_name,
        "short_name": state.short_name,
        "display_name": state.display_name,
        "firmware_version": state.firmware_version,
        "location_name": location_name,
        "node_type": node_type,
    }


def _state_from_dict(d: dict) -> NodeState:
    return NodeState(
        node_id=d["node_id"],
        last_seen_ts=d.get("last_seen_ts"),
        lat=d.get("lat"),
        lon=d.get("lon"),
        alt=d.get("alt"),
        sats=d.get("sats"),
        hdop=d.get("hdop"),
        soil_raw=d.get("soil_raw"),
        soil_percent=d.get("soil_percent"),
        soil_band=d.get("soil_band"),
        battery_level=d.get("battery_level"),
        battery_usb=d.get("battery_usb"),
        voltage=d.get("voltage"),
        uptime_seconds=d.get("uptime_seconds"),
        rx_rssi=d.get("rx_rssi"),
        rx_snr=d.get("rx_snr"),
        long_name=d.get("long_name"),
        short_name=d.get("short_name"),
        display_name=d.get("display_name"),
        firmware_version=d.get("firmware_version"),
    )


class CloudSyncQueue:
    """SQLite-backed queue for cloud writes that failed due to connectivity loss."""

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._lock = threading.Lock()
        self._init_schema()
        logger.info("Cloud sync queue at %s", db_path)

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sync_queue (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    queued_at INTEGER NOT NULL,
                    target    TEXT    NOT NULL,
                    payload   TEXT    NOT NULL,
                    attempts  INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            # Backward-compatible migration: a pre-existing queue.db created by an
            # older schema has no `attempts` column. Add it so the already-stuck
            # row is processed on deploy without manually deleting the database.
            cols = {row[1] for row in self._conn.execute("PRAGMA table_info(sync_queue)")}
            if "attempts" not in cols:
                self._conn.execute(
                    "ALTER TABLE sync_queue ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0"
                )
            # Dead-letter store for entries that will never succeed (additive).
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sync_dead_letter (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    orig_id   INTEGER,
                    queued_at INTEGER,
                    failed_at INTEGER NOT NULL,
                    target    TEXT    NOT NULL,
                    payload   TEXT    NOT NULL,
                    error     TEXT
                )
                """
            )
            self._conn.commit()

    def enqueue(self, target: str, payload: dict) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO sync_queue (queued_at, target, payload) VALUES (?, ?, ?)",
                (int(time.time()), target, json.dumps(payload)),
            )
            self._conn.commit()
        logger.debug("Queued failed %s write (queue size=%d).", target, self.size())

    def peek(self, limit: int = 100) -> List[Tuple[int, str, str, int]]:
        # Return the raw payload string (not parsed) so a single corrupt JSON
        # payload cannot raise here and block flushing of the whole queue.
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, target, payload, attempts FROM sync_queue ORDER BY id LIMIT ?",
                (limit,),
            ).fetchall()
        return [(row[0], row[1], row[2], row[3]) for row in rows]

    def delete(self, ids: List[int]) -> None:
        if not ids:
            return
        with self._lock:
            placeholders = ",".join("?" * len(ids))
            self._conn.execute(f"DELETE FROM sync_queue WHERE id IN ({placeholders})", ids)
            self._conn.commit()

    def bump_attempts(self, row_id: int) -> int:
        """Increment the retry counter for a row and return the new value."""
        with self._lock:
            self._conn.execute(
                "UPDATE sync_queue SET attempts = attempts + 1 WHERE id = ?", (row_id,)
            )
            self._conn.commit()
            row = self._conn.execute(
                "SELECT attempts FROM sync_queue WHERE id = ?", (row_id,)
            ).fetchone()
        return row[0] if row else 0

    def dead_letter(self, row_id: int, error: str) -> None:
        """Atomically move a queue row to the dead-letter table.

        Reads the original row (preserving its queued_at), inserts a dead-letter
        record, and deletes the queue row in a single locked transaction.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT queued_at, target, payload FROM sync_queue WHERE id = ?",
                (row_id,),
            ).fetchone()
            if row is None:
                return
            queued_at, target, payload = row
            self._conn.execute(
                """
                INSERT INTO sync_dead_letter
                    (orig_id, queued_at, failed_at, target, payload, error)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (row_id, queued_at, int(time.time()), target, payload, error),
            )
            self._conn.execute("DELETE FROM sync_queue WHERE id = ?", (row_id,))
            self._conn.commit()

    def list_dead_letters(self, limit: int = 100) -> List[Tuple]:
        """Return dead-lettered rows as
        (id, orig_id, queued_at, failed_at, target, error, payload)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, orig_id, queued_at, failed_at, target, error, payload "
                "FROM sync_dead_letter ORDER BY id LIMIT ?",
                (limit,),
            ).fetchall()
        return [tuple(r) for r in rows]

    def count_dead_letters(self) -> int:
        with self._lock:
            return self._conn.execute(
                "SELECT COUNT(*) FROM sync_dead_letter"
            ).fetchone()[0]

    def purge_dead_letters(self) -> int:
        with self._lock:
            n = self._conn.execute(
                "SELECT COUNT(*) FROM sync_dead_letter"
            ).fetchone()[0]
            self._conn.execute("DELETE FROM sync_dead_letter")
            self._conn.commit()
        return n

    def size(self) -> int:
        with self._lock:
            return self._conn.execute("SELECT COUNT(*) FROM sync_queue").fetchone()[0]

    def close(self) -> None:
        self._conn.close()


class CommandLogWriter:
    """
    Records downlink command outcomes in public.command_log (see sql/002).

    Kept entirely separate from PostgresWriter and from the NodeState cache on purpose.
    Every other topic in this process is node-keyed and flows through
    classify_topic -> apply_payload -> write_outputs; command rows are keyed by cmd_id
    instead. Forcing them through that pipeline would mean bending the per-node
    telemetry path, which is the part of this system that must not break.
    """

    def __init__(self, dsn: str):
        self._dsn = dsn
        self._conn = None
        self._enabled = bool(dsn)

    @property
    def enabled(self) -> bool:
        return self._enabled and psycopg is not None

    def connect(self) -> None:
        if not self.enabled:
            logger.warning("Command log disabled: PG_DSN not set or psycopg missing.")
            self._enabled = False
            return
        self._conn = psycopg.connect(
            self._dsn,
            keepalives=1, keepalives_idle=60, keepalives_interval=10, keepalives_count=5,
        )
        self._conn.autocommit = True

    def record_status(self, payload: dict) -> None:
        """
        Upsert the outcome of a command.

        The bridge publishes twice per command (once on transmit, once when the node's
        ack arrives), so this has to be an upsert rather than an update.

        `notified` is reset to false when the state actually CHANGES, so the LXMF poller
        reports the newest outcome rather than stopping after the first one -- but does not
        re-report the same outcome once per arriving ack.

        That distinction is load-bearing for a broadcast. One `^all` command produces one ack
        per node against a single row (this table is keyed by cmd_id), and the firmware now
        spreads those replies across ~45 s so they do not collide. Resetting on every write
        would turn that into an outcome message per poll cycle -- the farmer tapping "all
        sensors" would get the same green confirmation eight or nine times.
        """
        if not self.enabled or self._conn is None:
            return

        cmd_id = payload.get("cmd_id")
        if cmd_id in (None, "", 0):
            # An unsolicited ack carries cmd_id 0 -- either a quiet mode that self-expired
            # or a node's boot announce. There is no request row to attach either to, and
            # the bridge has already logged it. Nothing is lost by dropping it here: the
            # boot announce's whole payload is its firmware version, which reaches
            # mesh_nodes on its own retained topic rather than through this table.
            logger.info("Command log: ignoring status with no cmd_id: %s", payload)
            return

        state = payload.get("state") or "unknown"
        detail = payload.get("detail")
        node_id = payload.get("node_id") or "^all"

        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.command_log
                        (cmd_id, verb, target, params, requested_by, state, detail, updated_at, notified)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, now(), false)
                    ON CONFLICT (cmd_id) DO UPDATE SET
                        state = EXCLUDED.state,
                        detail = EXCLUDED.detail,
                        updated_at = now(),
                        notified = CASE
                            WHEN public.command_log.state IS DISTINCT FROM EXCLUDED.state
                            THEN false
                            ELSE public.command_log.notified
                        END
                    """,
                    (
                        str(cmd_id),
                        payload.get("verb") or "unknown",
                        node_id,
                        json.dumps(payload.get("params")) if payload.get("params") is not None else None,
                        payload.get("requested_by") or "bridge",
                        state,
                        json.dumps(detail) if detail is not None else None,
                    ),
                )
            logger.info("Command log: cmd_id=%s state=%s", cmd_id, state)
        except Exception as exc:
            logger.error("Command log write failed for cmd_id=%s: %s", cmd_id, exc)


class PostgresWriter:
    def __init__(self, dsn: str, table_name: str = "mesh_nodes"):
        if table_name not in POSTGRES_TABLES:
            raise ValueError(f"Postgres table is not allowlisted: {table_name!r}")
        self._dsn = dsn
        self._table_name = table_name
        self._conn = None
        self._enabled = bool(dsn)

    @property
    def enabled(self) -> bool:
        return self._enabled and psycopg is not None

    def connect(self) -> None:
        if not self._enabled:
            logger.warning("Postgres disabled: PG_DSN not set.")
            return
        if psycopg is None:
            logger.warning("Postgres disabled: psycopg is not installed.")
            self._enabled = False
            return
        self._conn = psycopg.connect(
            self._dsn,
            keepalives=1, keepalives_idle=60, keepalives_interval=10, keepalives_count=5,
        )
        self._conn.autocommit = True
        self.ensure_schema()
        logger.info("Connected to Postgres/PostGIS.")

    def _try_reconnect(self) -> bool:
        if not self._enabled or psycopg is None:
            return False
        try:
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
            self._conn = psycopg.connect(
                self._dsn,
                keepalives=1, keepalives_idle=60, keepalives_interval=10, keepalives_count=5,
            )
            self._conn.autocommit = True
            self.ensure_schema()
            logger.info("Postgres reconnected.")
            return True
        except Exception as e:
            logger.warning("Postgres reconnect failed: %s", e)
            self._conn = None
            return False

    def ensure_schema(self) -> None:
        if self._conn is None:
            return
        # The constructor strictly allowlists this identifier; no environment or
        # request value can be interpolated into these statements.
        table = self._table_name
        node_index = f"idx_{table}_node_id"
        geom_index = f"idx_{table}_geom"
        # Create core table (IF NOT EXISTS is a no-op for existing tables)
        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    node_id   TEXT PRIMARY KEY,
                    last_seen TIMESTAMPTZ DEFAULT now(),
                    lat       DOUBLE PRECISION,
                    lon       DOUBLE PRECISION,
                    metadata  JSONB
                );
                """
            )
        # Add PRIMARY KEY if table was created by an old schema without one
        with self._conn.cursor() as cur:
            try:
                cur.execute(
                    f"""
                    DO $$ BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint c
                            JOIN pg_class t ON c.conrelid = t.oid
                            WHERE c.contype = 'p' AND t.relname = '{table}'
                        ) THEN
                            ALTER TABLE {table} ADD PRIMARY KEY (node_id);
                        END IF;
                    END $$;
                    """
                )
            except Exception as e:
                logger.warning("Could not add PRIMARY KEY to %s: %s", table, e)
        # Belt-and-suspenders: unique index guarantees ON CONFLICT (node_id) works
        # even if the PRIMARY KEY migration above failed.
        with self._conn.cursor() as cur:
            try:
                cur.execute(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS {node_index}"
                    f" ON {table} (node_id);"
                )
            except Exception as e:
                logger.warning("Could not create unique index on %s.node_id: %s", table, e)
        # PostGIS geom column — optional, isolated so its failure doesn't break the rest
        with self._conn.cursor() as cur:
            try:
                cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
                cur.execute(
                    f"""
                    DO $$ BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_schema = 'public'
                              AND table_name = '{table}' AND column_name = 'geom'
                        ) THEN
                            ALTER TABLE {table} ADD COLUMN geom geometry(Point, 4326);
                        END IF;
                    END $$;
                    """
                )
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS {geom_index} ON {table} USING GIST (geom);"
                )
                self._postgis = True
            except Exception as e:
                logger.info("PostGIS not available, geom column skipped: %s", e)
                self._postgis = False

    def upsert_node(self, state: NodeState, location_name: str, node_type: str) -> None:
        if self._conn is None:
            if not self._try_reconnect():
                raise ConnectionError("Postgres not connected and reconnect failed.")

        ts = state.last_seen_ts or int(datetime.now(tz=timezone.utc).timestamp())
        metadata_json = json.dumps(state.metadata(location_name, node_type))
        has_coords = state.lat is not None and state.lon is not None
        table = self._table_name
        # Backstop for anything that reaches Postgres with an older timestamp
        # than the row already holds -- chiefly the cloud sync flusher, which
        # replays serialized states carrying their original last_seen_ts long
        # after newer ones have been written. >= rather than > so a reading and
        # a position stamped the same second both land.
        fresh = f"EXCLUDED.last_seen >= {table}.last_seen"
        # soil_last_ts is derived from this process's in-memory applied_ts, which is
        # empty after a restart. Retained MQTT refills it within seconds, but a live
        # battery or link packet arriving first would write a null over a perfectly
        # good stored value in the meantime -- briefly wrong, then self-healing,
        # which is the failure shape this whole change exists to stop. Carry the
        # stored value forward whenever the incoming one has nothing to say.
        # NULLIF is load-bearing: `->` on a JSON null yields the jsonb literal
        # 'null', which is not SQL NULL, so COALESCE would happily choose it and the
        # carry-forward would silently do nothing. Verified against the real schema.
        metadata_expr = (
            f"EXCLUDED.metadata || jsonb_build_object('soil_last_ts', "
            f"COALESCE(NULLIF(EXCLUDED.metadata->'soil_last_ts', 'null'::jsonb), "
            f"{table}.metadata->'soil_last_ts'))"
        )

        try:
            with self._conn.cursor() as cur:
                if has_coords and getattr(self, "_postgis", False):
                    cur.execute(
                        f"""
                        INSERT INTO {table} (node_id, last_seen, lat, lon, geom, metadata)
                        VALUES (
                            %s,
                            to_timestamp(%s),
                            %s,
                            %s,
                            ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                            %s::jsonb
                        )
                        ON CONFLICT (node_id) DO UPDATE SET
                            last_seen = GREATEST({table}.last_seen, EXCLUDED.last_seen),
                            lat       = CASE WHEN {fresh} THEN EXCLUDED.lat  ELSE {table}.lat  END,
                            lon       = CASE WHEN {fresh} THEN EXCLUDED.lon  ELSE {table}.lon  END,
                            geom      = CASE WHEN {fresh} THEN EXCLUDED.geom ELSE {table}.geom END,
                            metadata  = CASE WHEN {fresh} THEN {metadata_expr} ELSE {table}.metadata END;
                        """,
                        (state.node_id, ts, state.lat, state.lon, state.lon, state.lat, metadata_json),
                    )
                elif has_coords:
                    cur.execute(
                        f"""
                        INSERT INTO {table} (node_id, last_seen, lat, lon, metadata)
                        VALUES (%s, to_timestamp(%s), %s, %s, %s::jsonb)
                        ON CONFLICT (node_id) DO UPDATE SET
                            last_seen = GREATEST({table}.last_seen, EXCLUDED.last_seen),
                            lat       = CASE WHEN {fresh} THEN EXCLUDED.lat ELSE {table}.lat END,
                            lon       = CASE WHEN {fresh} THEN EXCLUDED.lon ELSE {table}.lon END,
                            metadata  = CASE WHEN {fresh} THEN {metadata_expr} ELSE {table}.metadata END;
                        """,
                        (state.node_id, ts, state.lat, state.lon, metadata_json),
                    )
                else:
                    cur.execute(
                        f"""
                        INSERT INTO {table} (node_id, last_seen, metadata)
                        VALUES (%s, to_timestamp(%s), %s::jsonb)
                        ON CONFLICT (node_id) DO UPDATE SET
                            last_seen = GREATEST({table}.last_seen, EXCLUDED.last_seen),
                            metadata  = CASE WHEN {fresh} THEN {metadata_expr} ELSE {table}.metadata END;
                        """,
                        (state.node_id, ts, metadata_json),
                    )
            logger.info("Upserted %s row for %s (coords=%s).", table, state.node_id, has_coords)
        except Exception:
            self._conn = None  # mark stale so next call reconnects
            raise

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


class InfluxWriter:
    def __init__(self, url: str, token: str, org: str, bucket: str):
        self._url = url
        self._token = token
        self._org = org
        self._bucket = bucket
        self._client = None
        self._write_api = None
        self._enabled = bool(url and token and org and bucket)

    @property
    def enabled(self) -> bool:
        return self._enabled and InfluxDBClient is not None

    @property
    def connected(self) -> bool:
        """True only when a live write API exists (so writes won't silently no-op)."""
        return self._write_api is not None

    def connect(self) -> None:
        if not self._enabled:
            logger.warning("InfluxDB disabled: missing INFLUX_* environment variables.")
            return
        if InfluxDBClient is None:
            logger.warning("InfluxDB disabled: influxdb-client is not installed.")
            self._enabled = False
            return
        self._client = InfluxDBClient(url=self._url, token=self._token, org=self._org)
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
        logger.info("Connected to InfluxDB.")

    def write_soil(self, state: NodeState) -> None:
        if self._write_api is None:
            return
        ts = datetime.fromtimestamp(
            state.last_seen_ts or int(datetime.now(tz=timezone.utc).timestamp()),
            tz=timezone.utc,
        )
        point = Point("soil_moisture").tag("node_id", state.node_id)
        if state.soil_raw is not None:
            point = point.field("raw", float(state.soil_raw))
        if state.soil_percent is not None:
            point = point.field("percent", float(state.soil_percent))
        if state.soil_band is not None:
            point = point.field("band", str(state.soil_band))
        if state.battery_level is not None:
            point = point.field("battery_level", float(state.battery_level))
        if state.battery_usb is not None:
            point = point.field("battery_usb", int(state.battery_usb))  # 1=USB, 0=battery
        if state.voltage is not None:
            point = point.field("voltage", float(state.voltage))
        if state.uptime_seconds is not None:
            point = point.field("uptime_seconds", float(state.uptime_seconds))
        if state.lat is not None:
            point = point.field("lat", float(state.lat))
        if state.lon is not None:
            point = point.field("lon", float(state.lon))
        point = point.time(ts, WritePrecision.S)
        self._write_api.write(bucket=self._bucket, org=self._org, record=point)
        logger.info("Wrote InfluxDB soil point for %s.", state.node_id)

    def write_link(self, state: NodeState) -> None:
        """Write RSSI/SNR link quality as a time-series measurement."""
        if self._write_api is None:
            return
        if state.rx_rssi is None and state.rx_snr is None:
            return
        ts = datetime.fromtimestamp(
            state.last_seen_ts or int(datetime.now(tz=timezone.utc).timestamp()),
            tz=timezone.utc,
        )
        point = Point("link_quality").tag("node_id", state.node_id)
        if state.rx_rssi is not None:
            point = point.field("rssi", float(state.rx_rssi))
        if state.rx_snr is not None:
            point = point.field("snr", float(state.rx_snr))
        point = point.time(ts, WritePrecision.S)
        self._write_api.write(bucket=self._bucket, org=self._org, record=point)
        logger.info("Wrote InfluxDB link point for %s (rssi=%s snr=%s).", state.node_id, state.rx_rssi, state.rx_snr)

    def write_position(self, state: NodeState) -> None:
        """Write GPS position as a time-series measurement."""
        if self._write_api is None:
            return
        if state.lat is None or state.lon is None:
            return
        ts = datetime.fromtimestamp(
            state.last_seen_ts or int(datetime.now(tz=timezone.utc).timestamp()),
            tz=timezone.utc,
        )
        point = Point("position").tag("node_id", state.node_id)
        point = point.field("lat", float(state.lat))
        point = point.field("lon", float(state.lon))
        if state.alt is not None:
            point = point.field("alt", float(state.alt))
        if state.sats is not None:
            point = point.field("sats", float(state.sats))
        if state.hdop is not None:
            point = point.field("hdop", float(state.hdop))
        point = point.time(ts, WritePrecision.S)
        self._write_api.write(bucket=self._bucket, org=self._org, record=point)
        logger.info("Wrote InfluxDB position point for %s (lat=%s lon=%s).", state.node_id, state.lat, state.lon)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
            self._write_api = None


class CloudSyncWorker:
    """Background thread that flushes the SQLite queue to cloud DBs when connectivity returns."""

    def __init__(
        self,
        queue: CloudSyncQueue,
        pg_cloud: PostgresWriter,
        influx_cloud: InfluxWriter,
        interval: int = 30,
    ):
        self._queue = queue
        self._pg = pg_cloud
        self._influx = influx_cloud
        self._interval = interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True, name="cloud-sync")

    def start(self) -> None:
        self._thread.start()
        logger.info("Cloud sync worker started (retry interval=%ds).", self._interval)

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            pending = self._queue.size()
            if pending == 0:
                continue
            logger.info("Cloud sync: %d item(s) queued, attempting flush...", pending)
            try:
                self._flush()
            except Exception as e:
                logger.warning("Cloud sync worker unexpected error: %s", e)

    def _flush(self) -> None:
        rows = self._queue.peek(limit=100)
        flushed = []
        for row_id, target, payload_str, attempts in rows:
            # Parse per row so one corrupt payload cannot abort the whole flush.
            try:
                payload = json.loads(payload_str)
            except Exception as e:
                logger.error(
                    "Cloud sync: dead-lettering row id=%d with malformed payload: %s",
                    row_id, type(e).__name__,
                )
                self._queue.dead_letter(
                    row_id, f"malformed payload JSON: {type(e).__name__}"
                )
                continue

            try:
                state = _state_from_dict(payload)
                if target == "pg":
                    self._pg.upsert_node(
                        state,
                        payload.get("location_name", ""),
                        payload.get("node_type", ""),
                    )
                elif target in ("influx", "influx_link", "influx_position"):
                    # Never count a write as flushed when the destination is down:
                    # InfluxWriter.write_*() silently no-ops if _write_api is None,
                    # which would delete the row without persisting it. Treat an
                    # unconnected writer as a transient failure so the row is kept.
                    if not self._influx.connected:
                        raise ConnectionError("cloud InfluxDB not connected")
                    if target == "influx":
                        self._influx.write_soil(state)
                    elif target == "influx_link":
                        self._influx.write_link(state)
                    else:
                        self._influx.write_position(state)
                else:
                    # Never silently mark an unsupported target as flushed.
                    logger.error(
                        "Cloud sync: dead-lettering row id=%d with unknown target=%r",
                        row_id, target,
                    )
                    self._queue.dead_letter(row_id, f"unknown target: {target!r}")
                    continue
                flushed.append(row_id)
            except Exception as e:
                category = classify_cloud_failure(e, target)
                diag = _redact_error(e)
                # Track attempts on every failed write for observability...
                attempt_count = self._queue.bump_attempts(row_id)

                if category == DROP_RETENTION:
                    logger.warning(
                        "Cloud sync: dropping stale row id=%d (outside retention): %s",
                        row_id, diag,
                    )
                    self._queue.delete([row_id])
                    continue
                if category == PERMANENT:
                    logger.error(
                        "Cloud sync: dead-lettering row id=%d target=%s (permanent): %s",
                        row_id, target, diag,
                    )
                    self._queue.dead_letter(row_id, diag)
                    continue
                # ...but only UNKNOWN errors are capped. Known connectivity/429/5xx
                # outages stay queued indefinitely so we never lose data.
                if category == UNKNOWN and attempt_count >= CLOUD_QUEUE_MAX_ATTEMPTS:
                    logger.error(
                        "Cloud sync: dead-lettering row id=%d after %d attempts (unknown): %s",
                        row_id, attempt_count, diag,
                    )
                    self._queue.dead_letter(row_id, diag)
                    continue

                logger.info(
                    "Cloud sync: transient failure on id=%d (attempt %d), will retry: %s",
                    row_id, attempt_count, diag,
                )
                break  # preserve FIFO — stop and wait for the next interval
        if flushed:
            self._queue.delete(flushed)
            logger.info("Cloud sync: flushed %d queued writes.", len(flushed))


class MqttToDbIngestor:
    def __init__(self) -> None:
        self.cfg = load_config()
        self.db_cfg = self._load_db_config()
        self.cache: Dict[str, NodeState] = {}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()

        # Local writers — primary, always write
        self.pg = PostgresWriter(self.db_cfg.pg_dsn, "mesh_nodes")
        # Audit trail for downlink commands. Local only: this is an operational record of
        # what was done to the field hardware, not sensor data for the cloud.
        self.command_log = CommandLogWriter(self.db_cfg.pg_dsn)
        self.influx = InfluxWriter(
            url=self.db_cfg.influx_url,
            token=self.db_cfg.influx_token,
            org=self.db_cfg.influx_org,
            bucket=self.db_cfg.influx_bucket,
        )

        # Cloud writers — secondary, best-effort
        self.pg_cloud = PostgresWriter(
            self.db_cfg.pg_cloud_dsn,
            FARM_CLOUD_TABLES[self.cfg.farm_id],
        )
        self.influx_cloud = InfluxWriter(
            url=self.db_cfg.influx_cloud_url,
            token=self.db_cfg.influx_cloud_token,
            org=self.db_cfg.influx_cloud_org,
            bucket=self.db_cfg.influx_cloud_bucket,
        )

        # Offline sync queue — persists writes that failed due to connectivity loss
        queue_path = os.getenv("SYNC_QUEUE_PATH", "cloud_sync_queue.db")
        self.sync_queue = CloudSyncQueue(queue_path)
        self.sync_worker = CloudSyncWorker(
            queue=self.sync_queue,
            pg_cloud=self.pg_cloud,
            influx_cloud=self.influx_cloud,
            interval=CLOUD_RETRY_INTERVAL,
        )

        try:
            # paho-mqtt >= 2.0 requires explicit callback API version
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        except AttributeError:
            # paho-mqtt < 2.0 fallback
            self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

        self.topic_patterns = {
            "soil_raw": f"{self.cfg.root_sensors}/soil/+/raw",
            "soil_percent": f"{self.cfg.root_sensors}/soil/+/percent",
            "soil_band": f"{self.cfg.root_sensors}/soil/+/band",
            "position": f"{self.cfg.root_nodes}/+/position",
            "battery": f"{self.cfg.root_nodes}/+/battery",
            "link": f"{self.cfg.root_nodes}/+/link",
            "info": f"{self.cfg.root_nodes}/+/info",
            "firmware": f"{self.cfg.root_nodes}/+/firmware",
            # Not node-keyed; handled ahead of classify_topic in on_message().
            "cmd_status": f"{self.cfg.root_cmd}/status",
        }

    @staticmethod
    def _load_db_config() -> DatabaseConfig:
        return DatabaseConfig(
            pg_dsn=os.getenv("PG_DSN", ""),
            influx_url=os.getenv("INFLUX_URL", ""),
            influx_token=os.getenv("INFLUX_TOKEN", ""),
            influx_org=os.getenv("INFLUX_ORG", ""),
            influx_bucket=os.getenv("INFLUX_BUCKET", "soil"),
            pg_cloud_dsn=os.getenv("PG_CLOUD_DSN", ""),
            influx_cloud_url=os.getenv("INFLUX_CLOUD_URL", ""),
            influx_cloud_token=os.getenv("INFLUX_CLOUD_TOKEN", ""),
            influx_cloud_org=os.getenv("INFLUX_CLOUD_ORG", ""),
            influx_cloud_bucket=os.getenv("INFLUX_CLOUD_BUCKET", ""),
            location_name=os.getenv("LOCATION_NAME", "FAU Garden"),
            node_type=os.getenv("NODE_TYPE", "field-node"),
        )

    def start(self) -> None:
        self.pg.connect()
        self.influx.connect()

        # Non-fatal: losing the audit trail must not stop telemetry ingestion.
        try:
            self.command_log.connect()
        except Exception as e:
            logger.warning("Command log unavailable at startup: %s", e)

        # Cloud connections — failures are non-fatal; sync queue handles the backlog
        try:
            self.pg_cloud.connect()
        except Exception as e:
            logger.warning("Cloud Postgres unavailable at startup (will retry): %s", e)

        try:
            self.influx_cloud.connect()
        except Exception as e:
            logger.warning("Cloud InfluxDB unavailable at startup (will retry): %s", e)

        self.sync_worker.start()

        logger.info(
            "Connecting to MQTT broker at %s:%s...",
            self.cfg.mqtt_host,
            self.cfg.mqtt_port,
        )
        self.client.connect(self.cfg.mqtt_host, self.cfg.mqtt_port, 60)
        self.client.loop_start()

    def stop(self) -> None:
        self.stop_event.set()
        self.sync_worker.stop()
        try:
            self.client.loop_stop()
        except Exception:
            pass
        try:
            self.client.disconnect()
        except Exception:
            pass
        self.pg.close()
        self.influx.close()
        self.pg_cloud.close()
        self.influx_cloud.close()
        self.sync_queue.close()

    # FIX: paho-mqtt v2 passes a ReasonCode object as the 4th arg, not a plain
    # int. Using hasattr("is_failure") handles both v1 (plain int) and v2
    # (ReasonCode) without breaking on either version.
    def on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        connect_flags: Any,
        reason_code: Any,
        properties: Any = None,
    ) -> None:
        failed = (
            reason_code.is_failure
            if hasattr(reason_code, "is_failure")
            else reason_code != 0
        )
        if failed:
            logger.error("MQTT connect failed: %s", reason_code)
            return
        logger.info("Connected to MQTT broker.")
        for name, topic in self.topic_patterns.items():
            client.subscribe(topic)
            logger.info("Subscribed to %s -> %s", name, topic)

    # FIX: paho-mqtt v2 passes disconnect_flags as a ReasonCode object; the
    # plain rc int is always 0 in v2. Check disconnect_flags first.
    def on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        disconnect_flags: Any = None,
        rc: int = 0,
        properties: Any = None,
    ) -> None:
        reason = (
            disconnect_flags
            if hasattr(disconnect_flags, "is_failure")
            else rc
        )
        is_failure = (
            reason.is_failure
            if hasattr(reason, "is_failure")
            else reason != 0
        )
        if is_failure:
            logger.warning("Unexpected MQTT disconnect: %s", reason)
        else:
            logger.info("Disconnected from MQTT broker.")

    def on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
        topic = msg.topic
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except Exception as exc:
            logger.error("Failed to decode JSON on topic %s: %s", topic, exc)
            return

        logger.info("MQTT received topic=%s payload=%s", topic, payload)

        # Command status is keyed by cmd_id, not node_id, so it is routed here rather
        # than through classify_topic()/NodeState. Handled before that call so a command
        # topic can never be misread as an unexpected node topic.
        if topic == f"{self.cfg.root_cmd}/status":
            self.command_log.record_status(payload)
            return

        with self.lock:
            kind, node_id = self.classify_topic(topic)
            if kind is None or node_id is None:
                logger.warning("Ignoring unexpected topic: %s", topic)
                return

            # Skip gateway node's own data
            if node_id in GATEWAY_NODE_IDS:
                return

            state = self.cache.setdefault(node_id, NodeState(node_id=node_id))
            self.apply_payload(state, kind, payload)
            self.write_outputs(state, kind)

    def classify_topic(self, topic: str) -> Tuple[Optional[str], Optional[str]]:
        soil_prefix = f"{self.cfg.root_sensors}/soil/"
        nodes_prefix = f"{self.cfg.root_nodes}/"

        if topic.startswith(soil_prefix):
            suffix = topic[len(soil_prefix):]
            parts = suffix.split("/")
            if len(parts) != 2:
                return None, None
            node_id, metric = parts
            if metric == "raw":
                return "soil_raw", node_id
            if metric == "percent":
                return "soil_percent", node_id
            if metric == "band":
                return "soil_band", node_id
            return None, None

        if topic.startswith(nodes_prefix):
            suffix = topic[len(nodes_prefix):]
            parts = suffix.split("/")
            if len(parts) != 2:
                return None, None
            node_id, metric = parts
            if metric in {"position", "battery", "link", "info", "firmware"}:
                return metric, node_id
            return None, None

        return None, None

    def apply_payload(self, state: NodeState, kind: str, payload: Dict[str, Any]) -> None:
        ts = self._coerce_int(payload.get("ts")) or int(datetime.now(tz=timezone.utc).timestamp())

        # A packet older than the newest one already applied for this kind is a
        # delayed delivery or a replay, not news. Applying it would rewrite a
        # field with a stale value that the next fresh packet of any other kind
        # then carries into the database under a current timestamp -- which is
        # how a live node's position jumped 2 km and its last_seen went
        # backwards three days while it was reporting normally.
        previous = state.applied_ts.get(kind)
        if previous is not None and ts < previous:
            logger.info(
                "Ignoring stale %s for %s: packet ts %d is older than applied ts %d.",
                kind, state.node_id, ts, previous,
            )
            return
        state.applied_ts[kind] = ts

        # max(), not assignment: recency is the newest packet from this node in
        # any kind, and a stale packet that survives the check above (equal ts)
        # must not pull it back either.
        state.last_seen_ts = max(state.last_seen_ts or 0, ts)

        if kind == "soil_raw":
            state.soil_raw = self._coerce_float(payload.get("value"))
        elif kind == "soil_percent":
            state.soil_percent = self._coerce_float(payload.get("value"))
        elif kind == "soil_band":
            band = payload.get("value")
            state.soil_band = str(band) if band is not None else None
        elif kind == "position":
            state.lat = self._coerce_float(payload.get("lat"))
            state.lon = self._coerce_float(payload.get("lon"))
            state.alt = self._coerce_float(payload.get("alt"))
            state.sats = self._coerce_int(payload.get("sats"))
            state.hdop = self._coerce_float(payload.get("hdop"))
        elif kind == "battery":
            state.battery_level = self._coerce_float(payload.get("batteryLevel"))
            state.voltage = self._coerce_float(payload.get("voltage"))
            # batteryUsb and uptimeSeconds arrive from FORMAT B text messages
            # TELEMETRY_APP packets won't have them — that's fine, we just skip
            if "batteryUsb" in payload:
                state.battery_usb = bool(payload["batteryUsb"])
            if "uptimeSeconds" in payload:
                state.uptime_seconds = self._coerce_int(payload.get("uptimeSeconds"))
        elif kind == "link":
            state.rx_rssi = self._coerce_float(payload.get("rxRssi"))
            state.rx_snr = self._coerce_float(payload.get("rxSnr"))
        elif kind == "info":
            state.long_name = self._coerce_name(
                payload.get("longName", payload.get("long_name"))
            )
            state.short_name = self._coerce_name(
                payload.get("shortName", payload.get("short_name"))
            )
            state.display_name = state.short_name or state.long_name
        elif kind == "firmware":
            # Guarded rather than assigned: this topic is retained, so a redelivery on
            # reconnect must not be able to blank a version the node has since reported.
            version = self._coerce_name(payload.get("firmwareVersion"))
            if version:
                state.firmware_version = version

    def write_outputs(self, state: NodeState, kind: str) -> None:
        # --- Local (primary — always write) ---
        if self.influx.enabled:
            if kind in {"soil_raw", "soil_percent", "soil_band", "battery"}:
                try:
                    self.influx.write_soil(state)
                except Exception as e:
                    logger.warning("Local InfluxDB soil write failed: %s", e)
            elif kind == "link":
                try:
                    self.influx.write_link(state)
                except Exception as e:
                    logger.warning("Local InfluxDB link write failed: %s", e)
            elif kind == "position":
                try:
                    self.influx.write_position(state)
                except Exception as e:
                    logger.warning("Local InfluxDB position write failed: %s", e)

        if self.pg.enabled:
            try:
                self.pg.upsert_node(state, self.db_cfg.location_name, self.db_cfg.node_type)
            except Exception as e:
                logger.warning("Local Postgres write failed: %s", e)

        # --- Cloud (secondary — best-effort, queue on failure) ---
        if self.influx_cloud.enabled:
            if kind in {"soil_raw", "soil_percent", "soil_band", "battery"}:
                try:
                    self.influx_cloud.write_soil(state)
                except Exception as e:
                    logger.warning("Cloud InfluxDB soil write failed, queuing: %s", e)
                    self.sync_queue.enqueue("influx", _state_to_dict(state))
            elif kind == "link":
                try:
                    self.influx_cloud.write_link(state)
                except Exception as e:
                    logger.warning("Cloud InfluxDB link write failed, queuing: %s", e)
                    self.sync_queue.enqueue("influx_link", _state_to_dict(state))
            elif kind == "position":
                try:
                    self.influx_cloud.write_position(state)
                except Exception as e:
                    logger.warning("Cloud InfluxDB position write failed, queuing: %s", e)
                    self.sync_queue.enqueue("influx_position", _state_to_dict(state))

        if self.pg_cloud.enabled:
            try:
                self.pg_cloud.upsert_node(state, self.db_cfg.location_name, self.db_cfg.node_type)
            except Exception as e:
                logger.warning("Cloud Postgres write failed, queuing: %s", e)
                self.sync_queue.enqueue(
                    "pg",
                    _state_to_dict(state, self.db_cfg.location_name, self.db_cfg.node_type),
                )

    @staticmethod
    def _coerce_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _coerce_int(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _coerce_name(value: Any) -> Optional[str]:
        if not isinstance(value, str):
            return None
        value = value.strip()
        return value or None


def main() -> int:
    ingestor = MqttToDbIngestor()

    # FIX: set stop_event instead of calling sys.exit() so the finally block
    # below always runs and all connections are closed cleanly on SIGINT/SIGTERM.
    def _shutdown(signum: int, frame: Any) -> None:
        logger.info("Shutting down on signal %s...", signum)
        ingestor.stop_event.set()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    ingestor.start()
    logger.info("mqtt_to_db ingestor is running. Press Ctrl+C to stop.")

    try:
        while not ingestor.stop_event.is_set():
            ingestor.stop_event.wait(1.0)
    finally:
        ingestor.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
