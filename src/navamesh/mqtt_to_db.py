import json
import logging
import os
import signal
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(usecwd=True))

import paho.mqtt.client as mqtt

from navamesh.config import load_config

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

CLOUD_RETRY_INTERVAL = int(os.getenv("CLOUD_RETRY_INTERVAL", "30"))


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
    farm_id: str


@dataclass
class NodeState:
    node_id: str
    farm_id: str
    last_seen_ts: Optional[int] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    alt: Optional[float] = None
    sats: Optional[int] = None
    hdop: Optional[float] = None
    soil_raw: Optional[float] = None
    soil_percent: Optional[float] = None
    battery_level: Optional[float] = None
    battery_usb: Optional[bool] = None    # True when RAK4631 reports "Bat: USB"
    voltage: Optional[float] = None
    uptime_seconds: Optional[int] = None  # from "Up: Xh Ym" in status messages
    rx_rssi: Optional[float] = None
    rx_snr: Optional[float] = None

    def metadata(self, location_name: str, node_type: str) -> Dict[str, Any]:
        return {
            "farm_id": self.farm_id,
            "location": location_name,
            "type": node_type,
            "status": "online",
            "soil_raw": self.soil_raw,
            "soil_percent": self.soil_percent,
            "battery_level": self.battery_level,
            "battery_usb": self.battery_usb,
            "voltage": self.voltage,
            "uptime_seconds": self.uptime_seconds,
            "alt": self.alt,
            "sats": self.sats,
            "hdop": self.hdop,
            "rx_rssi": self.rx_rssi,
            "rx_snr": self.rx_snr,
            "last_packet_ts": self.last_seen_ts,
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
        "battery_level": state.battery_level,
        "battery_usb": state.battery_usb,
        "voltage": state.voltage,
        "uptime_seconds": state.uptime_seconds,
        "rx_rssi": state.rx_rssi,
        "rx_snr": state.rx_snr,
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
        battery_level=d.get("battery_level"),
        battery_usb=d.get("battery_usb"),
        voltage=d.get("voltage"),
        uptime_seconds=d.get("uptime_seconds"),
        rx_rssi=d.get("rx_rssi"),
        rx_snr=d.get("rx_snr"),
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
                    payload   TEXT    NOT NULL
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

    def peek(self, limit: int = 100) -> List[Tuple[int, str, dict]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, target, payload FROM sync_queue ORDER BY id LIMIT ?",
                (limit,),
            ).fetchall()
        return [(row[0], row[1], json.loads(row[2])) for row in rows]

    def delete(self, ids: List[int]) -> None:
        if not ids:
            return
        with self._lock:
            placeholders = ",".join("?" * len(ids))
            self._conn.execute(f"DELETE FROM sync_queue WHERE id IN ({placeholders})", ids)
            self._conn.commit()

    def size(self) -> int:
        with self._lock:
            return self._conn.execute("SELECT COUNT(*) FROM sync_queue").fetchone()[0]

    def close(self) -> None:
        self._conn.close()


class PostgresWriter:
    def __init__(self, dsn: str):
        self._dsn = dsn
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
        self._conn = psycopg.connect(self._dsn)
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
            self._conn = psycopg.connect(self._dsn)
            self._conn.autocommit = True
            self.ensure_schema()
            logger.info("Postgres reconnected.")
            return True
        except Exception as e:
            logger.debug("Postgres reconnect failed: %s", e)
            self._conn = None
            return False

    def ensure_schema(self) -> None:
        if self._conn is None:
            return
        with self._conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS mesh_nodes (
                    farm_id  TEXT NOT NULL,
                    node_id  TEXT NOT NULL,
                    last_seen TIMESTAMPTZ DEFAULT now(),
                    lat      DOUBLE PRECISION,
                    lon      DOUBLE PRECISION,
                    geom     geometry(Point, 4326),
                    metadata JSONB,
                    PRIMARY KEY (farm_id, node_id)
                );
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_mesh_nodes_geom ON mesh_nodes USING GIST (geom);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_mesh_nodes_farm_id ON mesh_nodes (farm_id);"
            )

    def upsert_node(self, state: NodeState, location_name: str, node_type: str) -> None:
        if self._conn is None:
            if not self._try_reconnect():
                raise ConnectionError("Postgres not connected and reconnect failed.")

        ts = state.last_seen_ts or int(datetime.now(tz=timezone.utc).timestamp())
        metadata_json = json.dumps(state.metadata(location_name, node_type))
        has_coords = state.lat is not None and state.lon is not None

        try:
            with self._conn.cursor() as cur:
                if has_coords:
                    cur.execute(
                        """
                        INSERT INTO mesh_nodes (farm_id,node_id, last_seen, lat, lon, geom, metadata)
                        VALUES (
                            %s,
                            to_timestamp(%s),
                            %s,
                            %s,
                            ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                            %s::jsonb
                        )
                        ON CONFLICT (farm_id, node_id) DO UPDATE SET
                            last_seen = EXCLUDED.last_seen,
                            lat       = EXCLUDED.lat,
                            lon       = EXCLUDED.lon,
                            geom      = EXCLUDED.geom,
                            metadata  = EXCLUDED.metadata;
                        """,
                        (state.farm_id, state.node_id, ts, state.lat, state.lon, state.lon, state.lat, metadata_json),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO mesh_nodes (farm_id,node_id, last_seen, metadata)
                        VALUES (%s, %s, to_timestamp(%s), %s::jsonb)
                        ON CONFLICT (farm_id, node_id) DO UPDATE SET
                            last_seen = EXCLUDED.last_seen,
                            metadata  = EXCLUDED.metadata;
                        """,
                        (state.farm_id, state.node_id, ts, metadata_json),
                    )
            logger.info("Upserted mesh_nodes row for %s (coords=%s).", state.node_id, has_coords)
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
        self.farm_id = os.getenv("FARM_ID", "farm_1")
        self._enabled = bool(url and token and org and bucket)

    @property
    def enabled(self) -> bool:
        return self._enabled and InfluxDBClient is not None

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
        ts = datetime.fromtimestamp(state.last_seen_ts or int(datetime.now().timestamp()), tz=timezone.utc)
        point = Point("soil_moisture").tag("node_id", state.node_id).tag("farm_id", state.farm_id)
        if state.soil_raw is not None:
            point = point.field("raw", float(state.soil_raw))
        if state.soil_percent is not None:
            point = point.field("percent", float(state.soil_percent))
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
        for row_id, target, payload in rows:
            try:
                state = _state_from_dict(payload)
                if target == "pg":
                    self._pg.upsert_node(
                        state,
                        payload.get("location_name", ""),
                        payload.get("node_type", ""),
                    )
                elif target == "influx":
                    self._influx.write_soil(state)
                flushed.append(row_id)
            except Exception as e:
                logger.debug("Flush failed for queue id=%d target=%s: %s", row_id, target, e)
                break  # cloud still unreachable — stop and wait for next interval
        if flushed:
            self._queue.delete(flushed)
            logger.info("Cloud sync: flushed %d/%d queued writes.", len(flushed), len(rows))


class MqttToDbIngestor:
    def __init__(self) -> None:
        self.cfg = load_config()
        self.db_cfg = self._load_db_config()
        self.cache: Dict[str, NodeState] = {}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.ignored_nodes = set(filter(None, os.getenv("IGNORED_NODES", "").split(",")))

        # Local writers — primary, always write
        self.pg = PostgresWriter(self.db_cfg.pg_dsn)
        self.influx = InfluxWriter(
            url=self.db_cfg.influx_url,
            token=self.db_cfg.influx_token,
            org=self.db_cfg.influx_org,
            bucket=self.db_cfg.influx_bucket,
        )

        # Cloud writers — secondary, best-effort
        self.pg_cloud = PostgresWriter(self.db_cfg.pg_cloud_dsn)
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
            "position": f"{self.cfg.root_nodes}/+/position",
            "battery": f"{self.cfg.root_nodes}/+/battery",
            "link": f"{self.cfg.root_nodes}/+/link",
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
            farm_id=os.getenv("FARM_ID", "farm_1"),
        )

    def start(self) -> None:
        self.pg.connect()
        self.influx.connect()

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

    def on_connect(self, client: mqtt.Client, userdata: Any, flags: Dict[str, Any], rc: int, properties=None) -> None:
        if rc != 0:
            logger.error("MQTT connect failed with rc=%s", rc)
            return
        logger.info("Connected to MQTT broker.")
        for name, topic in self.topic_patterns.items():
            client.subscribe(topic)
            logger.info("Subscribed to %s -> %s", name, topic)

    def on_disconnect(self, client: mqtt.Client, userdata: Any, *args) -> None:
        rc = args[0] if args else 0
        if rc != 0:
            logger.warning("Unexpected MQTT disconnect rc=%s", rc)
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

        with self.lock:
            kind, node_id = self.classify_topic(topic)
            if kind is None or node_id is None:
                logger.warning("Ignoring unexpected topic: %s", topic)
                return

            # Skip gateway node's own data
            if node_id in self.ignored_nodes:
                return

            cache_key = f"{self.cfg.farm_id}:{node_id}"
            if cache_key not in self.cache:
                self.cache[cache_key] = NodeState(node_id=node_id, farm_id=self.cfg.farm_id)
            state = self.cache[cache_key]
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
            return None, None

        if topic.startswith(nodes_prefix):
            suffix = topic[len(nodes_prefix):]
            parts = suffix.split("/")
            if len(parts) != 2:
                return None, None
            node_id, metric = parts
            if metric in {"position", "battery", "link"}:
                return metric, node_id
            return None, None

        return None, None

    def apply_payload(self, state: NodeState, kind: str, payload: Dict[str, Any]) -> None:
        ts = self._coerce_int(payload.get("ts")) or int(datetime.now(tz=timezone.utc).timestamp())
        state.last_seen_ts = ts

        if kind == "soil_raw":
            state.soil_raw = self._coerce_float(payload.get("value"))
        elif kind == "soil_percent":
            state.soil_percent = self._coerce_float(payload.get("value"))
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

    def write_outputs(self, state: NodeState, kind: str) -> None:
        # --- Local (primary — always write) ---
        if kind in {"soil_raw", "soil_percent", "battery"} and self.influx.enabled:
            self.influx.write_soil(state)
        if self.pg.enabled:
            self.pg.upsert_node(state, self.db_cfg.location_name, self.db_cfg.node_type)

        # --- Cloud (secondary — best-effort, queue on failure) ---
        if kind in {"soil_raw", "soil_percent", "battery"} and self.influx_cloud.enabled:
            try:
                self.influx_cloud.write_soil(state)
            except Exception as e:
                logger.warning("Cloud InfluxDB write failed, queuing: %s", e)
                self.sync_queue.enqueue("influx", _state_to_dict(state))

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


def main() -> int:
    ingestor = MqttToDbIngestor()

    def _shutdown(signum: int, frame: Any) -> None:
        logger.info("Shutting down on signal %s...", signum)
        ingestor.stop()
        sys.exit(0)

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
