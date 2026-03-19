import json
import logging
import os
import signal
import sys
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

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


@dataclass(frozen=True)
class DatabaseConfig:
    pg_dsn: str
    influx_url: str
    influx_token: str
    influx_org: str
    influx_bucket: str
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
    battery_level: Optional[float] = None
    voltage: Optional[float] = None
    rx_rssi: Optional[float] = None
    rx_snr: Optional[float] = None

    def metadata(self, location_name: str, node_type: str) -> Dict[str, Any]:
        return {
            "location": location_name,
            "type": node_type,
            "status": "online",
            "soil_raw": self.soil_raw,
            "soil_percent": self.soil_percent,
            "battery_level": self.battery_level,
            "voltage": self.voltage,
            "alt": self.alt,
            "sats": self.sats,
            "hdop": self.hdop,
            "rx_rssi": self.rx_rssi,
            "rx_snr": self.rx_snr,
            "last_packet_ts": self.last_seen_ts,
        }


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

    def ensure_schema(self) -> None:
        if self._conn is None:
            return
        with self._conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS mesh_nodes (
                    node_id TEXT PRIMARY KEY,
                    last_seen TIMESTAMPTZ DEFAULT now(),
                    lat DOUBLE PRECISION,
                    lon DOUBLE PRECISION,
                    geom geometry(Point, 4326),
                    metadata JSONB
                );
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_mesh_nodes_geom ON mesh_nodes USING GIST (geom);"
            )

    def upsert_node(self, state: NodeState, location_name: str, node_type: str) -> None:
        if self._conn is None:
            return
        if state.lat is None or state.lon is None:
            logger.info("Skipping PostGIS upsert for %s: no coordinates yet.", state.node_id)
            return

        ts = state.last_seen_ts or int(datetime.now(tz=timezone.utc).timestamp())
        metadata_json = json.dumps(state.metadata(location_name, node_type))

        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO mesh_nodes (node_id, last_seen, lat, lon, geom, metadata)
                VALUES (
                    %s,
                    to_timestamp(%s),
                    %s,
                    %s,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                    %s::jsonb
                )
                ON CONFLICT (node_id) DO UPDATE SET
                    last_seen = EXCLUDED.last_seen,
                    lat = EXCLUDED.lat,
                    lon = EXCLUDED.lon,
                    geom = EXCLUDED.geom,
                    metadata = EXCLUDED.metadata;
                """,
                (state.node_id, ts, state.lat, state.lon, state.lon, state.lat, metadata_json),
            )
        logger.info("Upserted mesh_nodes row for %s.", state.node_id)

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
        point = Point("soil_moisture").tag("node_id", state.node_id)
        if state.soil_raw is not None:
            point = point.field("raw", float(state.soil_raw))
        if state.soil_percent is not None:
            point = point.field("percent", float(state.soil_percent))
        if state.battery_level is not None:
            point = point.field("battery_level", float(state.battery_level))
        if state.voltage is not None:
            point = point.field("voltage", float(state.voltage))
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


class MqttToDbIngestor:
    def __init__(self) -> None:
        self.cfg = load_config()
        self.db_cfg = self._load_db_config()
        self.cache: Dict[str, NodeState] = {}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()

        self.pg = PostgresWriter(self.db_cfg.pg_dsn)
        self.influx = InfluxWriter(
            url=self.db_cfg.influx_url,
            token=self.db_cfg.influx_token,
            org=self.db_cfg.influx_org,
            bucket=self.db_cfg.influx_bucket,
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
            location_name=os.getenv("LOCATION_NAME", "FAU Garden"),
            node_type=os.getenv("NODE_TYPE", "field-node"),
        )

    def start(self) -> None:
        self.pg.connect()
        self.influx.connect()

        logger.info(
            "Connecting to MQTT broker at %s:%s...",
            self.cfg.mqtt_host,
            self.cfg.mqtt_port,
        )
        self.client.connect(self.cfg.mqtt_host, self.cfg.mqtt_port, 60)
        self.client.loop_start()

    def stop(self) -> None:
        self.stop_event.set()
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

    def on_connect(self, client: mqtt.Client, userdata: Any, flags: Dict[str, Any], rc: int, properties=None) -> None:
        if rc != 0:
            logger.error("MQTT connect failed with rc=%s", rc)
            return
        logger.info("Connected to MQTT broker.")
        for name, topic in self.topic_patterns.items():
            client.subscribe(topic)
            logger.info("Subscribed to %s -> %s", name, topic)

    def on_disconnect(self, client: mqtt.Client, userdata: Any, rc: int, properties=None) -> None:
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
        elif kind == "link":
            state.rx_rssi = self._coerce_float(payload.get("rxRssi"))
            state.rx_snr = self._coerce_float(payload.get("rxSnr"))

    def write_outputs(self, state: NodeState, kind: str) -> None:
        if kind in {"soil_raw", "soil_percent", "battery"} and self.influx.enabled:
            self.influx.write_soil(state)

        if self.pg.enabled:
            self.pg.upsert_node(
                state,
                location_name=self.db_cfg.location_name,
                node_type=self.db_cfg.node_type,
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