"""
Proves that PRIVATE_APP raw ADC is the ONLY authoritative source of soil data.

These tests drive the real navamesh._bridge.main() dispatch with the serial radio
and MQTT broker patched out, so they exercise the actual routing decisions rather
than reimplementing them.

Covers:
  A. PRIVATE_APP raw_adc -> soil_raw + Pi-derived soil_percent
  B. Legacy "Soil: 100%" text does NOT publish soil_percent
  C. TELEMETRY_APP environment_metrics.soil_moisture does NOT publish soil_percent
  D. A legacy message arriving after a valid reading cannot overwrite the percentage
  E. Position / info / battery / link handling is unaffected
"""

import os
import types

import pytest

os.environ.setdefault("FARM_ID", "farm1")
os.environ.setdefault("SOIL_ADC_DRY", "3120")
os.environ.setdefault("SOIL_ADC_WET", "1567")
os.environ.setdefault("PRIVATE_CHANNEL_INDEX", "1")

import navamesh._bridge as B
from navamesh.processors.soil_proto import SOIL_SOURCE
from navamesh.proto import navamesh_pb2

CH = 1
NODE = "!a1b2c3d4"


class FakeMqtt:
    def __init__(self, *a, **k):
        self.published = []
        self.cleared = []
        self.retained_store = {}

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, payload, retain))

    def clear_retained(self, topic):
        self.cleared.append(topic)

    def collect_retained(self, topic_filter, settle_seconds=2.0):
        prefix = topic_filter.replace("+/", "").replace("/percent", "").replace("/raw", "")
        suffix = topic_filter.rsplit("/", 1)[-1]
        return {t: p for t, p in self.retained_store.items()
                if t.startswith(prefix.rstrip("/")) and t.endswith("/" + suffix)}

    def close(self):
        pass


@pytest.fixture
def bridge(monkeypatch):
    """Boot the real main(), hand back (on_receive, mqtt)."""
    holder = {}

    class FakeBridge:
        def __init__(self, port, on_receive=None):
            holder["on_receive"] = on_receive
        def start(self): pass
        def stop(self): pass

    mqtt = FakeMqtt()
    monkeypatch.setattr(B, "MqttPublisher", lambda *a, **k: mqtt)
    monkeypatch.setattr(B, "MeshBridge", FakeBridge)
    monkeypatch.setattr(B, "load_dotenv", lambda *a, **k: None)
    monkeypatch.setattr(B, "find_dotenv", lambda *a, **k: "")
    monkeypatch.setattr(
        B, "time",
        types.SimpleNamespace(sleep=lambda s: (_ for _ in ()).throw(KeyboardInterrupt())),
    )
    B.main()
    mqtt.published.clear()  # drop startup noise
    return holder["on_receive"], mqtt


def soil_topics(mqtt, suffix):
    return [(t, p) for t, p, _ in mqtt.published
            if t.startswith("farm/sensors/soil/") and t.endswith("/" + suffix)]


def private_app_packet(raw_adc=1842, battery_percent=85, battery_mv=4020, uptime=86400):
    wire = navamesh_pb2.SoilReading(
        raw_adc=raw_adc, battery_percent=battery_percent,
        battery_mv=battery_mv, uptime_seconds=uptime,
    ).SerializeToString()
    return {"fromId": NODE, "channel": CH,
            "decoded": {"portnum": "PRIVATE_APP", "payload": wire}}


def legacy_text_packet(text="Soil: 100% | Bat: 85% | Up: 24h 0m"):
    return {"fromId": NODE, "channel": CH,
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "text": text}}


# ===========================================================================
# A. PRIVATE_APP is authoritative
# ===========================================================================

def test_A_private_app_produces_raw_and_derived_percent(bridge):
    on_receive, mqtt = bridge
    on_receive(private_app_packet(raw_adc=1842))

    raw = soil_topics(mqtt, "raw")
    pct = soil_topics(mqtt, "percent")

    assert len(raw) == 1 and raw[0][1]["value"] == 1842
    assert len(pct) == 1 and pct[0][1]["value"] == pytest.approx(82.29, abs=0.01)
    # Both stamped as authoritative so startup purge can recognise them.
    assert raw[0][1]["source"] == SOIL_SOURCE
    assert pct[0][1]["source"] == SOIL_SOURCE


# ===========================================================================
# B. Legacy "Soil: XX%" text must not publish soil percent
# ===========================================================================

def test_B_legacy_soil_text_does_not_publish_percent(bridge):
    on_receive, mqtt = bridge
    on_receive(legacy_text_packet("Soil: 100% | Bat: 85% | Up: 24h 0m"))

    assert soil_topics(mqtt, "percent") == []
    assert soil_topics(mqtt, "raw") == []


def test_B2_legacy_text_still_salvages_battery(bridge):
    """Requirement 7: unrelated legacy processing is kept."""
    on_receive, mqtt = bridge
    on_receive(legacy_text_packet("Soil: 100% | Bat: 85% | Up: 24h 0m"))

    bat = [p for t, p, _ in mqtt.published if t == f"farm/nodes/{NODE}/battery"]
    assert len(bat) == 1
    assert bat[0]["batteryLevel"] == 85.0
    assert bat[0]["uptimeSeconds"] == 86400


def test_B3_new_adc_debug_text_is_not_parsed_into_soil_storage(bridge):
    """Requirement 4: the ADC debug line must not become a soil measurement."""
    on_receive, mqtt = bridge
    on_receive(legacy_text_packet("ADC: 1842 | Bat: 85% (4.02V) | Up: 24h 0m"))

    assert soil_topics(mqtt, "percent") == []
    assert soil_topics(mqtt, "raw") == []


# ===========================================================================
# C. TELEMETRY_APP environment_metrics.soil_moisture must be ignored
# ===========================================================================

def test_C_environment_metrics_soil_moisture_ignored(bridge):
    on_receive, mqtt = bridge
    on_receive({
        "fromId": NODE, "channel": CH,
        "decoded": {
            "portnum": "TELEMETRY_APP",
            "telemetry": {"environmentMetrics": {"soilMoisture": 100,
                                                 "soilTemperature": 21.5}},
        },
    })

    assert soil_topics(mqtt, "percent") == []
    assert soil_topics(mqtt, "raw") == []


def test_C2_device_metrics_battery_still_works(bridge):
    """Requirement 8: battery/device telemetry handling is untouched."""
    on_receive, mqtt = bridge
    on_receive({
        "fromId": NODE, "channel": CH,
        "decoded": {
            "portnum": "TELEMETRY_APP",
            "telemetry": {"deviceMetrics": {"batteryLevel": 72, "voltage": 3.85,
                                            "uptimeSeconds": 12120}},
        },
    })

    bat = [p for t, p, _ in mqtt.published if t == f"farm/nodes/{NODE}/battery"]
    assert len(bat) == 1 and bat[0]["batteryLevel"] == 72
    assert soil_topics(mqtt, "percent") == []


# ===========================================================================
# D. Legacy arriving AFTER a valid reading cannot overwrite the percentage
# ===========================================================================

def test_D_legacy_after_valid_reading_cannot_overwrite(bridge):
    on_receive, mqtt = bridge

    on_receive(private_app_packet(raw_adc=1842))
    authoritative = soil_topics(mqtt, "percent")[0][1]["value"]

    # Legacy node shouting 100% afterwards must publish nothing to the soil topics.
    on_receive(legacy_text_packet("Soil: 100% | Bat: 85% | Up: 24h 0m"))
    on_receive(legacy_text_packet("Soil: 0% | Bat: 85% | Up: 24h 0m"))

    pct = soil_topics(mqtt, "percent")
    assert len(pct) == 1, "legacy text published a second soil percent"
    assert pct[0][1]["value"] == authoritative == pytest.approx(82.29, abs=0.01)


def test_D2_startup_purges_legacy_retained_but_keeps_authoritative(monkeypatch):
    """
    Requirement 6: a retained legacy percent must not survive into the new world,
    while a retained value from the raw-ADC path is preserved.
    """
    mqtt = FakeMqtt()
    mqtt.retained_store = {
        # Legacy: no source marker -> must be purged
        "farm/sensors/soil/!legacy01/percent": {"value": 100, "fromId": "!legacy01"},
        # Authoritative: marked -> must be kept
        "farm/sensors/soil/!new00001/percent": {"value": 82.29, "fromId": "!new00001",
                                                "source": SOIL_SOURCE},
        "farm/sensors/soil/!new00001/raw": {"value": 1842, "fromId": "!new00001",
                                            "source": SOIL_SOURCE},
    }

    cleared = B.purge_legacy_retained_soil(mqtt, "farm/sensors")

    assert "farm/sensors/soil/!legacy01/percent" in mqtt.cleared
    assert "farm/sensors/soil/!new00001/percent" not in mqtt.cleared
    assert "farm/sensors/soil/!new00001/raw" not in mqtt.cleared
    assert cleared == 1


# ===========================================================================
# E. Unrelated packet handling is unaffected
# ===========================================================================

def test_E_position_info_link_unaffected(bridge):
    on_receive, mqtt = bridge

    on_receive({
        "fromId": NODE, "channel": CH, "rxRssi": -95, "rxSnr": 6.25,
        "decoded": {
            "portnum": "POSITION_APP",
            "position": {"latitude": 26.37, "longitude": -80.10, "altitude": 5,
                         "satsInView": 9},
        },
    })
    on_receive({
        "fromId": NODE, "channel": CH,
        "decoded": {"portnum": "NODEINFO_APP",
                    "user": {"longName": "Field Node 1", "shortName": "FN1"}},
    })

    published = {t for t, _, _ in mqtt.published}
    assert f"farm/nodes/{NODE}/position" in published
    assert f"farm/nodes/{NODE}/info" in published
    assert f"farm/nodes/{NODE}/link" in published

    pos = [p for t, p, _ in mqtt.published if t.endswith("/position")][0]
    assert pos["lat"] == pytest.approx(26.37)
    info = [p for t, p, _ in mqtt.published if t.endswith("/info")][0]
    assert info["longName"] == "Field Node 1"
    link = [p for t, p, _ in mqtt.published if t.endswith("/link")][0]
    assert link["rxRssi"] == -95

    # And none of it invented a soil measurement.
    assert soil_topics(mqtt, "percent") == []
    assert soil_topics(mqtt, "raw") == []


def test_E2_private_app_still_publishes_battery_and_raw_firehose(bridge):
    on_receive, mqtt = bridge
    on_receive(private_app_packet())

    published = {t for t, _, _ in mqtt.published}
    assert "farm/raw/rx" in published
    assert f"farm/nodes/{NODE}/battery" in published

    bat = [p for t, p, _ in mqtt.published if t.endswith("/battery")][0]
    assert bat["voltage"] == 4.02  # only the protobuf path carries voltage
