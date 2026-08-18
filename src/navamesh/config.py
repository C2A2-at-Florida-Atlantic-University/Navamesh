import os
from dataclasses import dataclass

FARM_ID_ALIASES = {
    "farm1": "farm1",
    "farm_1": "farm1",
    "farm2": "farm2",
    "farm_2": "farm2",
}


def normalize_farm_id(value: str) -> str:
    """Return the canonical farm ID, rejecting unconfigured farms."""
    normalized = FARM_ID_ALIASES.get(value.strip().lower())
    if normalized is None:
        allowed = ", ".join(sorted(FARM_ID_ALIASES))
        raise ValueError(f"Unknown FARM_ID {value!r}; expected one of: {allowed}")
    return normalized

@dataclass(frozen=True)
class Config:
    serial_port: str
    private_channel_index: int

    mqtt_host: str
    mqtt_port: int

    root_raw: str
    root_sensors: str
    root_nodes: str

    # Downlink command bus. reticulum_bridge publishes requests here and the bridge
    # process (the only one holding the serial port) consumes them. Kept off the
    # sensor roots so the ingestor's node-keyed topic classifier never sees them.
    root_cmd: str

    farm_id: str

    # Soil calibration is no longer two env-tunable endpoints. The bench data
    # (15-18 Aug 2026) showed the response is a step, not a line: flat below
    # ~9.5% moisture, ~4000 counts inside half a percent, then saturated above
    # 20%. A two-point linear fit cannot represent that at any setting, so the
    # band thresholds now live in navamesh.calibration. SOIL_ADC_DRY and
    # SOIL_ADC_WET in .env are dead and can be removed.

def load_config() -> Config:
    def getenv_int(name: str, default: int) -> int:
        val = os.getenv(name)
        return int(val) if val is not None and val != "" else default

    return Config(
        serial_port=os.getenv("SERIAL_PORT", "COM4"),
        private_channel_index=getenv_int("PRIVATE_CHANNEL_INDEX", 1),

        mqtt_host=os.getenv("MQTT_HOST", "127.0.0.1"),
        mqtt_port=getenv_int("MQTT_PORT", 1883),

        root_raw=os.getenv("ROOT_RAW", "farm/raw"),
        root_sensors=os.getenv("ROOT_SENSORS", "farm/sensors"),
        root_nodes=os.getenv("ROOT_NODES", "farm/nodes"),
        root_cmd=os.getenv("ROOT_CMD", "farm/cmd"),

        farm_id=normalize_farm_id(os.getenv("FARM_ID", "farm1")),
    )
