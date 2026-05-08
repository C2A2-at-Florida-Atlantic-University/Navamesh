import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Config:
    serial_port: str
    private_channel_index: int

    mqtt_host: str
    mqtt_port: int

    root_raw: str
    root_sensors: str
    root_nodes: str

    adc_dry: int
    adc_wet: int
    
    farm_id: str

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

        adc_dry=getenv_int("ADC_DRY", 3500),
        adc_wet=getenv_int("ADC_WET", 1200),
        farm_id=os.getenv("FARM_ID", "farm_1"),
    )
