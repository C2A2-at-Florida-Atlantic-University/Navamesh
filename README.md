# Navamesh — Private Meshtastic → MQTT Gateway (Offline-First)

Navamesh is an **offline-first telemetry pipeline** that bridges a **private Meshtastic LoRa mesh channel** into a **local MQTT broker (Mosquitto)** running on a MiniPC (house/gateway node). It publishes both **raw mesh packets** and **clean, structured topics** for soil moisture, link quality (RSSI/SNR), GIS position, and battery telemetry.

This repo contains the modular Python bridge that:
- Reads packets from a **USB-connected Meshtastic gateway radio** (RAK4631 / Heltec / etc.)
- Filters traffic to a **private channel** (e.g., `navamesh`, channel index `1`)
- Publishes into Mosquitto on `127.0.0.1:1883`
- Outputs clean MQTT topics for downstream storage and dashboards

Deployment docs:
- [Raspberry Pi deployment](docs/raspberry-pi-deployment.md) — start here for
  field Pi setup, Docker Compose, systemd startup, serial radio ports, logs, and
  common deployment troubleshooting.
- [Farm isolation deployment](docs/farm-isolation-deployment.md) — cloud table
  rollout and farm-specific verification.

---

## Architecture

**Field node(s)** (sensor radios)  
→ **Meshtastic LoRa mesh (private channel + PSK)**  
→ **Gateway radio (USB serial to MiniPC)**  
→ **Navamesh Python Bridge (`src/main.py`)**  
→ **Mosquitto (MQTT broker, local)**  
→ **Subscribers** (CLI, DB writer, dashboard, alerts)

---

## MQTT Topic Schema

### Raw / Debug (what the gateway receives)
- `farm/raw/rx` — all bridged packets (JSON)
- `farm/raw/text` — text packets (JSON)

### Subscribe:

-`mosquitto_sub -h 127.0.0.1 -p 1883 -t "farm/raw/#" -v`

## Soil Moisture (clean topics from text messages like MOISTURE_RAW=585)

-`farm/sensors/soil/<fromId>/raw`

-`farm/sensors/soil/<fromId>/percent`

### Example Payloads:

-`{"value":324,"fromId":"!86b0c98d","ts":1772843516}`

-`{"value":100.0,"fromId":"!86b0c98d","ts":1772843516}`

### Subscribe:
-`mosquitto_sub -h 127.0.0.1 -p 1883 -t "farm/sensors/#" -v`

## Link Quality (RSSI/SNR + hop info)

-`farm/nodes/<fromId>/link`

### Example Payload:
 
`{
  "ts":1772843634,
  "fromId":"!86b0c98d",
  "rxRssi":-36,
  "rxSnr":5.75,
  "hopLimit":3,
  "hopStart":3,
  "relayNode":141,
  "transportMechanism":"TRANSPORT_LORA"
}`

### Subscribe:

-`mosquitto_sub -h 127.0.0.1 -p 1883 -t "farm/nodes/+/link" -v`

## GIS Position (fixed position or POSITION_APP packets)

-`farm/nodes/<fromId>/position`

### Example Payload:

-`{"ts":1772844000,"fromId":"!86b0c98d","lat":26.3979008,"lon":-80.084992,"alt":4}`

### Subscribe:

-`mosquitto_sub -h 127.0.0.1 -p 1883 -t "farm/nodes/+/position" -v`

## Battery (from TELEMETRY_APP device metrics)

-`farm/nodes/<fromId>/battery`

### Subscribe:

-`mosquitto_sub -h 127.0.0.1 -p 1883 -t "farm/nodes/+/battery" -v`

## Requirements

### Software: 

1) Mosquitto MQTT broker installed and running
2)python 3.x

### Python Dependencies:

-`pip install -r requirements.txt`

### Hardware: 

At least two meshtastic nodes:
1) Gateway node connected via USB to the MiniPc
2) Sensor/field nodes on the same private mesh channel 

## Private Channel Setup (Meshtastic)

The pipeline assumes sensor traffic is sent on a private channel, for example:
- channel name: navamesh
- channel index: 1
- Shared PSK: must match across the nodes

# Mosquitto Setup:

Check if mosquito is listening on port 1883:

-`netstat -an | findstr 1883`

Local Test:

-`mosquitto_sub -h 127.0.0.1 -p 1883 -t "farm/test" -v`

-`mosquitto_pub -h 127.0.0.1 -p 1883 -t "farm/test" -m "hello"`

## Run the bridge:

### Activate venv if you use one
`.\.venv\Scripts\activate`

### Ensure runtime can import from src/
$env:PYTHONPATH=".\src"

python .\src\main.py`

## Moisture Calibration (ADC_DRY/ADC_WET)
The soil sensor reports a raw ADC value (e.g. MOISTURE_RAW=324). To convert raw readings into a moisture percentage, the bridge uses two calibration points:
-`ADC_DRY: average raw reading when the sensor is completely dry (air / dry soil)`

-`ADC_WET: average raw reading when the sensor is fully wet (water / saturated soil)`

## Running the tests

```bash
python -m venv .venv && source .venv/bin/activate      # .\.venv\Scripts\activate on Windows
pip install pytest rns lxmf staticmap pillow python-dotenv meshtastic paho-mqtt "protobuf>=6.33.4"
PYTHONPATH=src pytest tests/ -q
```

A correct run reports **248 passed, 0 skipped**.

### Read the skip count, not just the pass count

Every test module that touches `reticulum_bridge` skips itself at collection when the
Reticulum stack is missing:

```python
except (ImportError, SystemExit) as exc:  # rns/lxmf/staticmap/dotenv not installed
    pytest.skip(f"reticulum_bridge unavailable: {exc}", allow_module_level=True)
```

That is deliberate — it keeps the suite usable on a machine without the radio stack. But
it means **a run with those dependencies missing reports success having executed none of
the bridge tests**: the map labels, the command handling, the soil formatting, the
farmer-facing wording. The summary line says "passed" either way, and the only difference
is a skip count nobody reads.

This is not hypothetical. Commit `cd60737` changed the map labels and every command
confirmation, and was verified against a suite that was silently skipping the exact files
covering it. The gap only surfaced when the dependencies were installed deliberately and
the skip count dropped to zero.

So: if `pytest` finishes suspiciously fast, or reports skips, the bridge tests did not
run. See the "Stop the test suite from skipping itself into a green CI" entry in
`TODO.md`.

## Trouble Shooting:
Port in use / access denied
Only one process can open the COM port at a time.

-Stop the bridge before running meshtastic --listen

-Close other apps using the serial port

Verify mesh packets are being received (no MQTT)
Stop the bridge, then:

-`meshtastic --port COM4 --listen`

Verify:

-`mosquitto_sub -h 127.0.0.1 -p 1883 -t "#" -v`
