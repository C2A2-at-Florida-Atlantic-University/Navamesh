# Navamesh — orientation for an agent working across the three repos

Soil sensors on a farm with no internet. RAK4631 radios read a probe, send the raw ADC
over LoRa to a Raspberry Pi gateway, and a farmer drives the whole thing from an Android
app over Reticulum. This file is the map; `TODO.md` is the list of known problems and why
they matter.

## The three repos and how they fit

They are not siblings on disk. Typical layout:

```
dev/
├── Navamesh-main/                        ← this repo. The Pi.
├── Navamesh_sideband_wrapper/            ← the Android app (a Sideband fork)
└── Navamesh-Hardware/
    └── meshtastic-soil-sensor/           ← the node firmware (a Meshtastic fork)
```

All three track the branch **`raw-adc-private-app`**. The firmware repo pushes to the
`myfork` remote, not `origin` — `origin` (Sarkors) is read-only for us and a push there
403s.

A command flows through all three, which is why a change in one usually needs a matching
change in another:

```
app (button)  →  LXMF over Reticulum  →  Pi reticulum_bridge  →  MQTT
              →  Pi _bridge.py  →  LoRa portnum 258  →  node NavameshCommand.cpp
              →  applied  →  ack on portnum 259  →  back up the same chain to the app
```

Readings flow the other way: node → `navamesh.SoilReading` on portnum 256 → Pi bridge →
MQTT → ingestor → Postgres/Influx → the app's map and status replies.

The wire contract lives in `navamesh.proto`, duplicated **byte-identically** in this repo
(`src/navamesh/proto/`) and the firmware (`proto/navamesh/`). If you change one, change
both, and regenerate. Bounds are deliberately triplicated — UI, Pi, firmware — so a bad
value is caught early, explained in the middle, and clamped as a last resort.

## Which process runs what on the Pi

Three containers build from **this** repo, and picking the wrong one wastes a rebuild:

| Container | Runs | Handles |
|---|---|---|
| `navamesh_reticulum` | `src/navamesh/reticulum_bridge.py` | **the phone's commands**, map rendering, text replies |
| `navamesh_bridge` | `main.py` | serial radio ↔ MQTT, ack decoding |
| `navamesh_ingestor` | `src/navamesh/mqtt_to_db.py` | MQTT → Postgres/Influx |

Deploy by pushing from the dev machine and pulling on the Pi (`/home/tj/Navamesh`), never
the reverse. Then `docker compose build <service> && docker compose up -d <service>`.

## Traps that have already cost real time

**The test suite skips itself into a green run.** Without `rns/lxmf/staticmap/dotenv`
installed, every bridge test module skips at collection and the summary still says
"passed". See "Running the tests" in `README.md`. Read the skip count.

**The Pi's `.env` is test-bench configuration, not deployment configuration.** It is
gitignored, so it cannot leak into a deployment — but do not copy it around or treat it as
canonical. On the test Pi the cloud targets (`PG_CLOUD_DSN`, `INFLUX_CLOUD_*`) are
commented out deliberately, because a test node's readings should not reach the cloud.

**`IGNORED_NODES` filters silently.** A node on that list transmits normally, appears in
the Meshtastic app and the bridge logs, and never reaches the database or the app's node
picker. Nothing logs that it is being dropped. A replacement radio that reused a retired
id looked broken for an hour because of this.

**A node in the wrong role looks perfectly healthy.** `CLIENT` acks commands, broadcasts
NodeInfo and sits in the picker while never sending a single reading. Provisioning order
that works: region → role `SENSOR` → channel/PSK at **index 1** → **reboot** → shorten the
interval. The reboot is required: `EnvironmentTelemetry.cpp` returns early at init when
environment telemetry is off, so the sensor list is fixed at boot.

**Channel index matters.** `navamesh` must be a *secondary* channel at index 1; the Pi
sends commands there (`PRIVATE_CHANNEL_INDEX=1`). A node with only the default channel
still shows up — the default PSK is public — while being deaf to every command.

**Nothing records a node's firmware version**, so "this build lacks the handler" and "the
handler rejected the value" look identical from the Pi. Meshtastic embeds the build hash
in its version string, visible over serial or BLE (`2.7.20.a36db94`).

**The Pi's ssh host key is stale** under `raspberrypi.local`. Either
`ssh-keygen -R raspberrypi.local` once, or connect with
`-o HostKeyAlias=raspberrypi`, which is already trusted.

## Conventions worth matching

Comments here explain **why**, especially where the obvious approach was tried and
rejected. Several are load-bearing: the proto file records why portnums 258/259 are
separate, `applySetLocation()` records why it mirrors AdminModule rather than calling it,
and `applyTelemetryInterval()` records why it avoids `reloadConfig()`. Do not compress
those into restatements of the code.

`TODO.md` entries are open questions with a **Status**, the context that produced them,
and what closing them would take — not a bullet list of tasks. Match that shape.

## Recent work (Aug 2026)

Ends at: app **1.9.18**, Pi `781e61d`, firmware `a36db9409`.

- **`SET_LOCATION`** — set a node's fixed position from the phone's GPS over LoRa, since
  the nodes have no receiver of their own. Verified end to end on three nodes.
- **OTA app updates that survive sleep** — handed to Android's `DownloadManager`, because
  an in-process download died the moment the screen went off and restarted from byte 0.
  This exposed that cleartext HTTP is blocked for Android's own network stack but never
  for `urllib`; the manifest attribute is patched into p4a's templates by
  `scripts/build_apk.sh` in the app repo, because buildozer's documented option for it is
  broken.
- **GPS fixes are read from `LocationManager` directly**, not plyer, whose facade drops
  the timestamp — so a cached fix from wherever the farmer last stood could not be
  distinguished from a current one, and would silently pin a node to the wrong place.
- **Soil is reported as DRY/DAMP/WET everywhere, never a percentage.** The probe is blind
  below ~9.5% moisture and saturated above 20%, so a figure outside that window described
  the rail it was pinned to rather than the soil.
- **Command confirmations use the app's own words**, not wire verbs.
