# TODO

## Publish app 1.9.24 to spirit-farm-pi

**Status:** open, 2026-08-30. The dev Pi is already serving it; the farm Pi is not.

`devpi` serves **1.9.24** at `/home/tj/navamesh-updates` (verified: `version.json` correct,
Range request returns 206). `spirit-farm-pi` is still on **1.9.23**, so the phones there
will not see the change that made this build -- node pickers showing each node's name
instead of its Meshtastic hex id.

Deferred only because of the link. It was built at the ISU conference with the Mac on an
iPhone hotspot, and pushing a 91 MB APK over cellular to New Mexico -- the worst link in
the project -- was not worth it when nothing at the farm needed it that night. The APK is
already built and sitting in `dist/`, so this is a single command on a real connection:

```bash
cd navamesh-sideband-wrapper
bash scripts/publish_update.sh pi@spirit-farm-pi          # defaults to /home/pi/navamesh-updates
```

`publish_update.sh` takes the newest APK in `dist/` **by mtime**, so check the filename it
echoes says `1.9.24` -- rebuilding another branch first would silently ship that instead.
Reaching the Pi needs the tailnet hop (`spirit-farm-pi` shell function; it reverts on
exit). Afterwards confirm the farm Pi serves it and still answers a Range request with
**206**, not 200:

```bash
curl -s http://spirit-farm-pi:8090/version.json
curl -s -o /dev/null -w '%{http_code}\n' -r 0-999 \
  http://spirit-farm-pi:8090/navameshfarm-1.9.24-arm64-v8a-debug.apk   # want 206
```

## Restrict control commands to known senders

**Status:** deliberately deferred. Not a blocker for testing or first deployment.

Right now `AUTHORIZED_FARMER_HASHES` is empty, which means **any device that can reach the
gateway over Reticulum may issue control commands** (`ble`, `interval`, `quiet`) and
reconfigure deployed field nodes. This was chosen so the test devices and the first
deployment can drive the mesh without per-device setup.

What actually limits access today is that the verb list lives in the wrapper app. Two
things to know about relying on that:

- The `help` verb prints the control commands to anyone who asks, so the verbs are not
  really secret. Removing them from `HELP_TEXT` for unauthorized senders would be a
  one-line change if that matters.
- The LoRa leg is separately protected by the `navamesh` channel PSK, so this gap is about
  who can reach *the Pi*, not who can talk to the nodes directly.

**To close it:** put each trusted device's RNS identity hash in `AUTHORIZED_FARMER_HASHES`
(comma-separated; find it in Sideband under the device's identity/address) and restart the
`reticulum` container. No code change is needed — the check is implemented in
`is_sender_authorized()` in `src/navamesh/reticulum_bridge.py` and is exercised by
`tests/test_handle_write_command.py`.

Worth doing before: the system carries data anyone outside the project can reach, more
people have Sideband installed than should be able to mute a node, or a node going quiet
unexpectedly would cost real experiment data.

## Make an ignored node visibly ignored

**Status:** closed as won't-do, 2026-08-23. The list is wanted exactly as it is: it holds
the gateways, and one person maintains it and the `.env` it lives in. Revisit only if
someone else starts diagnosing missing nodes, or if node hardware is recycled between
roles again. The context below is kept because the failure it describes will recur then.

Original entry:

`IGNORED_NODES` in `.env` drops a node's DB writes entirely, and does it silently. A node
on that list transmits normally, appears in the Meshtastic app, shows up in the bridge
logs with good RSSI — and never reaches `mesh_nodes`, the map, or the app's node picker.
Nothing anywhere says it is being filtered.

This bit us when a replacement radio happened to have the id `!cfd91e1c`, which was still
on the list from a previous role. The gateway had heard it 18 times in 15 minutes while we
were trying to work out why it "hadn't reported to the Pi."

**To close it:** log once per ignored node id (at INFO, on first sight rather than per
packet) in the path that consults `GATEWAY_NODE_IDS`, and consider surfacing the list in
whatever health view the operator sees. A stronger version distinguishes "ignored on
purpose" from "never heard" in the UI, since those look identical today.

Worth doing before: node hardware gets recycled between roles, or anyone other than the
person who wrote the `.env` has to diagnose a missing node.

## Detect a node that is present but never reports

**Status:** closed 2026-08-23 in f690b0e. `metadata.soil_last_ts` records when soil
specifically was last heard, and `classify_node_health()` in `reticulum_bridge.py` returns
`reporting` / `not_reporting` / `unheard` from that against `last_seen` at 2.5 expected
intervals. Deliberately "no readings in a while" rather than "none ever", so a probe that
dies after a month of good data is caught as well as one that never worked. A Pi-side
query, no radio traffic: a broadcast probe would have been worse, since a CLIENT-role node
answers it correctly — acking is the one thing that role does well.

The written provisioning checklist is still worth having, and note the firmware repo now
records that **flashing does not set the role** at all.

Original entry:

A field node left in `CLIENT` role acks control commands, broadcasts NodeInfo, appears in
the node picker, and reports a healthy link — while never sending a single soil reading.
`!d60add90` sat in exactly that state for its entire life (19 transmissions, 0 readings,
against ~1950/275 for its siblings) and read as an unreliable radio rather than an
unprovisioned one.

Two contributing details worth knowing: the SENSOR role defaults are applied by
`installRoleDefaults()` on role change, so setting the role is genuinely sufficient — but
`EnvironmentTelemetry.cpp` returns early at init when environment telemetry is disabled, so
the sensor list is fixed at boot and a **reboot** is required before readings start. And the
SENSOR default interval is 28800 s (8 h), so a correctly provisioned node still looks silent
for hours.

**To close it:** flag nodes with recent `last_seen` but no telemetry ever (or none in N
intervals) — the Pi already has everything needed to compute that. Pair it with a written
provisioning checklist: region, role SENSOR, channel/PSK, **reboot**, then shorten the
interval for verification.

Worth doing before: any deployment where a node's readings are the experiment, since this
failure is invisible from the app and indistinguishable from a weak link.

## Publish each node's firmware version

**Status:** closed 2026-08-24. Every node now reports its Meshtastic `APP_VERSION` string
three ways: an unsolicited broadcast once at boot (`command_id 0`, `GET_FIRMWARE_INFO`,
jittered 5-35 s so a fleet power-cycle does not collide), on every `NavameshAck`, and on
request via `fwinfo <id|^all>`. The Pi publishes it to `farm/nodes/<id>/firmware` (retained,
its own topic rather than a field on `/info`, which would have nulled a node's display name
on every firmware-only payload) and stores it in `mesh_nodes.metadata->>'firmware_version'`.

Operator surfaces only: `navamesh-cmd fwinfo`, the gateway's `firmware` and `ophelp` verbs,
and the metadata column. Nothing reaches the app, and `test_operator_surface.py` pins that —
`HELP_TEXT` is asserted not to contain "firmware", "version" or "build".

"Never reported" stays distinguishable from a recorded value (NULL, shown as "Not reported
yet") rather than defaulting to something that reads like an answer. Note reboots are not
only reflashes, so hearing a version does not mean a node was just flashed.

Needed a firmware change and therefore had to land before the flash, which it did:
`1f179a7b8`, shipped as `2.7.20.1f179a7`.

Original entry:

Blocked two separate diagnoses on 2026-08-21.

Sharper as of 2026-08-23: a fleet-wide flash is imminent, so a partially-updated fleet is
about to be the normal state and "which nodes still need it" should be a query rather than
a guess. It also pairs with what the firmware repo now records — that **flashing does not
set the role**, so knowing the build a node runs is not the same as knowing it is
provisioned. `classify_node_health()` covers the second half of that; this is the first.

Nothing in the system records what firmware a node runs. Not MQTT, not `mesh_nodes`, not
the bridge logs. When `setloc` came back `ok=False`, the only way to tell "this firmware has
no SET_LOCATION handler" from "the handler rejected this value" was elimination — twice,
because two nodes had been flashed with the wrong build (`459b09e`, which predates
SET_LOCATION, rather than `a36db94`).

The nodes already send this: Meshtastic embeds the build hash in its version string, and
the bridge receives NodeInfo/DeviceMetadata.

**This is NOT a Pi-side-only change, contrary to what this entry used to say.** Checked on
2026-08-23: `meshtastic_User`, which is what NodeInfo carries, has **no version field at
all**. Only `DeviceMetadata` has `firmware_version[18]`, and it is produced in exactly two
places — `PhoneAPI.cpp:300`, the *local* serial/BLE link, and `AdminModule.cpp:1234`, in
reply to an admin-channel `get_device_metadata_request` that needs the session handshake and
`admin_channel_enabled` (false by default). A remote field node never broadcasts its version
over LoRa. Neither of our own protos carries it either: `SoilReading` is `raw_adc`,
`battery_percent`, `battery_mv`.

So closing this needs a **field on the wire**, which means firmware, which means it has to
land **before** a flash rather than after. Planned as Mac-side work (2026-08-24).

**Put it in the ack, not in `SoilReading`.** A version string on every reading costs LoRa
airtime forever; acks are already sent, and any command elicits one, which also turns "which
nodes still need updating" into something the Pi can actively poll rather than wait for. A
4-byte hash rather than the 18-char string would be cheaper again if airtime matters.

**What is already answerable, today, with no changes:** "has this node been flashed?" The
legacy firmware sends a percentage as text; the new firmware sends a `SoilReading` protobuf
with raw ADC, and the Pi only ever populates `soil_raw` from that path. A row with
`soil_percent` set and `soil_raw` NULL has not been flashed — which is exactly what all 18
`spirit-farm-pi` nodes look like right now. That covers the current legacy → new rollout;
what it cannot do is tell one *new* build from another, which is what the next rollout needs.

**Then:** carry `firmware_version` into `farm/nodes/<id>/info` and into the `mesh_nodes`
metadata.

**Surface it to the operator, not to the farmer.** "Wherever the operator picks a node to
command" — as an earlier draft of this put it — reads as the app's node picker, which is the
wrong place: that picker belongs to a farmer who needs DRY/DAMP/WET and has no use for a
build hash, and putting one there re-introduces the protocol-facing surface cd60737 and the
`HELP_TEXT` rewrite removed. The operator's surfaces are `navamesh-cmd`, a query against
`mesh_nodes`, and whatever health view gets built; that is where it belongs.

The node should announce it **unsolicited at boot** rather than answer a request for it —
the value changes only on a reflash and a reflash always reboots, so boot is exactly when it
is worth sending, and nothing has to poll. See the firmware repo's `CLAUDE.md`, including the
jitter needed so a fleet power-cycle does not have 18 nodes broadcasting at once.

Worth doing before: the next fleet-wide flash, where a partially-updated fleet is the
normal state and "which nodes still need it" should be a query rather than a guess.

## Stop replayed packets rewriting a node's recency

**Status:** closed 2026-08-23 in f9d6abf. Needed two layers, not the one this entry
assumed: `apply_payload()` rejects a packet older than the newest already applied **per
payload kind**, and the upsert backstops with `GREATEST` plus a freshness `CASE` on
`lat`/`lon`/`geom`/`metadata`. Without the in-memory half a stale position lands in the
cache and the next genuine packet of any other kind carries it to Postgres under a current
timestamp, passing any SQL check. Per-kind because retained topics all arrive at once on
connect carrying their own original timestamps, so one rule for the whole state would let
the first arrival discard the rest.

Original entry:

The `mesh_nodes` upsert ends `last_seen = EXCLUDED.last_seen` with no guard, and the value
comes from the packet (`state.last_seen_ts`) rather than ingest time. A queued or replayed
packet therefore moves a live node's `last_seen` **backwards** — we watched `!0b9aed49` jump
from 2026-08-21 20:52 back to 2026-08-18 23:42 while it was actively reporting.

**To close it:** `last_seen = GREATEST(mesh_nodes.last_seen, EXCLUDED.last_seen)`, and
decide deliberately whether `lat`/`lon`/`metadata` should be guarded the same way — a stale
replay can currently also overwrite a newer position.

## Age nodes out instead of calling everything online

**Status:** the read half is closed 2026-08-23 in f690b0e; the stored literal remains.

Liveness is now derived at read time by `classify_node_health()`, and the node list flags
stale nodes rather than filtering them — a node the farmer has forgotten is still standing
in their field either way, and hiding it is how it gets forgotten. Each state gets its own
guidance, since telling someone a sensor "will answer commands" when nothing has been heard
from it sends them after the wrong fault.

`metadata->>'status'` is still written as the literal `"online"`, deliberately, because
`Navamesh-Cloud` still selects it. It is documented as write-time-only. Removing it belongs
with the Navamesh-Cloud work below.

Original entry:

Every `mesh_nodes` row carries `"status": "online"` in its metadata regardless of
`last_seen`, including one node unheard since 2026-07-20. Nothing ever transitions a node to
offline, so `last_seen` is the only honest signal and every consumer has to know that.

This is the server half of a problem whose client half is the node picker: with no recency
filter, a month-dead node sits next to live ones in the list the farmer taps to send a
command.

**To close it:** derive status from `last_seen` against the node's expected reporting
interval rather than storing a literal, and filter or visually separate stale nodes in the
picker. Deleting rows is not a fix — retained MQTT and queued replays recreate them.

## Attribute packets that arrive before their NodeInfo

**Status:** closed 2026-08-23 in de04769. `processors/node_id.py` resolves `fromId` →
NodeInfo user id → numeric `from` (masked, since a uint32 nodenum arrives sign-extended on
some paths). The fallback turned out to be in four more places than this entry found —
`link`, `position` and `telemetry` each had their own copy, and `node_info` was dropping
app renames outright rather than falling back. `spirit-farm-pi` has a real `unknown` row
from this, last seen 2026-08-19, which is worth cleaning up when that Pi is next updated.

Original entry:

`src/navamesh/_bridge.py` does `from_id = packet.get("fromId") or "unknown"` in three
places, including the authoritative soil path. `fromId` is resolved from the interface's
node DB, so it is empty for a node that reports telemetry before its NodeInfo arrives — and
that node's readings are then filed under a node called `unknown`, which also appears in the
picker and (with a position) on the map.

It self-heals in the worst possible way: once NodeInfo lands, readings start landing
correctly, so the gap looks like a brief outage rather than data written to the wrong place.

**To close it:** derive the id from the numeric `from` field, which is always present — a
Meshtastic id is just `!` + the nodenum in hex. Keep `"unknown"` only for the case where
even that is missing, which should be never.

## Bound the cloud sync queue

**Status:** deliberately deferred 2026-08-23, and **two of the claims below are wrong** —
read the correction before acting on it.

Deferred because an outage on the deployment Pi gets attended to quickly, so the queue is
not expected to stack up. Measured for the decision: 18 nodes, 16 heard within the hour,
several MQTT topics per report ≈ 90 items/hour ≈ 65,000/month — so the 62,000 figure was
roughly a month of continuous downtime, not a test-bench artifact. `cloud_sync_queue.db` on
`spirit-farm-pi` is 12 KB and last modified in May, so the uplink there has never actually
backed up.

If it is ever picked up, the shape that fits a public historical timeline is a cap high
enough that a realistic outage never reaches it (500k ≈ 7 months, ~150-250 MB of SQLite),
and **downsampling the oldest region** rather than dropping it — one reading per node per
hour keeps the curve continuous and only loses resolution during the outage.

**Corrections.** The queue is far more built out than this entry suggests: `sync_dead_letter`
exists, and the flusher already classifies failures four ways — `DROP_RETENTION` deletes,
`PERMANENT` dead-letters, `UNKNOWN` dead-letters after `CLOUD_QUEUE_MAX_ATTEMPTS` (50), and
connectivity/429/5xx stay queued forever *deliberately*. There is also a guard stopping
Influx from silently no-op'ing a row into deletion while disconnected. What is genuinely
missing is only the size cap.

- *"Each flush attempt replays queued packets through the local write path"* — it does not.
  `_flush()` writes only to `self._pg`/`self._influx`, both cloud handles. On the machine
  where rows reappeared, `PG_CLOUD_DSN` and the local DSN most likely resolved to the same
  database.
- *"Something enqueues cloud pg writes even when `pg_cloud.enabled` is false"* — the only
  `enqueue("pg")` call site is inside `if self.pg_cloud.enabled:`. The real cause is that
  `PostgresWriter._enabled = bool(dsn)` is evaluated **once at construction**, so commenting
  a DSN out of `.env` changes nothing until the container restarts.

**A real, separate bug found while verifying this:** the flusher retries every queued
target regardless of whether that writer is enabled, so a disabled cloud target spins on
its backlog forever — observed at attempt 6199 on the dev Pi, one log line every 30
seconds. No data loss, but it will make the deployment Pi's logs hard to read during the
rollout. Three-line fix: skip a target whose writer is disabled.

Original entry:

With the cloud Postgres unreachable, `sync_queue` grows without limit — there is no
`MAX_QUEUE`, `maxlen`, or equivalent in `mqtt_to_db.py`. Worse, each flush attempt replays
queued packets through the local write path, so deleting rows from `mesh_nodes` was futile
while the backlog drained: rows reappeared seconds later with their original timestamps.

A related bug sits next to it: something enqueues cloud `pg` writes even when
`pg_cloud.enabled` is false, bypassing the guard at the `if self.pg_cloud.enabled:` call
site. Four items queued within a minute of disabling cloud sync entirely.

**To close it:** cap the queue (with a documented drop or dead-letter policy), make a
disabled cloud target skip enqueueing everywhere rather than at one call site, and separate
"replay to cloud" from "rewrite locally" so a backlog cannot mutate local state.

Worth doing before: any deployment where the uplink is intermittent by design, which is
every farm deployment.

## Teach the gateway watchdog about a radio that will not enumerate

**Status:** the reporting half is closed 2026-08-23 in 4a05fcd; the hardware half is open.

The toggle is now verified against a serial port reappearing under `/dev/serial/by-id`
rather than against a fixed sleep — the sysfs device directory deliberately is not the
check, since the 2026-08-20 failure kept a directory while never configuring an interface.
A recovery producing no port logs at error level, says a replug or power cycle is required,
exits non-zero so systemd records it, and counts consecutive failures in `/run` so an
unrecoverable radio reports its own history instead of repeating one line twelve times an
hour. Detection verified against the live gateway.

**Still open, and unreachable from software:** an actual remote recovery path needs a
per-port-switchable USB hub or the RAK's reset line on a Pi GPIO. Worth putting on the
deployment hardware list.

Original entry:

`navamesh-gateway-watchdog` recovers a hung radio by toggling the USB device's `authorized`
flag, which worked for the hangs on 2026-08-17 and 2026-08-18 — those hung the CDC-ACM layer
while the device still configured. On 2026-08-20 the failure was different: the RAK4631
answered descriptor reads but failed `SET_CONFIGURATION` (`error -71`), then stopped
accepting a USB address at all. Re-enumeration cannot reach that, so the watchdog fired every
five minutes for 17 hours and achieved nothing, while logging as though it had acted.

Only a physical replug recovered it.

**To close it:** distinguish "re-enumerated successfully" from "device will not enumerate"
and escalate loudly on the latter — the current failure mode is silent. A remote recovery
path needs hardware: a per-port-switchable USB hub (so `uhubctl` genuinely works) or the
RAK's reset line on a Pi GPIO.

## Items tracked in the other repos

Kept here so they are not lost, though they belong elsewhere:

- **`navamesh-sideband-wrapper`** — the cleartext manifest patch lives in
  `scripts/build_apk.sh`. A build made outside that script (the Linux-VM/CI path in
  `docs/BUILD.md`) silently produces an APK where DownloadManager cannot download at all,
  because `android:usesCleartextTraffic` is missing. Mirror the patch there.
- **`meshtastic-soil-sensor`** — `!0b9aed49` acked `SET_LOCATION ok=True` and persisted the
  position, but kept broadcasting its previous coordinates ~2 km away. Storage path works,
  broadcast path did not follow. Needs reproducing on a second node to tell a firmware bug
  from stale state on that one radio. **Partly addressed 2026-08-23** (`cff0bd52f`): the ack
  now reports the position read back out of the nodeDB instead of echoing the request, so a
  write that does not persist is visible. That catches a *storage* divergence; this case was
  a *broadcast* one, so reproducing it is still the open work.
- **`meshtastic-soil-sensor`** — a SENSOR node with environment telemetry disabled is now
  self-repaired at boot (`cff0bd52f`), which closes the "passes a role check and still
  reports nothing" hole. Worth re-checking after any future rebase on upstream Meshtastic,
  since the hole came from `loadFromDisk()` defaulting `config` and `moduleConfig`
  independently without applying role defaults.

## Stop the test suite from skipping itself into a green CI

**Status:** closed 2026-08-23 in 83ec1cf. `tests/conftest.py` ends any run that skipped a
module with a section naming what did not execute, and `NAVAMESH_REQUIRE_FULL_TESTS=1`
makes it a failed run. Verified in a venv holding only pytest+dotenv: 151 passed / exit 0
without the flag — the exact green-but-empty run — and exit 1 with it.

Writing it surfaced something worse than the documented problem.
`tests/test_handle_write_command.py` had **no collection guard**, and `reticulum_bridge`
raises `SystemExit` rather than `ImportError`; pytest does not treat a SystemExit during
collection as a collection error, so a half-installed environment killed the whole run with
`INTERNALERROR` before any summary could print. Guarded now, like its three siblings.

Original entry:

Every module that imports `reticulum_bridge` guards collection like this:

```python
except (ImportError, SystemExit) as exc:  # rns/lxmf/staticmap/dotenv not installed
    pytest.skip(f"reticulum_bridge unavailable: {exc}", allow_module_level=True)
```

That is the right behaviour on a laptop without the Reticulum stack installed. It is the
wrong behaviour in CI, where it reports success having executed none of the bridge tests
at all — the map labels, the command handling, the wording, the soil formatting. A run
with the dependencies missing is indistinguishable from a run where everything passed,
and the summary line says "passed" either way.

This is not hypothetical. The changes in cd60737 were written and shipped against a suite
that was silently skipping the very files covering them; the gap only surfaced when the
dependencies were installed deliberately and the skip count dropped to zero.

**To close it:** make the skip conditional on something CI cannot accidentally satisfy —
an env var (`NAVAMESH_REQUIRE_FULL_TESTS=1`) that turns the skip into a hard failure, or
a session-scoped conftest check that fails the run when the bridge stack is absent and
the marker says it should be present. Either way CI must distinguish "these tests passed"
from "these tests did not run". Reporting the skip count in the CI summary is the cheap
half-measure if the full fix waits.

Worth doing before: CI becomes the thing anyone trusts instead of a local run, or someone
other than the author starts merging changes to the bridge.

## Decide what the public site shows now that the app shows bands

**Status:** open, and currently inconsistent in production.

There is a fourth repo — **`Navamesh-Cloud`** (`metadavi/Navamesh-Cloud`, branch `main`),
the Flask backend and frontend behind nextg-ag.org. It reads this Pi's Postgres directly
and was not touched when soil moved to DRY/DAMP/WET in cd60737, so the same node now reads
**DAMP** in the farmer's app and **17.7%** on the website.

`api.py` selects `metadata->>'soil_percent' AS soil` for `/api/nodes`, and `/api/history`
maps `"soil" -> ("soil_moisture", "percent")` against InfluxDB. The frontend plots it on a
fixed 0-100 axis (`yDomain:[0,100]`, `yLabel:"Soil Moisture (%)"`) and describes the
sensing as "Soil moisture (%VWC)". `soil_band` appears nowhere in `api.py`, though the
ingestor already writes it alongside `soil_percent`, so the data is there.

The reasoning that removed the percentage from the app applies here unchanged: the probe
is blind below ~9.5% moisture and saturated above 20%, so a 0-100 axis presents resolution
the hardware does not have. A DRY node pinned at raw 4095 plots as a plausible-looking
number rather than as "no reading in the resolvable range".

This is genuinely a decision rather than a bug, which is why it is here and not a patch.
A public research site may reasonably want a continuous series for a time-series chart
even where a farmer wants a word — and a band renders poorly as a line graph. Options,
roughly in increasing effort:

- Leave the chart, relabel the axis honestly (it is %VWC-equivalent within a narrow band,
  not a calibrated 0-100 measure), and show the band on the node popup where a farmer-style
  answer belongs.
- Serve `soil_band` beside `soil_percent` from `/api/nodes` and let the frontend choose.
- Plot the raw ADC for the series (monotonic, honest, no invented calibration) with the
  band as the label.

**To close it:** pick one, and whichever it is, make the map popup on the site agree with
the app. Two farmer-facing surfaces disagreeing about the same node is worse than either
choice.

**Decided 2026-08-23: the third option — plot the raw ADC as the series, with the band as
the label.** It is monotonic, invents no calibration, agrees with the app, and it makes the
site the viewer for the very dataset the eventual calibration will be fitted against.
Implementation belongs to a separate session on that repo, which is not cloned on the
Fedora box.

Two things that session will need. The ingestor now also writes `metadata.soil_last_ts`,
which is what liveness should be derived from — `metadata->>'status'` is a write-time
literal and is only still written because `api.py` selects it; removing it is part of this
work. And note `spirit-farm-pi` currently has **no** `soil_raw`, `soil_band` or
`calibration.py`, because it is on `main`: the raw series only begins once that Pi is
updated and the nodes are reflashed. Nothing is being lost in the meantime, but the
calibration window opens then, not now.

Worth doing before: the site is shown to anyone who would compare it against the app, or
before the percentage is quoted anywhere it might be read as calibrated.
