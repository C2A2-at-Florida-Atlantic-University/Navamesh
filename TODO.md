# TODO

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

**Status:** open. Cost real debugging time on 2026-08-21.

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

**Status:** open. This is the failure that looked like broken hardware.

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

**Status:** open. Blocked two separate diagnoses on 2026-08-21.

Nothing in the system records what firmware a node runs. Not MQTT, not `mesh_nodes`, not
the bridge logs. When `setloc` came back `ok=False`, the only way to tell "this firmware has
no SET_LOCATION handler" from "the handler rejected this value" was elimination — twice,
because two nodes had been flashed with the wrong build (`459b09e`, which predates
SET_LOCATION, rather than `a36db94`).

The nodes already send this: Meshtastic embeds the build hash in its version string, and
the bridge receives NodeInfo/DeviceMetadata.

**To close it:** carry `firmware_version` into `farm/nodes/<id>/info` and into the
`mesh_nodes` metadata, and show it wherever the operator picks a node to command.

Worth doing before: the next fleet-wide flash, where a partially-updated fleet is the
normal state and "which nodes still need it" should be a query rather than a guess.

## Stop replayed packets rewriting a node's recency

**Status:** open. Observed corrupting live data.

The `mesh_nodes` upsert ends `last_seen = EXCLUDED.last_seen` with no guard, and the value
comes from the packet (`state.last_seen_ts`) rather than ingest time. A queued or replayed
packet therefore moves a live node's `last_seen` **backwards** — we watched `!0b9aed49` jump
from 2026-08-21 20:52 back to 2026-08-18 23:42 while it was actively reporting.

**To close it:** `last_seen = GREATEST(mesh_nodes.last_seen, EXCLUDED.last_seen)`, and
decide deliberately whether `lat`/`lon`/`metadata` should be guarded the same way — a stale
replay can currently also overwrite a newer position.

## Age nodes out instead of calling everything online

**Status:** open.

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

**Status:** open. Causes real measurement misattribution.

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

**Status:** open. Reached 62,000 items and was still growing.

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

**Status:** open. The watchdog reported success for ~17 hours while the radio was dead.

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
  from stale state on that one radio.
