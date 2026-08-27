# Navamesh — orientation for an agent working across the three repos

Soil sensors on a farm with no internet. RAK4631 radios read a probe, send the raw ADC
over LoRa to a Raspberry Pi gateway, and a farmer drives the whole thing from an Android
app over Reticulum. This file is the map; `TODO.md` is the list of known problems and why
they matter.

## The three repos and how they fit

**Each repo has its own `CLAUDE.md`** covering how to build it, how to test it, and what
bites. Read the one for whichever you are touching; this file is only the map between them.

| GitHub repo | What it is | Its own notes |
|---|---|---|
| `Navamesh` | the Pi: gateway, ingest, map, farmer replies | this file's repo |
| `navamesh-sideband-wrapper` | the Android app (a Sideband fork) | `CLAUDE.md` in that repo |
| `meshtastic-soil-sensor` | the node firmware (a Meshtastic fork) | `CLAUDE.md` in that repo |
| `Navamesh-Cloud` | the public site, nextg-ag.org (Flask + frontend) | — |

**`Navamesh-Cloud` is a fourth consumer that is easy to forget.** It reads this Pi's
Postgres directly, on branch `main` rather than `raw-adc-private-app`, and it is not part
of the mesh path below — which is exactly why a farmer-facing change here can leave it
contradicting the app. It currently still shows soil as a percentage while the app and map
show DRY/DAMP/WET; see `TODO.md`. Check it whenever you change what a reading *means*.

**Local directory names vary by machine and do not match the GitHub names everywhere** —
one machine has them flat under a parent (`Navamesh-Dev/`), another nests the firmware
inside a `Navamesh-Hardware/` folder and renames `Navamesh` to `Navamesh-main`. Identify a
repo by its git remote, not its folder name, and do not assume they are siblings on disk.

The three mesh repos track the branch **`raw-adc-private-app`** (`Navamesh-Cloud` is on `main`). The firmware repo pushes to the
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

### Putting `bin/` on PATH (per machine, not in git)

`bin/firmware` and `bin/navamesh-cmd` are wrappers around `docker exec navamesh_bridge`.
They live in the repo, but **being callable by name is machine-local state that git does
not carry** — a fresh Pi has the scripts and not the symlinks, so `firmware` there is
"command not found" while the file plainly exists. On `devpi` they are installed:

```bash
sudo ln -sf "$PWD/bin/firmware"     /usr/local/bin/firmware
sudo ln -sf "$PWD/bin/navamesh-cmd" /usr/local/bin/navamesh-cmd
```

Run from the repo root. The symlinks point at the working tree, so a `git pull` updates
the commands; moving the clone breaks them visibly rather than silently.

The caller also needs to be in the `docker` group (`sudo usermod -aG docker <user>`, then
log out and back in). Both scripts detect that case and say so, because docker's own error
for it is about a socket and reads like the container is down.

### A backwards clock silently mutes the whole fleet

`command_id` is the firmware's replay guard, and a node **silently drops** any command at
or below the last id it accepted — no ack, no error, and a retry carries a similarly low
id, so the state cannot be retried out of and reads exactly like a dead radio.

The id was a bare unix timestamp, which is monotonic only while the clock is. A Pi has no
guaranteed RTC and a farm gateway may have no NTP, so one that loses power for a week comes
back believing it is a week ago — and from then on **every node it has ever commanded
ignores it**, permanently, with nothing in any log on the Pi to say why.

Closed 2026-08-24: both `_next_cmd_id()` and `navamesh-cmd` now take their floor from
`max(cmd_id)` in `command_log` rather than trusting the clock. Note the cast — `cmd_id` is a
**TEXT** column, and lexicographic `max()` puts `"999999999"` above `"1787612909"` while
being numerically smaller, i.e. it fails in exactly the case the guard exists for.

`devpi` has an RTC and working NTP, so it was never exposed. **Check `spirit-farm-pi`**
(`ls /dev/rtc0`, `timedatectl`) — the farm has no internet, which is the deployment this
would actually happen to.

## Traps that have already cost real time

**The test suite used to skip itself into a green run.** Closed in 83ec1cf, but know
how: without `rns/lxmf/staticmap/dotenv`, every bridge test module skips at collection
and the summary still said "passed". `tests/conftest.py` now ends any such run with a
section naming what did not execute, and `NAVAMESH_REQUIRE_FULL_TESTS=1` makes it a
failed run — set that in CI and before shipping to a Pi. A correct run is **356 passed,
0 skipped** (2026-08-26; it was 296 when this was written, so trust "0 skipped" over the
count).

**There is no pytest on `devpi` — not anywhere.** Not in `~/Navamesh/.venv`, not in the
system python, not in the `navamesh-*` images. The stale `~/Navamesh/.pytest_cache` makes
it look otherwise. Installing it on the host does not help either: the suite needs
`rns`/`lxmf`/`staticmap`/`dotenv`, which exist only inside the images. Run it in a
throwaway container off an already-built image, with the repo mounted:

```bash
cd ~/Navamesh && docker run --rm --network host --env-file .env \
  -v /home/tj/Navamesh:/repo -w /repo navamesh-reticulum \
  sh -c "pip install -q pytest; NAVAMESH_REQUIRE_FULL_TESTS=1 python3 -m pytest -q"
```

`--rm` throws the pytest install away, so nothing on the Pi is mutated.

Note `reticulum_bridge` raises **`SystemExit`**, not `ImportError`, when its deps are
missing, and pytest does not treat a SystemExit during collection as a collection error.
A module importing it without a guard takes the entire run down with `INTERNALERROR`
before any summary prints. All four bridge test modules are guarded; keep it that way.

**The Pi's `.env` is test-bench configuration, not deployment configuration.** It is
gitignored, so it cannot leak into a deployment — but do not copy it around or treat it as
canonical. On the test Pi the cloud targets (`PG_CLOUD_DSN`, `INFLUX_CLOUD_*`) are
commented out deliberately, because a test node's readings should not reach the cloud.
**On `spirit-farm-pi` they must stay enabled** — that Pi is the live public demonstration
and its cloud writes are the point.

Its `CACHE_*` box is also still around the **FAU farm** (26.370–26.382, −80.104 to
−80.092), so development radios anywhere else are outside offline map coverage. That is
not a bug; it is why `map` falls back to OSM on the dev bench.

**`PostgresWriter.enabled` is evaluated once, at construction** (`_enabled = bool(dsn)`).
Commenting a DSN out of `.env` changes nothing until the container restarts — which is the
real explanation for "four items queued within a minute of disabling cloud sync". Related
and still open: the sync flusher retries every queued target regardless of whether that
writer is enabled, so a disabled cloud target spins on its backlog forever (observed at
attempt 6199 on the dev Pi). Log noise, not data loss.

**`IGNORED_NODES` filters silently.** A node on that list transmits normally, appears in
the Meshtastic app and the bridge logs, and never reaches the database or the app's node
picker. Nothing logs that it is being dropped. A replacement radio that reused a retired
id looked broken for an hour because of this.

**A node in the wrong role looks perfectly healthy.** `CLIENT` acks commands, broadcasts
NodeInfo and sits in the picker while never sending a single reading. Provisioning order
that works: region → role `SENSOR` → channel/PSK at **index 1** → **reboot** → shorten the
interval. The reboot is required: `EnvironmentTelemetry.cpp` returns early at init when
environment telemetry is off, so the sensor list is fixed at boot.

**Flashing does not set the role** — the config lives in LittleFS and a DFU flash does not
erase it, so a node keeps whatever role it had. See the firmware repo's `CLAUDE.md`. The Pi
can now *detect* this state rather than only suffering it: `classify_node_health()` returns
`not_reporting` for a node with a recent `last_seen` and no recent soil reading, which is
exactly the CLIENT-role signature.

**Channel index matters.** `navamesh` must be a *secondary* channel at index 1; the Pi
sends commands there (`PRIVATE_CHANNEL_INDEX=1`). A node with only the default channel
still shows up — the default PSK is public — while being deaf to every command.

**Nothing records a node's firmware version**, so "this build lacks the handler" and "the
handler rejected the value" look identical from the Pi. Meshtastic embeds the build hash
in its version string, visible over serial or BLE (`2.7.20.a36db94`). Still open — see
`TODO.md`.

**A missing ack is not a failed command, and RSSI is how you tell the two apart.**
Measured 2026-08-23/24: `spirit-farm-pi`'s 18 field nodes sit at **-65 to -90 dBm, SNR
5.0-7.5** — a healthy LoRa link, usable range being roughly -40 to -120. The dev bench sat
at **-10 to -16 dBm**, i.e. three nodes and the gateway on one desk, and *that* is where
commands and acks were dropped intermittently in both directions: one broadcast was applied
on the node (confirmed over serial) while its ack never arrived, and two unicasts never
reached the node at all. It flapped rather than settled — a `setloc` succeeded and the next
command 30 s later did not.

Too *strong* a signal, not too weak: near-field desense plus collisions from radios inches
apart. So do not read bench ack loss as a mesh problem, and do not read a single timeout as
a failure. `setloc` is the one command with no broadcast fallback, so it is the most exposed;
retry it, and confirm with `position` or `map <id>` rather than inferring from the silence.
Note also that RSSI here is what the gateway *hears from* a node — the uplink. Commands go
the other way, and the two directions can drop independently.

**`metadata->>'status'` is write-time only and must not be read as current.** A row is
only rewritten when that node is heard from, so a node that stops reporting keeps whatever
was stored last. Derive liveness from `last_seen` and `soil_last_ts` via
`classify_node_health()`. The key is retained solely because `Navamesh-Cloud` still selects
it.

**`->` on a JSON null yields the jsonb literal `'null'`, not SQL NULL.** So `COALESCE`
happily chooses it and a carry-forward silently does nothing. `NULLIF(x, 'null'::jsonb)`
is load-bearing in the `mesh_nodes` upsert for exactly this reason. It was caught by
running the statement against the real schema, not by reading it — worth doing for any
change to that upsert, in a `BEGIN; … ROLLBACK;` against the dev Pi's PostGIS.

**Reaching the dev Pi is fiddly and the failures look like different problems.** Its
DHCP lease moves (`192.168.1.114` → `.153` mid-session on 2026-08-23), and
`raspberrypi.local` is not a reliable substitute: avahi answers it with an **IPv6**
address while `/etc/nsswitch.conf` uses `mdns4_minimal` (IPv4 only) followed by
`[NOTFOUND=return]`, so `getaddrinfo` dead-ends before ever trying DNS. It appears to
work whenever an A record happens to be cached, then stops. The Fedora box's
`~/.ssh/config` has a `devpi` host pointing at the current address; a DHCP reservation
or putting the Pi on the tailnet is the real fix.

The stored **host key is under `raspberrypi`**, so any other name needs
`-o HostKeyAlias=raspberrypi` (already trusted) or a one-time
`ssh-keygen -R raspberrypi.local`.

## Conventions worth matching

Comments here explain **why**, especially where the obvious approach was tried and
rejected. Several are load-bearing: the proto file records why portnums 258/259 are
separate, `applySetLocation()` records why it mirrors AdminModule rather than calling it,
and `applyTelemetryInterval()` records why it avoids `reloadConfig()`. Do not compress
those into restatements of the code.

`TODO.md` entries are open questions with a **Status**, the context that produced them,
and what closing them would take — not a bullet list of tasks. Match that shape.

## Recent work (Aug 2026)

Ends at: app **1.9.23**, Pi `eefb8e2`, firmware `2133f6a42` (fleet flashed with
`7d66535` / `2.7.20.7d66535`).

**`main` and `raw-adc-private-app` are level again** for this repo and the app, as of
2026-08-26 — both fast-forwarded, nothing divergent. That was the long-standing
precondition for `spirit-farm-pi` (which is on `main`) to pull any of this. The firmware
stays on its fork branch.

### 2026-08-26 — a demo bench, and three silent failures found by measuring

The dev Pi was cut down to two nodes for a conference demo: **`!061d8b62` = Node A**,
**`!79d4bb41` = Node B**. Everything below was verified on that bench.

**Clearing a node out of the database means clearing its retained MQTT topics too.**
Deleting the `mesh_nodes` rows alone does nothing lasting: the ingestor replays every
retained `farm/nodes/<id>/*` and `farm/sensors/soil/<id>/*` topic on its next connect and
puts the rows straight back. Order that works: stop the ingestor, publish an empty
retained message to each of that node's topics, delete the rows, restart, confirm they
stay gone. `command_log` was deliberately left alone — it is invisible to the app, and it
is where `_next_cmd_id()` reads its replay-guard floor.

**`map <id>` drew a *wider* view than `map` did of the same nodes**, with a pin big
enough to hide the answer: measured 1233 m across with a 111 m pin, against 514 m and a
19 m pin for plain `map`. Two independent causes, either alone enough. The extent was
locked to the whole cached farm box for any bounded render, and the only command that
renders bounded-with-highlight is `map <id>` — so the guard applied exclusively to the one
case where a farm-wide view is useless. And pin radii went to StaticMap in *render-space*
pixels while `render_size` is not constant (576 for a fixed-extent render, 960 for a
node-centered one) before being thumbnailed to `max_dimension`, so the same nominal radius
came out ~2.4x larger on one path. Radii are now in delivered pixels and scaled by
`render_size/max_dimension`; 9 px reproduces exactly what `map` always drew, so that
output is unchanged. `_best_zoom()` returning 17 for a single point put the same gap back
on the node-centered path (the one taken outside the cache box, i.e. anywhere the kit is
carried), so both paths now share `MAP_SINGLE_NODE_ZOOM`.

**`MAP_SINGLE_NODE_ZOOM`** (default 18) is new in `.env`. `CACHE_ZOOM_MIN/MAX` could not
express this: they say which zooms the offline cache *holds*, so they bound the view but
cannot ask for one. It applies on `docker compose up -d reticulum` with no rebuild, which
is the point — it is a knob for someone standing in a field. Out of range clamps to 1..19
with a warning rather than being refused, because past 19 every tile 404s and the map
arrives as text, which reads as a broken gateway rather than a bad setting.

**Every text reply said "Node bb41" while the names sat in the database.** `_fmt_node()`
takes a node id and nothing else, so it could only ever render hex — and `fmt_status`,
`fmt_soil`, `fmt_battery`, `fmt_position`, `fmt_link` and `fmt_firmware` all called it
while holding the `NodeSnapshot` that had the name in it. They call `_node_label(node_id,
snap)` now. That helper also prefers the **long** name where it used to prefer
`display_name`; the ingestor sets `display_name = short_name or long_name`, so preferring
it meant the short name whenever one existed, and a farmer who named a sensor "North
Field" got "NF01". Map pins still take the short name and still truncate — a pin has a map
to share, a line of text has room. One test pins both directions.

**The `nodes` reply is a wire format, not a layout.** The app's `parse_nodes_reply()`
keeps each line starting with `!` **whole** and uses it as the node id, so the health flag
appended to that line became part of the id: a stale node's picker entry was literally
`!79d4bb41  ⚠️ no readings in 2h`, and every command aimed at it went to that string. The
id now sits alone on its line and everything else goes on a `·` continuation line beneath,
which that parser drops. The `·` also guarantees the continuation can never be mistaken
for an id line, including for a node whose long name begins with `!`. Verified against the
app's own parser, flagged and unflagged. **An unchanged app fixes itself when the Pi is
deployed — no APK rebuild.**

**Still open: `processors/position.py` throws away `precisionBits`.** The node transmits
it on every position packet and nothing stores it, which is why a position blurred to a
~5.8 km grid by a channel's `position_precision` was indistinguishable from "setloc never
applied" — see the firmware repo's `CLAUDE.md` for the full diagnosis. Storing it and
warning when it is below 32 would turn an invisible 3 km error into a sentence.

### 2026-08-23 — silent-failure sweep, all verified on the dev Pi

Nothing in the app changed, so **no APK rebuild or OTA is needed for any of it**. The
`help` text and every farmer-facing reply are rendered by the Pi; the app only sends the
verb and displays what comes back.

- **Packets are attributed by nodenum, not by a name that may not have arrived.**
  `fromId` is filled in by the Meshtastic library from its own node DB, so it is empty for
  any node transmitting before its NodeInfo is heard — normal after a gateway restart.
  Those readings were filed under `unknown`, which also sat in the picker and put a pin on
  the map. `processors/node_id.py` resolves `fromId` → NodeInfo user id → numeric `from`
  (masked; a uint32 nodenum arrives sign-extended on some paths). The deployment Pi has a
  real `unknown` row from this. The same fallback was in four more places than the TODO
  said, and `node_info` was dropping app renames outright.
- **A replayed packet can no longer rewrite recency or position.** Two layers, because
  either alone leaves the hole open: `apply_payload()` rejects a packet older than the
  newest already applied **per payload kind** (per-kind because retained topics all arrive
  at once on connect with their own original timestamps, so one rule for the whole state
  would let the first arrival discard the rest), and the upsert backstops with `GREATEST`
  plus a freshness `CASE` on `lat`/`lon`/`geom`/`metadata`. Without the in-memory half, a
  stale position lands in the cache and the next genuine packet of any other kind carries
  it to Postgres under a current timestamp.
- **A node that is gone reads differently from one present but not measuring.**
  `metadata.soil_last_ts` records when soil specifically was last heard — `last_seen`
  moves on any packet, which is why it could never see this — and
  `classify_node_health()` returns `reporting` / `not_reporting` / `unheard` at 2.5
  expected intervals (`NODE_EXPECTED_INTERVAL_SECONDS`, `NODE_STALE_INTERVALS`). "No
  readings in a while", not "none ever", so a probe that dies after a month is caught too.
  Stale nodes are **flagged in the node list, not filtered out of it** — one still
  standing in a field is worse forgotten — and each state gets its own guidance.
- **`map <id>` no longer refuses what `map` would draw.** Only `map <id>` consulted the
  `CACHE_*` box, so the same sensor was mapped or not depending on which button was
  pressed, while `map` fetched those very tiles from `MAP_TILE_FALLBACK`. Order is now
  offline cache → fallback tile server → text, for in-bounds and out-of-bounds nodes
  alike. Strict offline behaviour is expressed by leaving `MAP_TILE_FALLBACK` empty — a
  property of the link, not of which command was typed. Also revived the
  "outside offline map coverage" warning, whose counter nothing had ever incremented.
- **`help` speaks the farmer's language**, pinned by tests to `VERB_LABELS` so the
  buttons, the confirmations and the help text cannot be reworded apart again.
- **A run that skipped the bridge tests says so, and CI can refuse it** —
  `tests/conftest.py` plus `NAVAMESH_REQUIRE_FULL_TESTS=1`.
- **The gateway watchdog no longer reports success for a radio it cannot fix.** It now
  verifies a serial port came back under `/dev/serial/by-id`, escalates at error level
  with a consecutive-failure count in `/run`, and exits non-zero. The hardware half — a
  per-port-switchable hub or the RAK reset line on a GPIO — is still open.

### Where this leaves the deployment

Heads: `Navamesh` `51cb0f5`, `navamesh-sideband-wrapper` `884a3ec` (**untouched**),
`meshtastic-soil-sensor` `cff0bd52f`.

**Two Pis, and only one is safe to touch.** `raspberrypi` / `devpi` (`tj@`, on the LAN) is
the test bench and is where all work happens. **`spirit-farm-pi` (`pi@`, on the tailnet, in
Navajo, New Mexico) is live and READ ONLY** — it feeds the public site and is receiving real
field data. Read it for context freely; change nothing on it without saying so first.

`spirit-farm-pi` is on branch **`main`** and has **native Postgres and InfluxDB** under
systemd rather than containers — deliberate, so a second farm's Pi keeps its own data.
Its cloud writes are always on. **The merge that was planned here happened on 2026-08-26**:
`raw-adc-private-app` was fast-forwarded into `main` for both `Navamesh` and
`navamesh-sideband-wrapper` and pushed, so that Pi can now pull `main` and get everything.
The firmware stays on its fork branch. The schema changes are `ON CONFLICT` clauses and
metadata keys, so no migration is needed.

**Read "native Postgres and InfluxDB" narrowly: it means the databases.** The Navamesh
services themselves — `navamesh_bridge`, `navamesh_ingestor`, `navamesh_reticulum`,
`navamesh_tiles` — are Docker there exactly as on `devpi`, building from the same
`docker-compose.yml`. So deploying is `docker compose build` + `up -d`, not a systemd
restart. Its update path is `/home/pi/navamesh-updates`, not `/home/tj/...`.

**Deployed there 2026-08-26** (first pull since `638b12a`, taking it to `719f357`):

- Rebuilt and restarted **only `reticulum`**. `bridge` owns the serial radio and
  `ingestor` owns the MQTT→DB path, so leaving both running means no gap in field data;
  nothing outside the reply/map path changed anyway. The gateway keeps its LXMF identity
  across the restart (`/home/pi/.reticulum` is a bind mount), so paired phones stay paired.
- The suite runs there the same way it does on `devpi` — throwaway container, **356 passed**.
- **All 18 nodes report no `firmware_version` at all**, i.e. none has ever acked a command:
  they predate the NavameshCommand module. `setloc` and every other write verb will simply
  time out on that fleet until it is reflashed. Plan around it.
- **Their coordinates were therefore written straight into `mesh_nodes`** (lat/lon plus
  PostGIS `geom`), from a surveyed list, matched on `long_name` so a wrong id could not
  silently mis-place a sensor. That is safe to do precisely because these nodes never send
  position: the upsert only touches `lat`/`lon`/`geom` on a packet that carries
  coordinates, so battery/link/info traffic leaves them alone. Once the fleet is reflashed,
  prefer `setloc` — it puts the position on the node, where it belongs.
- All 18 sit inside the farm `CACHE_*` box with z14-19 tiles present, so maps render fully
  offline. Confirmed: `map` plots 18, `map <id>` plots 1 at the same scale.
- **`soil_last_ts` and `soil_raw` are NULL for the whole fleet**, so `classify_node_health()`
  flags every node "no soil readings recently" and the map draws `?` for soil — while soil
  is in fact arriving (351 points into InfluxDB in 24 h). The legacy percent path feeds
  Influx but not the node metadata the flag reads. Battery, voltage and link resolve
  normally, so the nodes are genuinely up. Expected to clear itself when the fleet moves to
  the raw-ADC firmware; until then do not read those warnings as dead sensors.
- Its `/home/pi/Navamesh` working tree carries ~22 untracked junk files (`[bridge]`,
  `exporting`, `transferring`, influx tarballs, an `mqtt_to_db.py.bak`) from a word-split
  build command. Harmless, and none collide with tracked files, so pulls are unaffected.
- To close the open question above: that Pi **has** `/dev/rtc0` and NTP is active and
  synchronized, so the backwards-clock `cmd_id` trap does not apply to it.

### Verified end to end on the bench, 2026-08-23

Three RAK4631s flashed with `cff0bd52f`, driven from the real app on a moto g play over
adb. All five commands round-tripped app → LXMF → Pi → MQTT → LoRa → node → ack → app:
`interval` (300 s applied), `setloc` from the phone's live GPS (±10 m, accepted by
`is_fresh_fix()`), `quiet` on and off, and `ble` from an earlier run. Read commands all
answered: `help`, `nodes`, `status`, `map`, `map <id>`.

Notable confirmations:

- **`map <id>` on an out-of-bounds node produced an image** — 27,024 bytes, 480×480,
  OSM-sourced — where it used to refuse. That was the reported bug.
- **`setloc` persisted, re-broadcast and ingested.** The bridge logged
  `sent … (lat=26.2849999, lon=-80.2743025)` then `ack … ok=True`, and `mesh_nodes` holds
  exactly those coordinates. So `!0b9aed49`'s storage-vs-broadcast divergence did not
  reproduce, and the new freshness guard on `lat`/`lon` does not block a legitimate change
  — worth knowing, since a guard that froze positions would look identical to a working one
  until someone moved a node.
- **The node picker had no `unknown` entry**, and all three nodes classified `reporting`.
- **A wording bug the bench caught that no test could have.** `quiet on` acked
  `applied=1440` — one day — while the app and the help text both promised "within 3 days".
  4320 is only the firmware's clamp ceiling; 1440 is the default the app triggers by sending
  no duration. Both texts now say "after a day", and a test pins it.

**Both nodes were left at a 300-second reporting interval** for the test. That is 96× the
SENSOR default and will not do for anything battery-powered — set them back with
`./bin/navamesh-cmd interval <id> 28800` before they matter.

One thing the bench cannot prove: with request and stored value agreeing, an echoing ack and
a reading-back ack are identical by construction. The read-back is confirmed present in the
flashed binary (its log strings are in the ELF) and only becomes *observable* when a node
disagrees, which is the point of it.

### Built on the Mac, 2026-08-24

Both landed together in one firmware/Pi/app change; the firmware half had to precede the
fleet flash and did (`1f179a7b8`, `2.7.20.1f179a7`).

**Firmware version reporting.** A node announces its build unsolicited at boot, carries it on
every ack, and answers `fwinfo <id|^all>` on demand. Full details in the firmware repo's
`CLAUDE.md`; what matters here is the Pi half:

- `_bridge.py` republishes any ack's version to `farm/nodes/<id>/firmware`, retained.
  **Its own topic, deliberately not a field on `/info`** — the ingestor's `info` branch
  assigns `long_name`/`short_name`/`display_name` unconditionally, so a firmware-only payload
  there would null a node's display name, and a NodeInfo packet would null the firmware back.
  Separate topics also get separate staleness timestamps.
- `mqtt_to_db.py` stores it in `mesh_nodes.metadata->>'firmware_version'`, guarded so a
  retained redelivery cannot blank a version already recorded.
- **Operator surfaces only, and the census is Pi-side.** On the Pi, plain **`firmware`**
  is the fleet view — a database read from inside the bridge container, no radio traffic, no phone
  involved. `navamesh-cmd fwinfo <id>` asks one node directly. The gateway also answers
  `firmware` and `ophelp` over LXMF for someone holding a phone instead of a terminal, but
  that path needs free-text messaging, which **the farm app deliberately does not offer to a
  gateway** — tapping a gateway opens the button dashboard, and `peer_chat` is for non-gateway
  peers. So the terminal is the real surface; treat the LXMF verbs as the fallback. It is deliberately absent from `HELP_TEXT` and
  from `VERB_LABELS` — that dict is the farmer's vocabulary and `test_farmer_wording` pins
  everything in it into the help text, so adding a verb there is a promise to show a farmer a
  build hash. `OPERATOR_VERB_LABELS` exists so those verbs still read well in outcomes.
- "Never reported" stays NULL and renders as "Not reported yet". `soil_raw IS NULL` remains
  the separate, independent answer to "flashed at all".

**The farmer's `help` carries no wire syntax.** Removed 2026-08-24: a farmer taps buttons
and never types a command, so `ble <id|^all> <minutes>` asked them to parse a format to use
their own sensor. It is not lost — `OPERATOR_HELP_TEXT` (reachable as `ophelp`) now carries
every typed form, and `test_farmer_wording` asserts both halves: absent from `HELP_TEXT`,
present in `OPERATOR_HELP_TEXT`. That test previously asserted the opposite, so read its
docstring before "restoring" anything.

**Reply text must fit 43 columns.** The app renders replies in a monospace Label that wraps
at ~44 on the deployed handsets. Over-wide lines wrap to column 0 and collide with the
6-space continuation indents, which reads as corruption rather than as wrapping — that is
what the help text looked like on a phone before 2026-08-24. Pinned by
`test_help_text_fits_the_phone_without_wrapping`.

**Units on the reporting interval.** `interval <id|^all> 30m` and `2h` now work alongside bare
seconds, which still mean seconds — the app, `navamesh-cmd` and anything scripted are
unchanged. `parse_interval_value()` lives in `processors/command_proto.py` rather than
`reticulum_bridge.py` so `navamesh-cmd` can use it without importing the RNS/LXMF stack. The
unit is applied *before* the bounds check, so a suffix cannot smuggle a value past a bound the
equivalent number of seconds would fail. Out-of-range is refused, never clamped, and the error
echoes both what was typed and what it came to.

The app gained "Enter a time" beside the interval presets — a number plus a Minutes/Hours
button, validated against the same `value_min`/`value_max` a preset would have been. The wire
string is unchanged (`interval <id> <seconds>`), so neither the Pi nor the firmware had to
learn a new format. Bounds stay triplicated: UI, `command_proto.py`, firmware clamp.

### Earlier in Aug 2026

- **`SET_LOCATION`** — set a node's fixed position from the phone's GPS over LoRa, since
  the nodes have no receiver of their own. Verified end to end on three nodes. The ack now
  reports the position read back from the nodeDB rather than echoing the request — see the
  firmware repo.
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
