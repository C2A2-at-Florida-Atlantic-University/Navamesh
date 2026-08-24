"""
cmd_cli.py — Send a control command to a field node from a terminal, and wait for its ack.

    navamesh-cmd ble      !0b9aed49 15     # Bluetooth window, 15 minutes
    navamesh-cmd interval !0b9aed49 300    # report every 300s, applied live
    navamesh-cmd quiet    !0b9aed49 on     # stop transmitting (receiver stays on)
    navamesh-cmd quiet    !0b9aed49 off    # resume
    navamesh-cmd ble      ^all 30          # every field node at once
    navamesh-cmd setloc   !0b9aed49 36.0721 -109.0450   # set its fixed position
    navamesh-cmd interval !0b9aed49 30m    # same, in minutes; 2h also works
    navamesh-cmd fwinfo   !0b9aed49        # what build is it running? changes nothing
    navamesh-cmd fwinfo   ^all             # ...across the fleet

Nodes announce their build unprompted at boot, so the Pi usually knows it already --
`fwinfo` is for asking again without waiting for a reboot.

This publishes to the MQTT command topic rather than opening the radio directly: the
bridge process owns the serial port, and two processes cannot share it. The bridge does
the transmitting, the node acks over LoRa, and the outcome comes back on the status topic,
which is what this waits for.

Exit status is 0 only if the node actually acknowledged, so this is safe to use in a
script: a command that was transmitted but never answered exits non-zero.
"""

import argparse
import json
import sys
import time
from typing import Optional

import paho.mqtt.client as mqtt

from navamesh import topics
from navamesh.config import load_config
from navamesh.processors.command_proto import (
    BLE_WINDOW_MAX_MINUTES,
    BLE_WINDOW_MIN_MINUTES,
    INTERVAL_MAX_SECONDS,
    INTERVAL_MIN_SECONDS,
    QUIET_MAX_MINUTES,
    QUIET_MIN_MINUTES,
    UNICAST_ONLY_VERBS,
    CommandValidationError,
    encode_command,
    parse_interval_value,
)

# How long to wait for the node to answer. The firmware jitters acks by up to 4s to keep 18
# nodes from replying at once, and a LoRa round trip at SF11 is not instant, so this is
# deliberately generous.
DEFAULT_TIMEOUT_SECONDS = 60


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="navamesh-cmd",
        description="Send a control command to a Navamesh field node and wait for its ack.",
        epilog=(
            "Targets: a node id like !0b9aed49, or ^all to reach every node.\n"
            f"^all is refused for: {', '.join(UNICAST_ONLY_VERBS)}.\n"
            f"Bounds: ble {BLE_WINDOW_MIN_MINUTES}-{BLE_WINDOW_MAX_MINUTES} min, "
            f"interval {INTERVAL_MIN_SECONDS}-{INTERVAL_MAX_SECONDS} s, "
            f"quiet duration {QUIET_MIN_MINUTES}-{QUIET_MAX_MINUTES} min."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("verb", choices=["ble", "interval", "quiet", "setloc", "fwinfo"])
    p.add_argument("target", help="node id (!hex), or ^all except for setloc")
    # Optional because fwinfo takes none -- it asks a question rather than setting anything.
    # _parse_value still requires one for every other verb, so a forgotten value is caught
    # with a message about that verb rather than by argparse.
    p.add_argument("value", nargs="?", default=None,
                   help="minutes / seconds (or 30m, 2h) / on|off / latitude; none for fwinfo")
    # setloc is the only verb taking two values. A bare negative longitude is safe as a
    # positional: argparse treats -109.045 as a number rather than a flag, because this
    # parser defines no option that looks like a negative number.
    p.add_argument("value2", nargs="?", default=None,
                   help="longitude (setloc only)")
    p.add_argument("--quiet-minutes", type=int, default=None,
                   help="quiet on: auto-resume after this many minutes (default: node's own 24h)")
    p.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS,
                   help=f"seconds to wait for the ack (default {DEFAULT_TIMEOUT_SECONDS})")
    p.add_argument("--no-wait", action="store_true",
                   help="publish and exit without waiting for the ack")
    return p


def _parse_value(verb: str, raw: str, raw2: Optional[str] = None):
    """
    Return (value, quiet_on, lat, lon) for the given verb, or raise CommandValidationError.

    Only setloc uses lat/lon, and only setloc uses raw2.
    """
    if verb == "fwinfo":
        if raw is not None:
            raise CommandValidationError("fwinfo takes no value")
        return None, None, None, None
    if raw is None:
        raise CommandValidationError(f"{verb} needs a value")
    if verb == "quiet":
        v = raw.strip().lower()
        if v not in ("on", "off"):
            raise CommandValidationError("quiet takes 'on' or 'off'")
        return None, v == "on", None, None
    if verb == "setloc":
        if raw2 is None:
            raise CommandValidationError("setloc takes a latitude and a longitude")
        try:
            return None, None, float(raw), float(raw2)
        except ValueError:
            raise CommandValidationError(
                f"{raw!r} {raw2!r} is not a latitude and longitude in decimal degrees"
            )
    if raw2 is not None:
        raise CommandValidationError(f"{verb} takes one value, got two")
    if verb == "interval":
        # Accepts "1800", "30m", "2h". Bare numbers stay seconds, so nothing scripted
        # against this tool changes meaning.
        seconds, error = parse_interval_value(raw)
        if error:
            raise CommandValidationError(error)
        return seconds, None, None, None
    try:
        return int(raw), None, None, None
    except ValueError:
        raise CommandValidationError(f"{raw!r} is not a number")


def main(argv: Optional[list] = None) -> int:
    args = _build_parser().parse_args(argv)
    cfg = load_config()

    target = args.target.strip()
    if target.lower() in ("all", "^all"):
        target = "^all"

    try:
        if target == "^all" and args.verb in UNICAST_ONLY_VERBS:
            raise CommandValidationError(
                f"{args.verb} must name one node -- ^all would give every node the same position"
            )
        value, quiet_on, lat, lon = _parse_value(args.verb, args.value, args.value2)
        if args.verb == "quiet" and quiet_on and args.quiet_minutes is not None:
            value = args.quiet_minutes
        # Validate here rather than letting the bridge reject it later, so a bad value is
        # reported before anything is transmitted. This is the same encoder the bridge uses.
        cmd_id = int(time.time())
        encode_command(args.verb, cmd_id, value=value, quiet_on=quiet_on, lat=lat, lon=lon)
    except CommandValidationError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    status_topic = topics.cmd_status(cfg.root_cmd)
    seen = []

    client = mqtt.Client()
    client.on_connect = lambda c, u, f, rc: c.subscribe(status_topic, qos=1)

    def on_message(c, u, msg):
        try:
            seen.append(json.loads(msg.payload.decode("utf-8")))
        except Exception:
            pass

    client.on_message = on_message
    client.connect(cfg.mqtt_host, cfg.mqtt_port, 60)
    client.loop_start()
    # Subscribe before publishing, or a fast ack could arrive before we are listening.
    time.sleep(0.5)

    request = {
        "cmd_id": cmd_id,
        "verb": args.verb,
        "target": target,
        "value": value,
        "quiet_on": quiet_on,
        "lat": lat,
        "lon": lon,
        "requested_by": "navamesh-cmd",
        "ts": cmd_id,
    }
    info = client.publish(topics.cmd_request(cfg.root_cmd), json.dumps(request), qos=1)
    info.wait_for_publish()

    detail = f"{args.verb} {target}" + (f" {value}" if value is not None else "")
    if quiet_on is not None:
        detail = f"quiet {target} {'on' if quiet_on else 'off'}"
    elif lat is not None:
        detail = f"setloc {target} {lat:.6f} {lon:.6f}"
    print(f"sent  [{cmd_id}] {detail}")

    if args.no_wait:
        client.loop_stop()
        return 0

    deadline = time.time() + args.timeout
    transmitted = False
    while time.time() < deadline:
        for s in list(seen):
            if str(s.get("cmd_id")) != str(cmd_id):
                continue
            state = s.get("state")
            if state == "sent" and not transmitted:
                transmitted = True
                print("      transmitted, waiting for the node...")
            elif state == "acked":
                d = s.get("detail") or {}
                applied = d.get("applied_value")
                alat, alon = d.get("applied_lat"), d.get("applied_lon")
                fw = d.get("firmware_version")
                if alat is not None and alon is not None:
                    shown = f" = {alat:.6f}, {alon:.6f}"
                elif args.verb == "fwinfo":
                    # applied_value is 0 here: the verb applies nothing, and the version
                    # *is* the answer. Without this the tool would print "ok" and drop it.
                    shown = f" = {fw or 'no version reported (firmware predates the field)'}"
                else:
                    shown = f" = {applied}" if applied else ""
                    # Every ack carries it, so show it on all of them -- it is the cheapest
                    # possible answer to "was this node even running the build I think?"
                    if fw:
                        shown += f"   [fw {fw}]"
                print(f"ok    node applied {d.get('command_type', args.verb)}{shown}")
                client.loop_stop()
                return 0
            elif state in ("nak", "error"):
                d = s.get("detail") or {}
                print(f"FAIL  {state}: {d.get('reason') or d}", file=sys.stderr)
                client.loop_stop()
                return 1
        time.sleep(0.5)

    client.loop_stop()
    # A timeout is genuinely ambiguous: the node may have applied the command and had its
    # ack lost, or never heard it at all. Say so rather than implying either.
    retry = ("" if args.verb in UNICAST_ONLY_VERBS else
             "\n         Nodes out of direct gateway range cannot be reached by "
             "unicast; try ^all.")
    print(f"TIMEOUT  no ack within {args.timeout:.0f}s. The command may still have been "
          f"applied.{retry}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
