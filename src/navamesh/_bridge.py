"""
Bridge entry-point logic.
Imported by both main.py (project root) and navamesh.__main__ (console script).
"""
import time
from dotenv import load_dotenv, find_dotenv

from navamesh.config import load_config
from navamesh.mqtt_client import MqttPublisher, MqttCommandSubscriber
from navamesh.bridge import MeshBridge
from navamesh import topics

from navamesh.processors.soil_text import (
    is_status_message,
    parse_status_message,
    make_status_battery_payload,
)
from navamesh.processors.soil_proto import (
    SOIL_SOURCE,
    extract_soil_reading,
    make_soil_mqtt_payloads,
)
from navamesh.processors.command_proto import (
    COMMAND_PORTNUM,
    UNICAST_ONLY_VERBS,
    VERB_SETLOC,
    CommandValidationError,
    encode_command,
    extract_command_ack,
)
from navamesh.processors.link import extract_link
from navamesh.processors.position import extract_position
from navamesh.processors.telemetry import extract_battery
from navamesh.processors.node_info import extract_node_info
from navamesh.processors.node_id import resolve_node_id


def _should_bridge(packet: dict, private_channel_index: int) -> bool:
    ch = packet.get("channel", None)
    if ch is None:
        return True
    return ch == private_channel_index


def _is_private_channel(packet: dict, private_channel_index: int) -> bool:
    return packet.get("channel") == private_channel_index


def purge_legacy_retained_soil(mqtt_pub, root_sensors: str) -> int:
    """
    Delete retained soil messages left over from the legacy "Soil: XX%" text era.

    Retained messages are replayed to any subscriber on connect, so a stale
    node-computed percentage sitting on farm/sensors/soil/<node>/percent would be
    re-applied by the ingestor on every restart — silently resurrecting a value
    that can no longer be recalibrated, and overwriting nothing but also never
    expiring. Purging them at startup guarantees the only soil data in the system
    came from the authoritative PRIVATE_APP raw-ADC path.

    Payloads stamped with SOIL_SOURCE are kept: those were produced by the new
    path and are still valid, so their replay-on-subscribe is desirable.

    Returns the number of retained topics cleared.
    """
    cleared = 0
    for suffix in ("percent", "raw"):
        topic_filter = f"{root_sensors}/soil/+/{suffix}"
        try:
            retained = mqtt_pub.collect_retained(topic_filter)
        except Exception as e:
            print(f"[STARTUP] could not scan {topic_filter}: {e}")
            continue

        for topic, payload in retained.items():
            if isinstance(payload, dict) and payload.get("source") == SOIL_SOURCE:
                continue  # produced by the new raw-ADC path, keep it
            mqtt_pub.clear_retained(topic)
            cleared += 1
            print(f"[STARTUP] purged legacy retained soil value: {topic}")
    return cleared


def main():
    load_dotenv(find_dotenv(usecwd=True))
    cfg = load_config()

    print(f"Connecting gateway radio on {cfg.serial_port} "
          f"(private channel index={cfg.private_channel_index})")
    mqtt_pub = MqttPublisher(cfg.mqtt_host, cfg.mqtt_port)

    # Soil moisture is now sourced exclusively from the PRIVATE_APP raw-ADC path.
    # Clear any retained percentages published by the legacy text parser before
    # they can be replayed to the ingestor.
    n = purge_legacy_retained_soil(mqtt_pub, cfg.root_sensors)
    print(f"[STARTUP] legacy retained soil topics purged: {n}")

    def on_receive(packet: dict, interface=None, **kwargs):
        try:
            if not _should_bridge(packet, cfg.private_channel_index):
                return

            # Raw packets — not retained (transient debug data)
            mqtt_pub.publish(topics.raw_rx(cfg.root_raw), packet)

            # Link quality — retained so reticulum cache survives restarts
            link = extract_link(packet)
            if link:
                mqtt_pub.publish(
                    topics.node_link(cfg.root_nodes, link["fromId"]),
                    link,
                    retain=True,
                )

            # Position — retained
            pos = extract_position(packet)
            if pos:
                mqtt_pub.publish(
                    topics.node_position(cfg.root_nodes, pos["fromId"]),
                    pos,
                    retain=True,
                )

            # Node names (from NODEINFO_APP) — retained so app renames survive restarts
            info = extract_node_info(packet)
            if info:
                mqtt_pub.publish(
                    topics.node_info(cfg.root_nodes, info["fromId"]),
                    info,
                    retain=True,
                )

            # Battery (from TELEMETRY_APP) — retained
            battery = extract_battery(packet)
            if battery:
                mqtt_pub.publish(
                    topics.node_battery(cfg.root_nodes, battery["fromId"]),
                    battery,
                    retain=True,
                )

            decoded = packet.get("decoded", {}) or {}

            # Command acknowledgement on PortNum 259. Checked before the soil path
            # purely for readability -- the two live on different portnums, so unlike
            # the old shared-256 scheme there is no decode-order requirement here.
            ack = extract_command_ack(packet)
            if ack is not None:
                from_id = resolve_node_id(packet) or "unknown"
                status = {
                    "cmd_id": ack["command_id"],
                    "node_id": from_id,
                    "state": "acked" if ack["ok"] else "nak",
                    "detail": {
                        "command_type": ack["command_type_name"],
                        "applied_value": ack["applied_value"],
                        "applied_lat": ack["applied_lat"],
                        "applied_lon": ack["applied_lon"],
                        "unsolicited": ack["unsolicited"],
                    },
                    "ts": int(time.time()),
                }
                mqtt_pub.publish(topics.cmd_status(cfg.root_cmd), status, qos=1)
                if ack["unsolicited"]:
                    print(f"[CMD] {from_id} self-resumed from quiet mode (unsolicited ack)")
                else:
                    print(f"[CMD] {from_id} ack id={ack['command_id']} "
                          f"{ack['command_type_name']} ok={ack['ok']} "
                          f"applied={ack['applied_value']}")
                return

            # FORMAT C: navamesh.SoilReading protobuf on PortNum 256 (PRIVATE_APP).
            # Authoritative path — the node sends the RAW averaged ADC and the
            # DRY/DAMP/WET band is derived here. Must be handled before the
            # TEXT_MESSAGE_APP early-return below, which would otherwise drop it.
            reading = extract_soil_reading(packet)
            if reading is not None:
                from_id = resolve_node_id(packet) or "unknown"
                raw_pl, pct_pl, band_pl, bat_pl = make_soil_mqtt_payloads(
                    from_id, reading
                )

                # Raw ADC — the authoritative measurement, stored verbatim
                mqtt_pub.publish(
                    topics.soil_raw(cfg.root_sensors, from_id),
                    raw_pl,
                    retain=True,
                )

                # Pi-derived band — DRY / DAMP / WET. What consumers act on.
                mqtt_pub.publish(
                    topics.soil_band(cfg.root_sensors, from_id),
                    band_pl,
                    retain=True,
                )

                # Coarse percentage. Absent outside the DAMP band, where the
                # bench data shows the probe has no resolution to report.
                if pct_pl is not None:
                    mqtt_pub.publish(
                        topics.soil_percent(cfg.root_sensors, from_id),
                        pct_pl,
                        retain=True,
                    )

                # Battery / voltage / uptime
                mqtt_pub.publish(
                    topics.node_battery(cfg.root_nodes, from_id),
                    bat_pl,
                    retain=True,
                )

                pct_str = f" ({pct_pl['value']}%)" if pct_pl else ""
                print(f"[SENSOR] {from_id} | raw_adc={reading['raw_adc']} | "
                      f"soil={band_pl['value']}{pct_str} | "
                      f"bat={reading['battery_percent']}% "
                      f"({bat_pl['voltage']}V) | up={reading['uptime_seconds']}s")
                return

            if decoded.get("portnum") != "TEXT_MESSAGE_APP":
                return

            if not _is_private_channel(packet, cfg.private_channel_index):
                return

            # Raw text — not retained
            mqtt_pub.publish(topics.raw_text(cfg.root_raw), packet)

            text    = decoded.get("text") or ""
            from_id = resolve_node_id(packet) or "unknown"

            # FORMAT B (LEGACY): "Soil: 47% | Bat: 82% | Up: 1h 23m"
            #
            # The soil percentage here is NOT authoritative and is deliberately
            # discarded — a node-computed percentage cannot be recalibrated after
            # the fact. Only battery/uptime is salvaged, so a node still on old
            # firmware keeps reporting power state but contributes no soil
            # measurement until it is reflashed.
            #
            # The new debug line ("ADC: 1842 | Bat: ...") does not match
            # is_status_message(), so it never reaches this branch at all.
            if is_status_message(text):
                parsed = parse_status_message(text)
                if parsed is None:
                    return

                # Battery from text message — retained. No soil publish.
                bat_pl = make_status_battery_payload(from_id, parsed)
                if bat_pl is not None:
                    mqtt_pub.publish(
                        topics.node_battery(cfg.root_nodes, from_id),
                        bat_pl,
                        retain=True,
                    )

                bat_str = "USB" if parsed["battery_usb"] else f"{parsed['battery_level']}%"
                print(f"[LEGACY] {from_id} | bat={bat_str} | up={parsed['uptime_seconds']}s "
                      f"| soil={parsed['soil_percent']}% IGNORED "
                      f"(node needs new firmware for raw-ADC soil)")
                return


        except Exception as e:
            print("[ERR] on_receive:", e)

    bridge = MeshBridge(cfg.serial_port, on_receive=on_receive)
    bridge.start()

    def on_command(req: dict) -> None:
        """
        Transmit a command that reticulum_bridge has already validated and logged.

        This process is the only one holding the serial port, which is why the command
        arrives over MQTT rather than being sent by the process that received it from
        the operator.

        Everything is re-validated here anyway. This is the last gate before RF, and it
        must hold even if a command were published by something other than
        reticulum_bridge.
        """
        cmd_id = req.get("cmd_id")
        verb = (req.get("verb") or "").strip().lower()
        target = (req.get("target") or "^all").strip()
        value = req.get("value")
        quiet_on = req.get("quiet_on")
        lat = req.get("lat")
        lon = req.get("lon")

        def report(state: str, detail) -> None:
            mqtt_pub.publish(
                topics.cmd_status(cfg.root_cmd),
                {
                    "cmd_id": cmd_id,
                    "node_id": target,
                    "state": state,
                    "detail": detail,
                    "ts": int(time.time()),
                },
                qos=1,
            )

        try:
            payload = encode_command(verb, cmd_id, value=value, quiet_on=quiet_on,
                                     lat=lat, lon=lon)
        except CommandValidationError as e:
            print(f"[CMD] rejecting {verb!r} for {target}: {e}")
            report("error", {"reason": str(e)})
            return

        # want_ack only for unicast. A broadcast has no hardware ack, so requesting one
        # would burn airtime on retries that cannot ever be satisfied. Either way the
        # node's own NavameshAck on PortNum 259 is what actually confirms delivery.
        is_broadcast = target in ("^all", "", "all")

        # Re-checked here rather than trusted from reticulum_bridge: this is the last gate
        # before RF, and a broadcast setloc would hand every node in the field the same
        # coordinates in a single unrecoverable transmission.
        if is_broadcast and verb in UNICAST_ONLY_VERBS:
            print(f"[CMD] refusing to broadcast {verb!r}")
            report("error", {"reason": f"{verb} must target one node, not {target}"})
            return

        sent = bridge.send_data(
            payload,
            destination_id="^all" if is_broadcast else target,
            port_num=COMMAND_PORTNUM,
            channel_index=cfg.private_channel_index,
            want_ack=not is_broadcast,
        )

        if sent:
            args = (f"lat={lat}, lon={lon}" if verb == VERB_SETLOC
                    else f"value={value}, quiet_on={quiet_on}")
            print(f"[CMD] sent id={cmd_id} {verb} -> {target} ({args})")
            report("sent", {"broadcast": is_broadcast})
        else:
            print(f"[CMD] transmit failed id={cmd_id} {verb} -> {target}")
            report("error", {"reason": "gateway radio unavailable"})

    cmd_sub = MqttCommandSubscriber(
        cfg.mqtt_host, cfg.mqtt_port, topics.cmd_request(cfg.root_cmd), on_command
    )

    print("Navamesh bridge running. Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        cmd_sub.close()
        bridge.stop()
        mqtt_pub.close()
        print("Stopped.")
