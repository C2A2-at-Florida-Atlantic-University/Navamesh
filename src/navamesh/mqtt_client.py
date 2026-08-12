import json
import time
import paho.mqtt.client as mqtt

class MqttPublisher:
    def __init__(self, host: str, port: int):
        self._client = mqtt.Client()
        self._client.connect(host, port, 60)
        self._client.loop_start()

    def publish(self, topic: str, obj, qos: int = 0, retain: bool = False) -> None:
        payload = json.dumps(obj, default=str, ensure_ascii=False)
        info = self._client.publish(topic, payload, qos=qos, retain=retain)
        print(f"[MQTT] published rc={info.rc} topic={topic}")

    def clear_retained(self, topic: str) -> None:
        """
        Delete a retained message by publishing a zero-length retained payload.
        Note this must NOT go through publish(), which would json-encode "" into
        the two-byte string '""' and simply retain that instead.
        """
        self._client.publish(topic, payload=None, qos=0, retain=True)
        print(f"[MQTT] cleared retained topic={topic}")

    def collect_retained(self, topic_filter: str, settle_seconds: float = 2.0) -> dict:
        """
        Subscribe to `topic_filter`, gather whatever retained messages the broker
        replays, then unsubscribe. Returns {topic: decoded_payload_or_None}.

        Retained messages are delivered immediately on subscribe, so a short
        settle window is sufficient; there is no way to know the count up front.
        """
        found = {}

        def on_message(client, userdata, msg):
            try:
                found[msg.topic] = json.loads(msg.payload.decode("utf-8"))
            except Exception:
                found[msg.topic] = None

        self._client.on_message = on_message
        self._client.subscribe(topic_filter)
        time.sleep(settle_seconds)
        self._client.unsubscribe(topic_filter)
        self._client.on_message = None
        return found

    def close(self) -> None:
        try:
            self._client.loop_stop()
        except Exception:
            pass
