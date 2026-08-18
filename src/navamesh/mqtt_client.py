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


class MqttCommandSubscriber:
    """
    Holds a durable subscription and invokes a callback per message.

    Deliberately its own paho client rather than a method on MqttPublisher:
    MqttPublisher.collect_retained() takes over `on_message` and then sets it back to
    None, which would silently tear down any long-lived subscription sharing that
    client. Keeping them separate means the startup retained-purge cannot break the
    command bus.
    """

    def __init__(self, host: str, port: int, topic: str, on_command, qos: int = 1):
        self._topic = topic
        self._qos = qos
        self._on_command = on_command
        self._client = mqtt.Client()
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        # connect_async, not connect: the bridge and the broker are separate containers
        # with no start-up ordering guarantee, and the gateway radio is far more
        # important than the command bus. A blocking connect here would abort the whole
        # bridge process just because the broker was a few seconds behind. paho's network
        # loop retries on its own, and _on_connect re-subscribes each time it succeeds.
        self._client.connect_async(host, port, 60)
        self._client.loop_start()

    def _on_connect(self, client, userdata, flags, rc):
        # Re-subscribing here (rather than once after connect) means the subscription
        # survives a broker restart, which would otherwise leave commands unheard.
        client.subscribe(self._topic, qos=self._qos)
        print(f"[MQTT] subscribed rc={rc} topic={self._topic}")

    def _on_message(self, client, userdata, msg):
        # A malformed command must never kill the network thread -- that would take the
        # whole command bus down until the process restarted.
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except Exception as e:
            print(f"[MQTT] ignoring undecodable command on {msg.topic}: {e}")
            return
        try:
            self._on_command(payload)
        except Exception as e:
            print(f"[MQTT] command handler raised on {msg.topic}: {e}")

    def close(self) -> None:
        try:
            self._client.loop_stop()
        except Exception:
            pass
