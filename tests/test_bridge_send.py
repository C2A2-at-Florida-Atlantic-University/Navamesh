"""
test_bridge_send.py — MeshBridge.send_data(), the one outbound path in the bridge.

No real serial port is involved: a fake interface object is assigned to the private
_iface, which is exactly what the reconnect loop would have done.
"""

from navamesh.bridge import MeshBridge


class FakeIface:
    def __init__(self, raises=None):
        self.calls = []
        self._raises = raises

    def sendData(self, payload, **kwargs):
        if self._raises:
            raise self._raises
        self.calls.append((payload, kwargs))


def _bridge(iface=None):
    b = MeshBridge("/dev/null", on_receive=lambda p: None)
    b._iface = iface
    return b


def test_send_data_passes_through_addressing_and_portnum():
    iface = FakeIface()
    b = _bridge(iface)

    assert b.send_data(b"\x08\x01", destination_id="!abc12345",
                       port_num=258, channel_index=1, want_ack=True) is True

    payload, kwargs = iface.calls[0]
    assert payload == b"\x08\x01"
    assert kwargs["destinationId"] == "!abc12345"
    assert kwargs["portNum"] == 258
    assert kwargs["channelIndex"] == 1
    assert kwargs["wantAck"] is True


def test_send_data_defaults_to_broadcast_on_the_command_portnum():
    iface = FakeIface()
    _bridge(iface).send_data(b"x")
    _, kwargs = iface.calls[0]
    assert kwargs["destinationId"] == "^all"
    assert kwargs["portNum"] == 258
    assert kwargs["wantAck"] is False


def test_send_data_returns_false_when_disconnected_instead_of_raising():
    """
    The command bus runs on the MQTT network thread. If a mid-reconnect send raised,
    it would take that thread down and the bridge would stop hearing commands entirely.
    """
    assert _bridge(None).send_data(b"x") is False


def test_send_data_returns_false_when_the_radio_errors():
    """A USB hiccup must be reported as "not sent", not propagated."""
    assert _bridge(FakeIface(raises=OSError("port went away"))).send_data(b"x") is False


def test_send_data_holds_the_lock_so_reconnects_cannot_interleave():
    iface = FakeIface()
    b = _bridge(iface)
    # Not reentrant: if send_data forgot to release, this second call would deadlock.
    assert b.send_data(b"a") is True
    assert b.send_data(b"b") is True
    assert len(iface.calls) == 2
