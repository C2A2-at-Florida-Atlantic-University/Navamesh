import time
import threading
from typing import Callable, Optional
from meshtastic.serial_interface import SerialInterface
from pubsub import pub


RECONNECT_DELAY_SECS = 10  # wait before retrying after disconnect


class MeshBridge:
    """
    Subscribes to Meshtastic pubsub receive events and forwards packets via a single callback.
    Automatically reconnects if the serial port drops.
    """
    def __init__(self, serial_port: str, on_receive: Callable[[dict], None]):
        self._iface: Optional[SerialInterface] = None
        self._serial_port = serial_port
        self._on_receive = on_receive
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def _connect(self) -> bool:
        try:
            print(f"[BRIDGE] Connecting to {self._serial_port}...")
            self._iface = SerialInterface(self._serial_port)
            self._pubsub_listener = lambda packet, interface=None, **kw: self._on_receive(packet)
            pub.subscribe(self._pubsub_listener, "meshtastic.receive")
            print(f"[BRIDGE] Connected to {self._serial_port}")
            return True
        except Exception as e:
            print(f"[BRIDGE] Connection failed: {e}")
            return False

    def _disconnect(self) -> None:
        try:
            if hasattr(self, '_pubsub_listener'):
                pub.unsubscribe(self._pubsub_listener, "meshtastic.receive")
        except Exception:
            pass
        try:
            if self._iface:
                self._iface.close()
        except Exception as e:
            print(f"[WARN] iface.close() error: {e}")
        self._iface = None

    def _run_loop(self) -> None:
        while self._running:
            if not self._connect():
                print(f"[BRIDGE] Retrying in {RECONNECT_DELAY_SECS}s...")
                time.sleep(RECONNECT_DELAY_SECS)
                continue

            # Monitor connection health
            while self._running:
                time.sleep(5)
                if self._iface is None:
                    print("[BRIDGE] Interface lost, reconnecting...")
                    break
                # Check if serial stream is still alive
                try:
                    if not self._iface.stream or not self._iface.stream.isOpen():
                        print("[BRIDGE] Serial port closed, reconnecting...")
                        break
                except Exception:
                    print("[BRIDGE] Serial check failed, reconnecting...")
                    break

            self._disconnect()
            if self._running:
                print(f"[BRIDGE] Waiting {RECONNECT_DELAY_SECS}s before reconnect...")
                time.sleep(RECONNECT_DELAY_SECS)

    def stop(self) -> None:
        self._running = False
        self._disconnect()
        if self._thread:
            self._thread.join(timeout=15)
        self._iface = None
