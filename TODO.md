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
