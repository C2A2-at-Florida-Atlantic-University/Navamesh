def raw_rx(root_raw: str) -> str:
    return f"{root_raw}/rx"

def raw_text(root_raw: str) -> str:
    return f"{root_raw}/text"

def soil_raw(root_sensors: str, from_id: str) -> str:
    return f"{root_sensors}/soil/{from_id}/raw"

def soil_percent(root_sensors: str, from_id: str) -> str:
    return f"{root_sensors}/soil/{from_id}/percent"

def soil_band(root_sensors: str, from_id: str) -> str:
    """DRY / DAMP / WET — the authoritative soil state (see navamesh.calibration).

    Published alongside /percent rather than replacing it: /percent still carries a
    coarse figure inside the DAMP band for trend lines, but it is absent outside it,
    where the sensor has no resolution. Consumers making decisions read this topic.
    """
    return f"{root_sensors}/soil/{from_id}/band"

def node_link(root_nodes: str, from_id: str) -> str:
    return f"{root_nodes}/{from_id}/link"

def node_position(root_nodes: str, from_id: str) -> str:
    return f"{root_nodes}/{from_id}/position"

def node_battery(root_nodes: str, from_id: str) -> str:
    return f"{root_nodes}/{from_id}/battery"

def node_info(root_nodes: str, from_id: str) -> str:
    return f"{root_nodes}/{from_id}/info"


# --- Downlink command bus -------------------------------------------------------
#
# Both of these MUST be published non-retained. A retained command would be
# redelivered to the bridge on every reconnect and silently re-command the mesh --
# reopening BLE windows or re-muting nodes long after the operator moved on.

def cmd_request(root_cmd: str) -> str:
    """reticulum_bridge -> bridge: a validated command to transmit."""
    return f"{root_cmd}/request"

def cmd_status(root_cmd: str) -> str:
    """bridge -> ingestor: transmit outcome, then the node's ack when it arrives."""
    return f"{root_cmd}/status"