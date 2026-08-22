#!/usr/bin/env python3
"""
NavaMesh Map Generator
======================
Generates a standalone offline Leaflet HTML map with color-coded moisture
markers from PostGIS. Designed to be called from reticulum_bridge.py and
sent as an LXMF file attachment to the farmer's Sideband app.

Can also be run standalone:
    python3 generate_map.py

Requirements:
    pip install psycopg2-binary   (or psycopg[binary])

Configuration:
    All settings read from environment / .env file.
    The reticulum bridge loads .env before importing this module, so
    no extra setup is needed when called from the plugin.
"""

import os
import math
import base64
import json
import datetime

# ─── CONFIG (all from environment — set in .env) ──────────────────────────────
# Mirrors the same PG_DSN that mqtt_to_db.py uses, so no new .env keys needed.
# Falls back to the individual DB_* variables for standalone use.

def _get_db_params():
    dsn = os.getenv("PG_DSN", "")
    if dsn:
        return {"dsn": dsn, "sslmode": "require"}
    return {
        "host":     os.getenv("DB_HOST",     "localhost"),
        "port":     int(os.getenv("DB_PORT", "5432")),
        "dbname":   os.getenv("DB_NAME",     "navamesh"),
        "user":     os.getenv("DB_USER",     "navamesh"),
        "password": os.getenv("DB_PASSWORD", ""),
        "sslmode":  "require",
    }


# Path to pre-cached tiles (run cache_tiles.py first)
TILES_DIR = os.getenv("TILES_DIR", "./tiles")

DEFAULT_CENTER = [
    float(os.getenv("MAP_CENTER_LAT", "26.3752")),
    float(os.getenv("MAP_CENTER_LON", "-80.0960")),
]
DEFAULT_ZOOM = int(os.getenv("MAP_DEFAULT_ZOOM", "17"))

# Moisture thresholds — can be overridden in .env
MOISTURE_DRY      = int(os.getenv("MOISTURE_DRY",      "30"))
MOISTURE_LOW      = int(os.getenv("MOISTURE_LOW",      "50"))
MOISTURE_MODERATE = int(os.getenv("MOISTURE_MODERATE", "70"))

# ── Confirmed keys from mqtt_to_db.py NodeState → metadata() ────────────────
# The ingestor writes  metadata->>'soil_percent'  (not 'soil_moisture').
MOISTURE_KEY = os.getenv("MOISTURE_KEY", "soil_percent")

# DRY / DAMP / WET. Preferred over the percentage: bench calibration showed the
# probe is blind below ~9.5% moisture and saturated above 20%, so soil_percent is
# deliberately absent outside the DAMP band. A node reading bone-dry has a band
# but no percentage -- colouring by percentage alone would render it "No reading".
BAND_KEY = os.getenv("BAND_KEY", "soil_band")

BAND_COLORS = {
    "DRY":  "#e74c3c",   # red    — needs water
    "DAMP": "#27ae60",   # green  — adequate
    "WET":  "#2980b9",   # blue   — saturated, do not irrigate
}
BAND_LABELS = {
    "DRY":  "Dry ⚠️ — needs water",
    "DAMP": "Damp ✓",
    "WET":  "Wet — saturated",
}

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def deg2tile(lat, lon, zoom):
    lat_r = math.radians(lat)
    n = 2 ** zoom
    x = int((lon + 180.0) / 360.0 * n)
    y = int((1.0 - math.log(math.tan(lat_r) + 1.0 / math.cos(lat_r)) / math.pi) / 2.0 * n)
    return x, y


def moisture_color(value, band=None):
    """Band wins when present; the percentage thresholds remain for legacy rows
    written before the band existed."""
    if band in BAND_COLORS:
        return BAND_COLORS[band]
    if value is None:
        return "#888888"
    if value < MOISTURE_DRY:
        return "#e74c3c"    # red — dry
    if value < MOISTURE_LOW:
        return "#e67e22"    # orange — low
    if value < MOISTURE_MODERATE:
        return "#f1c40f"    # yellow — moderate
    return "#27ae60"        # green — good


def moisture_label(value, band=None):
    """Always a band, never a percentage.

    The percentage was invented precision on this hardware: bench calibration showed the
    probe is blind below ~9.5% moisture and saturated above 20%, so a figure outside the
    DAMP window described the rail the probe was pinned to rather than the soil. Showing
    "17.7%" next to a sensor invited a reading of the number it could not support, and a
    farmer's decision is DRY / DAMP / WET either way -- water it, leave it, or hold off.

    Legacy rows carry a percentage but no band (they predate the band, or came from the
    old firmware's status strings). Those are mapped onto the same three words through the
    MOISTURE_* thresholds rather than shown as a number, so the map never mixes vocabularies.
    """
    if band in BAND_LABELS:
        return BAND_LABELS[band]
    if value is None:
        return "No reading"
    # Legacy percentage → the nearest band word. Deliberately no figure: it would be the
    # only percentage on the map and would read as more trustworthy than the bands.
    if value < MOISTURE_DRY:
        return BAND_LABELS["DRY"]
    if value < MOISTURE_MODERATE:
        return BAND_LABELS["DAMP"]
    return BAND_LABELS["WET"]


def tile_to_base64(z, x, y):
    path = os.path.join(TILES_DIR, str(z), str(x), f"{y}.png")
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        data = base64.b64encode(f.read()).decode("utf-8")
    return f"data:image/png;base64,{data}"


def get_tile_bounds(nodes, zoom):
    lats = [n["lat"] for n in nodes if n["lat"] is not None]
    lons = [n["lon"] for n in nodes if n["lon"] is not None]
    if not lats:
        lats = [DEFAULT_CENTER[0] - 0.003, DEFAULT_CENTER[0] + 0.003]
        lons = [DEFAULT_CENTER[1] - 0.004, DEFAULT_CENTER[1] + 0.004]

    lat_min, lat_max = min(lats) - 0.002, max(lats) + 0.002
    lon_min, lon_max = min(lons) - 0.003, max(lons) + 0.003

    x_min, y_max = deg2tile(lat_min, lon_min, zoom)
    x_max, y_min = deg2tile(lat_max, lon_max, zoom)

    tiles = [(x, y) for x in range(x_min, x_max + 1) for y in range(y_min, y_max + 1)]
    return tiles, (x_min, x_max, y_min, y_max)


# ─── DATABASE ────────────────────────────────────────────────────────────────

def fetch_nodes():
    """Query PostGIS for latest reading per node. Returns list of dicts."""
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError:
        try:
            import psycopg as psycopg2          # psycopg3 fallback
            psycopg2.extras = psycopg2          # type: ignore[attr-defined]
        except ImportError:
            raise RuntimeError(
                "psycopg2-binary (or psycopg[binary]) is required for map generation. "
                "Install with: pip install psycopg2-binary"
            )

    params = _get_db_params()
    if "dsn" in params:
        conn = psycopg2.connect(params["dsn"])
    else:
        conn = psycopg2.connect(**params)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("""
        SELECT
            node_id,
            lat,
            lon,
            last_seen,
            metadata,
            metadata->>%s                AS moisture_raw,
            metadata->>%s                AS soil_band,
            metadata->>'status'          AS status,
            metadata->>'rx_snr'          AS rx_snr,
            metadata->>'type'            AS node_type,
            metadata->>'battery_level'   AS battery
        FROM mesh_nodes
        ORDER BY last_seen DESC NULLS LAST;
    """, (MOISTURE_KEY, BAND_KEY))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    nodes = []
    for row in rows:
        moisture = None
        if row["moisture_raw"] is not None:
            try:
                moisture = float(row["moisture_raw"])
            except (ValueError, TypeError):
                pass

        nodes.append({
            "node_id":   row["node_id"],
            "lat":       float(row["lat"]) if row["lat"] is not None else None,
            "lon":       float(row["lon"]) if row["lon"] is not None else None,
            "last_seen": row["last_seen"].isoformat() if row["last_seen"] else "Unknown",
            "moisture":  moisture,
            "band":      row["soil_band"],
            "status":    row["status"] or "unknown",
            "rx_snr":    row["rx_snr"],
            "node_type": row["node_type"] or "field-node",
            "battery":   row["battery"],
            "metadata":  dict(row["metadata"]) if row["metadata"] else {},
        })
    return nodes


# ─── TILE CACHE ──────────────────────────────────────────────────────────────

def build_tile_lookup(nodes, zoom_levels=(16, 17, 18)):
    tile_data = {}
    missing = 0
    for zoom in zoom_levels:
        tiles, _ = get_tile_bounds(nodes, zoom)
        for (x, y) in tiles:
            key = f"{zoom}/{x}/{y}"
            b64 = tile_to_base64(zoom, x, y)
            if b64:
                tile_data[key] = b64
            else:
                missing += 1
    if missing > 0:
        print(f"  WARNING: {missing} tiles not in cache — those areas will use live OSM fallback.")
    return tile_data


# ─── HTML GENERATION ─────────────────────────────────────────────────────────

def generate_html(nodes, tile_data, zoom_levels=(16, 17, 18)):
    positioned = [n for n in nodes if n["lat"] and n["lon"]]
    if positioned:
        center_lat = sum(n["lat"] for n in positioned) / len(positioned)
        center_lon = sum(n["lon"] for n in positioned) / len(positioned)
    else:
        center_lat, center_lon = DEFAULT_CENTER

    markers_js = []
    for n in positioned:
        color = moisture_color(n["moisture"], n.get("band"))
        label = moisture_label(n["moisture"], n.get("band"))
        popup = (
            f"<b>{n['node_id']}</b><br>"
            f"Moisture: {label}<br>"
            f"Status: {n['status']}<br>"
            f"Battery: {n['battery'] or 'N/A'}%<br>"
            f"SNR: {n['rx_snr'] or 'N/A'} dB<br>"
            f"Last seen: {n['last_seen']}"
        )
        markers_js.append({
            "lat": n["lat"], "lon": n["lon"],
            "color": color, "label": label,
            "popup": popup, "id": n["node_id"],
        })

    offline_nodes = [n for n in nodes if not n["lat"] or not n["lon"]]

    tile_json    = json.dumps(tile_data)
    markers_json = json.dumps(markers_js)

    legend_items = [
        ("#27ae60", f"Good (≥{MOISTURE_MODERATE}%)"),
        ("#f1c40f", f"Moderate ({MOISTURE_LOW}–{MOISTURE_MODERATE}%)"),
        ("#e67e22", f"Low ({MOISTURE_DRY}–{MOISTURE_LOW}%)"),
        ("#e74c3c", f"Dry (<{MOISTURE_DRY}%)"),
        ("#888888", "No reading"),
    ]
    legend_html = "".join(
        f'<div class="legend-item"><span class="dot" style="background:{c}"></span>{l}</div>'
        for c, l in legend_items
    )

    offline_html = ""
    if offline_nodes:
        rows = "".join(
            f"<tr><td>{n['node_id']}</td><td>{n['status']}</td><td>{n['last_seen']}</td></tr>"
            for n in offline_nodes
        )
        offline_html = f"""
        <div class="offline-panel">
          <h3>Nodes without GPS fix ({len(offline_nodes)})</h3>
          <table><tr><th>Node ID</th><th>Status</th><th>Last Seen</th></tr>{rows}</table>
        </div>"""

    generated_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>NavaMesh Farm Map — {generated_at}</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: Arial, sans-serif; background: #1a1a2e; color: #eee; }}
    #header {{
      background: #16213e; padding: 12px 16px;
      border-bottom: 2px solid #e67e22;
      display: flex; justify-content: space-between; align-items: center;
    }}
    #header h1 {{ font-size: 1.1em; color: #e67e22; }}
    #header .ts {{ font-size: 0.75em; color: #aaa; }}
    #map {{ width: 100%; height: calc(100vh - 90px); }}
    #legend {{
      position: fixed; bottom: 16px; right: 16px;
      background: rgba(22,33,62,0.92); border: 1px solid #444;
      border-radius: 8px; padding: 10px 14px; z-index: 1000;
      font-size: 0.8em;
    }}
    #legend h4 {{ color: #e67e22; margin-bottom: 6px; }}
    .legend-item {{ display: flex; align-items: center; margin: 3px 0; }}
    .dot {{
      width: 12px; height: 12px; border-radius: 50%;
      display: inline-block; margin-right: 7px; flex-shrink: 0;
    }}
    .offline-panel {{
      background: #16213e; padding: 12px 16px;
      border-top: 1px solid #333; font-size: 0.82em;
    }}
    .offline-panel h3 {{ color: #aaa; margin-bottom: 6px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    td, th {{ padding: 4px 10px; border-bottom: 1px solid #333; text-align: left; }}
    th {{ color: #e67e22; }}
  </style>
  <link rel="stylesheet"
    href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" crossorigin=""/>
</head>
<body>
<div id="header">
  <h1>🌱 NavaMesh Farm Map</h1>
  <span class="ts">Generated: {generated_at} &nbsp;|&nbsp; {len(positioned)} nodes plotted &nbsp;|&nbsp; {len(offline_nodes)} without GPS</span>
</div>
<div id="map"></div>
<div id="legend">
  <h4>Soil Moisture</h4>
  {legend_html}
</div>
{offline_html}
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" crossorigin=""></script>
<script>
const TILE_CACHE = {tile_json};
const MARKERS    = {markers_json};

const OfflineTileLayer = L.TileLayer.extend({{
  createTile: function(coords, done) {{
    const tile = document.createElement('img');
    const key  = coords.z + '/' + coords.x + '/' + coords.y;
    tile.src   = TILE_CACHE[key] || ('https://tile.openstreetmap.org/' + key + '.png');
    tile.onload  = () => done(null, tile);
    tile.onerror = () => done('Tile load error', tile);
    return tile;
  }}
}});

const map = L.map('map').setView([{center_lat}, {center_lon}], {DEFAULT_ZOOM});
new OfflineTileLayer('', {{
  attribution: '© <a href="https://openstreetmap.org">OpenStreetMap</a>',
  maxZoom: 19, minZoom: 13,
}}).addTo(map);

MARKERS.forEach(function(node) {{
  const icon = L.divIcon({{
    className: '',
    html: `<div style="width:22px;height:22px;border-radius:50%;background:${{node.color}};border:3px solid #fff;box-shadow:0 2px 6px rgba(0,0,0,0.5);"></div>`,
    iconSize: [22, 22], iconAnchor: [11, 11], popupAnchor: [0, -14],
  }});
  L.marker([node.lat, node.lon], {{icon}})
    .addTo(map).bindPopup(node.popup, {{maxWidth: 220}});
}});

if (MARKERS.length > 1) {{
  map.fitBounds(L.latLngBounds(MARKERS.map(m => [m.lat, m.lon])), {{padding: [40, 40]}});
}}
</script>
</body>
</html>"""


# ─── PUBLIC API (called by reticulum_bridge.py) ───────────────────────────────

def generate_map_html(output_path: str = None) -> str:
    """
    Full pipeline: fetch DB → load tiles → render HTML.
    Returns the path to the written HTML file.
    Raises RuntimeError on DB failure so the bridge can send an error reply.
    """
    nodes     = fetch_nodes()
    tile_data = build_tile_lookup(nodes)
    html      = generate_html(nodes, tile_data)

    if output_path is None:
        ts          = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        output_path = f"/tmp/farm_map_{ts}.html"

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    return output_path


# ─── STANDALONE ENTRY POINT ──────────────────────────────────────────────────

def main():
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv(usecwd=True))

    print("NavaMesh Map Generator")
    print("=" * 40)

    print("Connecting to PostGIS...")
    try:
        path = generate_map_html()
    except Exception as e:
        print(f"ERROR: {e}")
        raise SystemExit(1)

    size_kb = os.path.getsize(path) / 1024
    print(f"\n✓ Map saved: {path}  ({size_kb:.0f} KB)")
    print("  Open in any browser or attach to a Sideband message.")


if __name__ == "__main__":
    main()
