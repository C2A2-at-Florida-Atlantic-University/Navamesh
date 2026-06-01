#!/usr/bin/env python3
"""
NavaMesh Tile Cacher
====================
Run ONCE (with internet) before shipping to pre-download OSM tiles for the farm area.
Tiles are saved to ./tiles/{z}/{x}/{y}.png and served offline by the local tile server.

Setup:
    Set the farm bounding box in .env before running:

        CACHE_LAT_MIN=26.3720
        CACHE_LAT_MAX=26.3785
        CACHE_LON_MIN=-80.1000
        CACHE_LON_MAX=-80.0920
        CACHE_ZOOM_MIN=14   # optional, default 14
        CACHE_ZOOM_MAX=19   # optional, default 19

Usage:
    python3 cache_tiles.py
"""

import os
import math
import time
import urllib.request
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(usecwd=True))


def _require_float(name: str) -> float:
    val = os.getenv(name)
    if val is None:
        raise SystemExit(
            f"ERROR: {name} is not set in .env\n"
            f"Set all four bounding box variables before running cache_tiles.py:\n"
            f"  CACHE_LAT_MIN, CACHE_LAT_MAX, CACHE_LON_MIN, CACHE_LON_MAX"
        )
    return float(val)


LAT_MIN   = _require_float("CACHE_LAT_MIN")
LAT_MAX   = _require_float("CACHE_LAT_MAX")
LON_MIN   = _require_float("CACHE_LON_MIN")
LON_MAX   = _require_float("CACHE_LON_MAX")
ZOOM_MIN  = int(os.getenv("CACHE_ZOOM_MIN", "14"))
ZOOM_MAX  = int(os.getenv("CACHE_ZOOM_MAX", "19"))

TILE_SERVER = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
OUTPUT_DIR  = os.getenv("TILES_DIR", "tiles")
DELAY_SEC   = 0.1


def deg2tile(lat, lon, zoom):
    lat_r = math.radians(lat)
    n = 2 ** zoom
    x = int((lon + 180.0) / 360.0 * n)
    y = int((1.0 - math.log(math.tan(lat_r) + 1.0 / math.cos(lat_r)) / math.pi) / 2.0 * n)
    return x, y


def download_tile(z, x, y):
    path = os.path.join(OUTPUT_DIR, str(z), str(x), f"{y}.png")
    if os.path.exists(path):
        return False

    os.makedirs(os.path.dirname(path), exist_ok=True)
    url = TILE_SERVER.format(z=z, x=x, y=y)
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "NavaMesh/1.0 (precision agriculture research)"}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            with open(path, "wb") as f:
                f.write(resp.read())
        return True
    except Exception as e:
        print(f"  WARNING: Failed to download tile {z}/{x}/{y}: {e}")
        return False


def main():
    print("NavaMesh Tile Cacher")
    print(f"  Bounding box: ({LAT_MIN},{LON_MIN}) → ({LAT_MAX},{LON_MAX})")
    print(f"  Zoom levels:  {ZOOM_MIN}–{ZOOM_MAX}")
    print(f"  Output dir:   {OUTPUT_DIR}")
    print()

    total_downloaded = 0
    total_skipped    = 0
    total_tiles      = 0

    for z in range(ZOOM_MIN, ZOOM_MAX + 1):
        x_min, y_max = deg2tile(LAT_MIN, LON_MIN, z)
        x_max, y_min = deg2tile(LAT_MAX, LON_MAX, z)
        count = (x_max - x_min + 1) * (y_max - y_min + 1)
        total_tiles += count
        print(f"  Zoom {z}: {count} tiles ({x_min}–{x_max}, {y_min}–{y_max})")

        for x in range(x_min, x_max + 1):
            for y in range(y_min, y_max + 1):
                downloaded = download_tile(z, x, y)
                if downloaded:
                    total_downloaded += 1
                    time.sleep(DELAY_SEC)
                else:
                    total_skipped += 1

    print()
    print(f"Done. {total_downloaded} downloaded, {total_skipped} already cached.")
    print(f"Tiles saved to: ./{OUTPUT_DIR}/")

    total_bytes = sum(
        os.path.getsize(os.path.join(dp, f))
        for dp, _, files in os.walk(OUTPUT_DIR)
        for f in files
    )
    print(f"Cache size: {total_bytes / 1_048_576:.1f} MB")


if __name__ == "__main__":
    main()
