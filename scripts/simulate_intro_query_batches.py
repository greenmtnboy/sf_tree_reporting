#!/usr/bin/env python3
"""Simulate intro zoom query behavior and optionally analyze real SQL logs.

This script models the intro path in TreeMap.vue and the batch key behavior in
useDuckDB.ts to estimate how many neighborhood-batch DuckDB queries are
triggered per detailed zoom level (z>=15).

It can also parse exported console JSON/NDJSON logs and count actual unique
batch ranges by zoom from SQL text.

Usage examples:
  C:/Users/ethan/coding_projects/sf_tree_reporting/.venv/Scripts/python.exe scripts/simulate_intro_query_batches.py
  C:/Users/ethan/coding_projects/sf_tree_reporting/.venv/Scripts/python.exe scripts/simulate_intro_query_batches.py --fps 60 --width 1728 --height 1117
  C:/Users/ethan/coding_projects/sf_tree_reporting/.venv/Scripts/python.exe scripts/simulate_intro_query_batches.py --log-file intro_logs.ndjson
"""

from __future__ import annotations

import argparse
import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

INTRO_CENTER = (-122.4194, 37.7749)
INTRO_START_ZOOM = 18.5
INTRO_END_ZOOM = 13.5
INTRO_DURATION_MS = 10_000
INTRO_ROTATION_DEG = 240  # retained for parity, not needed for range math
CHECKPOINTS = [INTRO_START_ZOOM, 17.6, 16.8, 16.0, 15.2, 14.3, INTRO_END_ZOOM]

WEB_MERCATOR_MAX_LAT = 85.05112878
TILE_SIZE = 512  # map style uses raster tileSize 512; good approximation for world size math

SQL_Z_RE = re.compile(r"ST_TileEnvelope\((\d+),", re.IGNORECASE)
SQL_X_RE = re.compile(r"xtile(?:_z\d+)?\s+BETWEEN\s+(-?\d+)\s+AND\s+(-?\d+)", re.IGNORECASE)
SQL_Y_RE = re.compile(r"ytile(?:_z\d+)?\s+BETWEEN\s+(-?\d+)\s+AND\s+(-?\d+)", re.IGNORECASE)


@dataclass(frozen=True)
class TileRange:
    z: int
    min_x: int
    max_x: int
    min_y: int
    max_y: int

    def key(self, revision: int = 1) -> str:
        return f"{revision}:{self.z}:{self.min_x}-{self.max_x}:{self.min_y}-{self.max_y}"


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def ease_smoothstep(t: float) -> float:
    return t * t * (3 - 2 * t)


def lonlat_to_world_px(lon: float, lat: float, zoom: float) -> tuple[float, float]:
    lat = clamp(lat, -WEB_MERCATOR_MAX_LAT, WEB_MERCATOR_MAX_LAT)
    world = TILE_SIZE * (2**zoom)
    x = ((lon + 180.0) / 360.0) * world
    sin_lat = math.sin(math.radians(lat))
    y = (0.5 - math.log((1 + sin_lat) / (1 - sin_lat)) / (4 * math.pi)) * world
    return x, y


def world_px_to_lonlat(x: float, y: float, zoom: float) -> tuple[float, float]:
    world = TILE_SIZE * (2**zoom)
    lon = (x / world) * 360.0 - 180.0
    n = math.pi - (2 * math.pi * y) / world
    lat = math.degrees(math.atan(math.sinh(n)))
    return lon, lat


def approx_bounds(center_lng: float, center_lat: float, zoom: float, width_px: int, height_px: int) -> tuple[float, float, float, float]:
    cx, cy = lonlat_to_world_px(center_lng, center_lat, zoom)
    half_w = width_px / 2.0
    half_h = height_px / 2.0

    west, north = world_px_to_lonlat(cx - half_w, cy - half_h, zoom)
    east, south = world_px_to_lonlat(cx + half_w, cy + half_h, zoom)
    return west, east, north, south


def lon_to_tile_x(lon: float, z: int) -> int:
    n = 2**z
    return math.floor(((lon + 180.0) / 360.0) * n)


def lat_to_tile_y(lat: float, z: int) -> int:
    lat = clamp(lat, -WEB_MERCATOR_MAX_LAT, WEB_MERCATOR_MAX_LAT)
    lat_rad = math.radians(lat)
    n = 2**z
    return math.floor(((1 - math.log(math.tan(lat_rad) + 1 / math.cos(lat_rad)) / math.pi) / 2) * n)


def tile_range_for_view(z: int, west: float, east: float, north: float, south: float) -> TileRange:
    n = 2**z
    min_x = max(0, min(n - 1, lon_to_tile_x(west, z)))
    max_x = max(0, min(n - 1, lon_to_tile_x(east, z)))
    min_y = max(0, min(n - 1, lat_to_tile_y(north, z)))
    max_y = max(0, min(n - 1, lat_to_tile_y(south, z)))
    return TileRange(z=z, min_x=min_x, max_x=max_x, min_y=min_y, max_y=max_y)


def generate_intro_frames(fps: int) -> list[tuple[float, float, float, int]]:
    """Return [(zoom, lng, lat, stage_index)]."""
    frames: list[tuple[float, float, float, int]] = []
    segments = len(CHECKPOINTS) - 1
    segment_duration_ms = round(INTRO_DURATION_MS / segments)

    for i in range(segments):
        from_zoom = CHECKPOINTS[i]
        to_zoom = CHECKPOINTS[i + 1]
        from_t = i / segments
        to_t = (i + 1) / segments

        frame_count = max(1, round((segment_duration_ms / 1000.0) * fps))
        for f in range(frame_count):
            local = f / frame_count
            eased_local = ease_smoothstep(local)
            global_t = from_t + (to_t - from_t) * eased_local

            zoom = from_zoom + (to_zoom - from_zoom) * eased_local
            angle = global_t * math.pi * 2
            radius_deg = 0.0012 * (1 - global_t)
            lng = INTRO_CENTER[0] + (math.cos(angle) * radius_deg) / max(0.2, math.cos(math.radians(INTRO_CENTER[1])))
            lat = INTRO_CENTER[1] + math.sin(angle) * radius_deg
            frames.append((zoom, lng, lat, i))

        # Ensure segment end checkpoint frame is represented (where stage prefetch happens).
        global_t = to_t
        angle = global_t * math.pi * 2
        radius_deg = 0.0012 * (1 - global_t)
        lng = INTRO_CENTER[0] + (math.cos(angle) * radius_deg) / max(0.2, math.cos(math.radians(INTRO_CENTER[1])))
        lat = INTRO_CENTER[1] + math.sin(angle) * radius_deg
        frames.append((to_zoom, lng, lat, i))

    return frames


def simulate_batch_keys(
    fps: int,
    width_px: int,
    height_px: int,
    freeze_visible_range_per_stage: bool,
    freeze_visible_range_per_zoom: bool,
) -> dict[str, object]:
    """Model neighborhood batch key creation for z>=15.

    Assumptions:
    - At least one tile request occurs each frame for the active rounded zoom.
    - useDuckDB neighborhood batch key is revision + z + visible range.
    - prefetchVisibleDetailTilesAtZoom is called at each stage boundary for z>=15.
    """
    revision = 1
    seen_keys: set[str] = set()
    keys_by_zoom: dict[int, set[str]] = defaultdict(set)
    count_by_zoom: Counter[int] = Counter()

    frames = generate_intro_frames(fps)
    frozen_range_by_stage_zoom: dict[tuple[int, int], TileRange] = {}
    frozen_range_by_zoom: dict[int, TileRange] = {}

    for zoom, lng, lat, stage_idx in frames:
        z = round(zoom)
        if z < 15:
            continue

        west, east, north, south = approx_bounds(lng, lat, zoom, width_px, height_px)
        tr = tile_range_for_view(z, west, east, north, south)

        if freeze_visible_range_per_zoom:
            if z not in frozen_range_by_zoom:
                frozen_range_by_zoom[z] = tr
            tr = frozen_range_by_zoom[z]
        elif freeze_visible_range_per_stage:
            skey = (stage_idx, z)
            if skey not in frozen_range_by_stage_zoom:
                frozen_range_by_stage_zoom[skey] = tr
            tr = frozen_range_by_stage_zoom[skey]

        key = tr.key(revision=revision)
        if key not in seen_keys:
            seen_keys.add(key)
            keys_by_zoom[z].add(key)
            count_by_zoom[z] += 1

    # Stage-boundary prefetch calls (same dedupe key path).
    for cp in CHECKPOINTS[1:]:
        z = round(cp)
        if z < 15:
            continue
        # prefetch uses current visible range; in this model it is already represented
        # by the final frame(s). If a missing key appears, add it.
        zoom = cp
        global_t = CHECKPOINTS.index(cp) / (len(CHECKPOINTS) - 1)
        angle = global_t * math.pi * 2
        radius_deg = 0.0012 * (1 - global_t)
        lng = INTRO_CENTER[0] + (math.cos(angle) * radius_deg) / max(0.2, math.cos(math.radians(INTRO_CENTER[1])))
        lat = INTRO_CENTER[1] + math.sin(angle) * radius_deg
        west, east, north, south = approx_bounds(lng, lat, zoom, width_px, height_px)
        tr = tile_range_for_view(z, west, east, north, south)

        if freeze_visible_range_per_zoom:
            tr = frozen_range_by_zoom.get(z, tr)
        elif freeze_visible_range_per_stage:
            stage_idx = CHECKPOINTS.index(cp) - 1
            skey = (stage_idx, z)
            tr = frozen_range_by_stage_zoom.get(skey, tr)

        key = tr.key(revision=revision)
        if key not in seen_keys:
            seen_keys.add(key)
            keys_by_zoom[z].add(key)
            count_by_zoom[z] += 1

    total = sum(count_by_zoom.values())
    return {
        "fps": fps,
        "viewport": f"{width_px}x{height_px}",
        "freeze_visible_range_per_stage": freeze_visible_range_per_stage,
        "freeze_visible_range_per_zoom": freeze_visible_range_per_zoom,
        "total_estimated_queries_z15_plus": total,
        "by_zoom": {z: count_by_zoom[z] for z in sorted(count_by_zoom.keys(), reverse=True)},
    }


def parse_log_file(path: Path) -> dict[str, object]:
    if not path.exists():
        raise FileNotFoundError(path)

    total_sql = 0
    by_zoom_total: Counter[int] = Counter()
    by_zoom_unique_ranges: dict[int, set[tuple[int, int, int, int]]] = defaultdict(set)

    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue

        sql_text = ""
        if line.startswith("{") and line.endswith("}"):
            try:
                obj = json.loads(line)
                value = obj.get("value")
                if isinstance(value, str):
                    sql_text = value
            except Exception:
                continue
        else:
            # fallback: raw sql line chunks unsupported; skip
            continue

        if "ST_AsMVT" not in sql_text:
            continue

        z_match = SQL_Z_RE.search(sql_text)
        x_match = SQL_X_RE.search(sql_text)
        y_match = SQL_Y_RE.search(sql_text)
        if not (z_match and x_match and y_match):
            continue

        z = int(z_match.group(1))
        min_x, max_x = int(x_match.group(1)), int(x_match.group(2))
        min_y, max_y = int(y_match.group(1)), int(y_match.group(2))

        total_sql += 1
        by_zoom_total[z] += 1
        by_zoom_unique_ranges[z].add((min_x, max_x, min_y, max_y))

    return {
        "log_file": str(path),
        "total_mvt_sql_entries": total_sql,
        "by_zoom_total": {z: by_zoom_total[z] for z in sorted(by_zoom_total.keys(), reverse=True)},
        "by_zoom_unique_range_keys": {
            z: len(by_zoom_unique_ranges[z]) for z in sorted(by_zoom_unique_ranges.keys(), reverse=True)
        },
    }


def print_recommendation(sim_dynamic: dict[str, object], sim_frozen: dict[str, object], parsed: dict[str, object] | None) -> None:
    print("\nRecommendation:")
    print("- z13/z14 aggregate caches are useful and already aligned with current code.")
    print("- For z16-z18, aggregates are usually not ideal because icon/category fidelity is needed.")
    print("- If query count is still high at z16-z18, prefer one of:")
    print("  1) Stage-lock visible range during intro (closest to one bulk query per LOD stage).")
    print("  2) Prebuild feature tables for z16-z18 (similar to z15 strategy) and do range lookups only.")
    print("  3) Keep current path but reduce intro frame-driven range changes (fewer range signatures).")

    if parsed:
        by_zoom = parsed.get("by_zoom_unique_range_keys", {})
        if isinstance(by_zoom, dict):
            z16_18 = sum(int(v) for k, v in by_zoom.items() if int(k) in (16, 17, 18))
            print(f"- Observed unique range keys in logs for z16-18: {z16_18}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fps", type=int, default=60)
    parser.add_argument("--width", type=int, default=1512, help="Viewport width in CSS pixels")
    parser.add_argument("--height", type=int, default=982, help="Viewport height in CSS pixels")
    parser.add_argument("--log-file", type=Path, default=None, help="Optional JSON/NDJSON console export with SQL in .value")
    args = parser.parse_args()

    sim_dynamic = simulate_batch_keys(
        fps=args.fps,
        width_px=args.width,
        height_px=args.height,
        freeze_visible_range_per_stage=False,
        freeze_visible_range_per_zoom=False,
    )
    sim_frozen = simulate_batch_keys(
        fps=args.fps,
        width_px=args.width,
        height_px=args.height,
        freeze_visible_range_per_stage=True,
        freeze_visible_range_per_zoom=False,
    )
    sim_zoom_locked = simulate_batch_keys(
        fps=args.fps,
        width_px=args.width,
        height_px=args.height,
        freeze_visible_range_per_stage=False,
        freeze_visible_range_per_zoom=True,
    )

    print("Simulation (current behavior, frame-updated visible range):")
    print(json.dumps(sim_dynamic, indent=2))
    print("\nSimulation (stage-locked visible range):")
    print(json.dumps(sim_frozen, indent=2))
    print("\nSimulation (zoom-locked visible range):")
    print(json.dumps(sim_zoom_locked, indent=2))

    parsed = None
    if args.log_file:
        parsed = parse_log_file(args.log_file)
        print("\nObserved from log file:")
        print(json.dumps(parsed, indent=2))

    print_recommendation(sim_dynamic, sim_frozen, parsed)


if __name__ == "__main__":
    main()
