#!/usr/bin/env python3
"""Build static fuel-estimate tiles for TankPrijzen (hosted on GitHub Pages).

The API runs on a free Render instance that sleeps; a cold start takes up to a minute.
These tiles are plain JSON files on a host that never sleeps, so the app can show
stations with an *estimated* price immediately and replace them with live prices
once the API answers.

Same grid as the charger tiles: x = floor(lng / 0.18), y = floor(lat / 0.12).

    {out}/v1/index.json
    {out}/v1/cells/{x}/{y}.json   {"s": [[id, lat, lng, brand, name, tier, e10, euro98, diesel, lpg], ...]}

Prices are EUR per litre, estimates only (CBS daily average + EU Weekly Oil Bulletin,
tier and brand adjusted by api/estimate.py). They are never live pump prices.

Usage (from the repo root, next to estimate.py and data/):
    python scripts/build_fuel_tiles.py --out public --min-count 90000
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import shutil
import sys
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite
import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import estimate as est  # noqa: E402

CELL_LAT = 0.12
CELL_LNG = 0.18
# Countries the app covers.
COUNTRIES = {"NL", "BE", "LU", "DE", "FR", "ES", "IT", "PT", "AT"}
FUELS = ("e10", "euro98", "diesel", "lpg")
FIELDS = ["id", "lat", "lng", "brand", "name", "tier", *FUELS]


def cell_of(lat: float, lng: float) -> tuple[int, int]:
    return math.floor(lng / CELL_LNG), math.floor(lat / CELL_LAT)


async def build(out: Path, min_count: int, refresh: bool) -> int:
    # A file DB (not :memory:) matches how the API runs this code.
    workdir = Path(tempfile.mkdtemp(prefix="fuel-tiles-"))
    db = await aiosqlite.connect(workdir / "work.db")
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    await db.commit()
    await est.ensure_estimate_schema(db)
    await est.load_cbs_seed(db)
    if refresh:
        try:
            async with httpx.AsyncClient(
                timeout=60.0, follow_redirects=True, headers={"User-Agent": "TankPrijzen-tiles/1.0"}
            ) as client:
                await est.refresh_market_data(db, client)
        except Exception as exc:  # noqa: BLE001
            print(f"market refresh failed, using bundled seeds: {exc}", file=sys.stderr)
        est.clear_cbs_mem_cache()
    await est.ensure_osm_bootstrap(db)

    cbs = await est.load_cbs_baselines(db)
    periods = sorted({p for _d, p, _f in cbs.values() if p})

    async with db.execute(
        "SELECT id, lat, lng, brand, name, location_tier, country FROM osm_stations"
    ) as cur:
        rows = await cur.fetchall()

    cells: dict[tuple[int, int], list[list]] = defaultdict(list)
    seen: set[str] = set()
    count = 0
    for row in rows:
        sid = str(row["id"])
        if sid in seen:
            continue
        seen.add(sid)
        lat, lng = float(row["lat"]), float(row["lng"])
        cc = est.country_for_point(lat, lng, tagged=row["country"])
        if cc not in COUNTRIES:
            continue
        prices: list[float | None] = []
        for fuel in FUELS:
            price, _source = est.estimate_from_baselines(
                fuel_key=fuel,
                brand=row["brand"],
                name=row["name"],
                lat=lat,
                lng=lng,
                location_tier=row["location_tier"],
                country=cc,
                cbs=cbs,
            )
            prices.append(round(price, 3) if price is not None else None)
        if all(p is None for p in prices):
            continue
        cells[cell_of(lat, lng)].append(
            [sid, round(lat, 5), round(lng, 5), row["brand"], row["name"], row["location_tier"], *prices]
        )
        count += 1

    await db.close()
    shutil.rmtree(workdir, ignore_errors=True)
    if count < min_count:
        print(f"refusing to publish: {count} stations < {min_count}", file=sys.stderr)
        return 1

    root = out / "v1"
    if root.exists():
        shutil.rmtree(root)
    (root / "cells").mkdir(parents=True)
    for (x, y), stations in cells.items():
        stations.sort(key=lambda s: s[0])
        path = root / "cells" / str(x) / f"{y}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"s": stations}, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    index = {
        "v": 1,
        "built": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "cell": [CELL_LAT, CELL_LNG],
        "fields": FIELDS,
        "countries": sorted(COUNTRIES),
        "stations": count,
        "cells": len(cells),
        "baseline_periods": periods,
        "note": "Estimated prices (EUR/L), not live pump prices.",
    }
    (root / "index.json").write_text(json.dumps(index, separators=(",", ":")), encoding="utf-8")
    (out / ".nojekyll").write_text("", encoding="utf-8")
    print(f"built {count} stations in {len(cells)} cells -> {root}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="public")
    ap.add_argument("--min-count", type=int, default=90000)
    ap.add_argument("--no-refresh", action="store_true", help="use bundled CBS/WOB seeds only")
    args = ap.parse_args()
    return asyncio.run(build(Path(args.out), args.min_count, refresh=not args.no_refresh))


if __name__ == "__main__":
    sys.exit(main())
