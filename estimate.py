"""CBS + OSM fuel price estimation for distributable NL/DE coverage.

Data sources (CC BY / ODbL / CC BY 4.0):
- CBS 80416NED — daily national pump averages (euro95, diesel, lpg) for NL
- CBS 81567NED — monthly tier breakdown (highway / non-highway / unmanned)
- OpenStreetMap — NL + DE amenity=fuel station locations (Overpass API)
- EU Weekly Oil Bulletin — weekly national averages incl. taxes (EU27, free reuse)
- Germany live pumps: Tankerkönig / MTS-K via the API layer (not this module)
- Germany fallback estimates: CBS daily × DE/NL ratio (Oil Bulletin) × tier/brand
"""

from __future__ import annotations

import asyncio
import gzip
import io
import json
import math
import re
import time
import zipfile
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Literal, Optional

import aiosqlite
import httpx


CBS_DAILY_URL = "https://opendata.cbs.nl/ODataApi/OData/80416NED/TypedDataSet"
CBS_TIERS_URL = "https://opendata.cbs.nl/ODataApi/OData/81567NED/TypedDataSet"
OVERPASS_URLS = (
    "https://overpass.kumi.systems/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://overpass-api.de/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
)
OVERPASS_HEADERS = {
    "User-Agent": "TankPrijzen/1.0 (OSM ODbL; amenity=fuel + charging_station)",
}

# Bundled amenity=fuel snapshots for free-tier cold starts (Overpass often times out).
OSM_SEED_PATH = Path(__file__).resolve().parent / "data" / "osm_nl_fuel.json.gz"
OSM_DE_SEED_PATH = Path(__file__).resolve().parent / "data" / "osm_de_fuel.json.gz"
OSM_FUEL_DIR = Path(__file__).resolve().parent / "data" / "osm_fuel"
WOB_SEED_PATH = Path(__file__).resolve().parent / "data" / "wob_seed.json"
WORLD_AVG_PATH = Path(__file__).resolve().parent / "data" / "world_avg_seed.json"
# NL and DE have their own seed files. Mainland China is never loaded.
_SKIP_FEED_COUNTRIES = {"NL", "DE", "CN"}
# Last CBS rows known at build time; loaded when SQLite has nothing newer
# (Render free disks are wiped on redeploy and opendata.cbs.nl is often unreachable).
CBS_SEED_PATH = Path(__file__).resolve().parent / "data" / "cbs_seed.json"

# "Prices with taxes — latest" workbook; the document id is a permanent link that is overwritten weekly.
WOB_LATEST_URL = (
    "https://energy.ec.europa.eu/document/download/264c2d0f-f161-4ea3-a777-78faae59bea0_en"
)
WOB_TTL_SEC = 24 * 60 * 60
# Oil Bulletin "Euro-super 95" for DE is mostly Super E5; E10 trades ~6 ct lower.
DE_E10_BELOW_E5_EUR = 0.06
WOB_RATIO_BOUNDS = (0.70, 1.40)

CBS_DAILY_TTL_SEC = 24 * 60 * 60
CBS_TIERS_TTL_SEC = 7 * 24 * 60 * 60
OSM_TTL_SEC = 7 * 24 * 60 * 60
# Background loop cadence; each tick is a no-op unless a TTL expired or the last fetch failed.
BACKGROUND_REFRESH_SEC = 3 * 60 * 60

# CBS Motorbrandstof codes → our fuel keys
CBS_FUEL_CODES: dict[str, str] = {
    "A047220": "e10",  # Euro95
    "A047219": "diesel",
    "A047221": "lpg",
}

LocationTier = Literal["highway", "manned", "unmanned"]

# Brand multipliers applied AFTER CBS tier (residual vs daily×tier).
# Calibrated on cached NL pump samples used ONLY for multiplier fit — never served
# as live prices in DISTRIBUTION_MODE. Typical residual error after fit: ~±€0.05
# median / ~±€0.08 MAE on non-highway; outliers (wrong tier, promo) up to ~±€0.15.
BRAND_FACTORS: dict[str, float] = {
    "tinq": 0.993,
    "tango": 1.015,
    "argos": 1.001,
    "anwb": 0.990,
    "total": 1.019,
    "totalenergies": 1.019,
    "bp": 1.032,
    "shell": 1.018,
    "avia": 1.018,
    "texaco": 1.020,
    "esso": 1.008,
    "q8": 1.005,
    "gulf": 1.010,
    "tamoil": 1.003,
    "star": 0.990,
    "makro": 0.975,
    "jumbo": 0.985,
    "lukoil": 0.995,
    "firezone": 0.990,
    "de haan": 1.021,
    "dehaan": 1.021,
    "haan": 1.021,
    "ok": 1.020,
    "fieten": 0.975,
    "berkman": 0.995,
    "kuster": 0.998,
}

DEFAULT_BRAND_FACTOR = 1.0

# Typical NL Super (E5/98) vs Euro95 (E10) so euro98 is never blank.
NL_EURO98_FACTOR = 1.07

# Last-resort CBS daily averages (EUR/L) if neither SQLite nor cbs_seed.json has rows
# (CBS 80416NED, 14 Sep 2026).
CBS_FALLBACK_DAILY: dict[str, float] = {
    "e10": 2.434,
    "diesel": 2.494,
    "lpg": 0.936,
}
CBS_FALLBACK_TIERS: dict[LocationTier, float] = {
    "manned": 0.997,
    "highway": 1.096,
    "unmanned": 0.976,
}

# DE/NL pump ratios when the Oil Bulletin is unavailable and no Tankerkönig key is set
# (Weekly Oil Bulletin 21 Sep 2026: DE E5 2.348 / diesel 2.457 / LPG 1.109 vs
# NL 2.449 / 2.579 / 0.946). Live DE prices come from Tankerkönig in main.py.
DE_NL_RATIO: dict[str, float] = {
    "e10": 0.934,
    "euro98": 0.959,  # Super E5 vs NL Euro95 baseline
    "diesel": 0.953,
    "lpg": 1.17,
}
DE_BRAND_FACTORS: dict[str, float] = {
    "aral": 1.030,
    "shell": 1.025,
    "esso": 1.012,
    "total": 1.015,
    "totalenergies": 1.015,
    "bp": 1.020,
    "jet": 0.970,
    "star": 0.972,
    "hem": 0.975,
    "classic": 0.978,
    "agip": 1.000,
    "eni": 1.000,
    "westfalen": 0.998,
    "bft": 0.988,
    "omv": 1.010,
    "q1": 0.980,
    "oil!": 0.975,
    "oil": 0.978,
    "avia": 1.000,
    "hoyer": 0.985,
    "sb": 0.968,
}
DE_DEFAULT_TIER_FACTORS: dict[str, float] = {
    "manned": 1.0,
    "highway": 1.12,
    "unmanned": 0.97,
}
DE_ESTIMATE_ERROR_BAND_EUR = (0.10, 0.18)

# Expected indication accuracy band (honest UX copy): typically within ±€0.05–0.10/L
# of the pump for city stations; highway/mis-tier cases can be wider.
ESTIMATE_ERROR_BAND_EUR = (0.05, 0.10)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


async def ensure_estimate_schema(db: aiosqlite.Connection) -> None:
    """Add estimation tables (schema v3 migration, non-destructive)."""
    await db.executescript(
        """
        CREATE TABLE IF NOT EXISTS cbs_daily (
            period TEXT NOT NULL,
            fuel_key TEXT NOT NULL,
            price REAL NOT NULL,
            fetched_at REAL NOT NULL,
            PRIMARY KEY (period, fuel_key)
        );
        CREATE TABLE IF NOT EXISTS cbs_tiers (
            period TEXT NOT NULL,
            fuel_key TEXT NOT NULL,
            avg_price REAL NOT NULL,
            highway REAL,
            manned REAL,
            unmanned REAL,
            fetched_at REAL NOT NULL,
            PRIMARY KEY (period, fuel_key)
        );
        CREATE TABLE IF NOT EXISTS osm_stations (
            id TEXT PRIMARY KEY,
            osm_type TEXT NOT NULL,
            osm_id INTEGER NOT NULL,
            lat REAL NOT NULL,
            lng REAL NOT NULL,
            brand TEXT,
            name TEXT,
            operator TEXT,
            location_tier TEXT NOT NULL DEFAULT 'manned',
            tags_json TEXT,
            fetched_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_osm_stations_bbox ON osm_stations(lat, lng);
        CREATE TABLE IF NOT EXISTS wob_prices (
            country TEXT NOT NULL,
            fuel_key TEXT NOT NULL,
            price REAL NOT NULL,
            week TEXT NOT NULL,
            fetched_at REAL NOT NULL,
            PRIMARY KEY (country, fuel_key)
        );
        """
    )
    await _ensure_column(db, "osm_stations", "country", "TEXT NOT NULL DEFAULT 'NL'")


async def _ensure_column(
    db: aiosqlite.Connection, table: str, column: str, ddl: str
) -> None:
    async with db.execute(f"PRAGMA table_info({table})") as cur:
        cols = {str(row[1]) for row in await cur.fetchall()}
    if column not in cols:
        await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


async def meta_get(db: aiosqlite.Connection, key: str) -> Optional[str]:
    async with db.execute("SELECT value FROM meta WHERE key = ?", (key,)) as cur:
        row = await cur.fetchone()
        return row["value"] if row else None


async def meta_set(db: aiosqlite.Connection, key: str, value: str) -> None:
    await db.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


async def fetch_cbs_daily(client: httpx.AsyncClient) -> list[dict[str, Any]]:
    """Fetch latest CBS daily national averages."""
    # OData $orderby on Perioden is unreliable; fetch recent window and pick max.
    since = (date.today() - timedelta(days=60)).strftime("%Y%m%d")
    resp = await client.get(
        CBS_DAILY_URL,
        params={"$filter": f"Perioden ge '{since}'", "$top": 500},
        timeout=30.0,
    )
    resp.raise_for_status()
    rows = resp.json().get("value") or []
    if not rows:
        return []
    latest = max(str(r["Perioden"]) for r in rows)
    row = next(r for r in rows if str(r["Perioden"]) == latest)
    period = str(row["Perioden"])
    out: list[dict[str, Any]] = []
    mapping = [
        ("e10", row.get("BenzineEuro95_1")),
        ("diesel", row.get("Diesel_2")),
        ("lpg", row.get("Lpg_3")),
    ]
    for fuel_key, price in mapping:
        if price is not None:
            out.append({"period": period, "fuel_key": fuel_key, "price": float(price)})
    return out


async def fetch_cbs_tiers(client: httpx.AsyncClient) -> list[dict[str, Any]]:
    """Fetch latest CBS monthly tier breakdown per fuel."""
    resp = await client.get(
        CBS_TIERS_URL,
        params={"$filter": f"Perioden ge '{date.today().year - 1}'", "$top": 500},
        timeout=30.0,
    )
    resp.raise_for_status()
    rows = resp.json().get("value") or []
    if not rows:
        return []

    latest_period = max(str(r["Perioden"]) for r in rows)
    out: list[dict[str, Any]] = []
    for row in rows:
        if str(row["Perioden"]) != latest_period:
            continue
        fuel_key = CBS_FUEL_CODES.get(str(row["Motorbrandstof"]))
        if not fuel_key:
            continue
        avg = row.get("GemiddeldePompprijs_1")
        if avg is None:
            continue
        out.append(
            {
                "period": latest_period,
                "fuel_key": fuel_key,
                "avg_price": float(avg),
                "highway": _float_or_none(row.get("PompprijsSnelwegBemandStation_2")),
                "manned": _float_or_none(row.get("PompprijsNietSnelwegBemandStation_3")),
                "unmanned": _float_or_none(row.get("PompprijsNietSnelwegOnbemandStation_4")),
            }
        )
    return out


def _float_or_none(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


async def store_cbs_daily(db: aiosqlite.Connection, rows: list[dict[str, Any]]) -> None:
    now = time.time()
    for row in rows:
        await db.execute(
            """
            INSERT INTO cbs_daily(period, fuel_key, price, fetched_at)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(period, fuel_key) DO UPDATE SET
                price = excluded.price, fetched_at = excluded.fetched_at
            """,
            (row["period"], row["fuel_key"], row["price"], now),
        )
    if rows:
        await meta_set(db, "cbs_daily_fetched_at", str(now))
        await meta_set(db, "cbs_daily_period", rows[0]["period"])


async def store_cbs_tiers(db: aiosqlite.Connection, rows: list[dict[str, Any]]) -> None:
    now = time.time()
    for row in rows:
        await db.execute(
            """
            INSERT INTO cbs_tiers(period, fuel_key, avg_price, highway, manned, unmanned, fetched_at)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(period, fuel_key) DO UPDATE SET
                avg_price = excluded.avg_price,
                highway = excluded.highway,
                manned = excluded.manned,
                unmanned = excluded.unmanned,
                fetched_at = excluded.fetched_at
            """,
            (
                row["period"],
                row["fuel_key"],
                row["avg_price"],
                row.get("highway"),
                row.get("manned"),
                row.get("unmanned"),
                now,
            ),
        )
    if rows:
        await meta_set(db, "cbs_tiers_fetched_at", str(now))
        await meta_set(db, "cbs_tiers_period", rows[0]["period"])


async def refresh_cbs(
    db: aiosqlite.Connection,
    client: httpx.AsyncClient,
    *,
    force: bool = False,
) -> dict[str, Any]:
    """Refresh CBS daily + tier data if stale."""
    now = time.time()
    daily_at = float(await meta_get(db, "cbs_daily_fetched_at") or 0)
    tiers_at = float(await meta_get(db, "cbs_tiers_fetched_at") or 0)

    daily_refreshed = tiers_refreshed = False
    daily_rows: list[dict[str, Any]] = []
    tier_rows: list[dict[str, Any]] = []

    if force or now - daily_at > CBS_DAILY_TTL_SEC:
        daily_rows = await fetch_cbs_daily(client)
        if daily_rows:
            await store_cbs_daily(db, daily_rows)
            daily_refreshed = True

    if force or now - tiers_at > CBS_TIERS_TTL_SEC:
        tier_rows = await fetch_cbs_tiers(client)
        if tier_rows:
            await store_cbs_tiers(db, tier_rows)
            tiers_refreshed = True

    await db.commit()
    if daily_refreshed or tiers_refreshed:
        clear_cbs_mem_cache()
    return {
        "daily_refreshed": daily_refreshed,
        "tiers_refreshed": tiers_refreshed,
        "daily_period": daily_rows[0]["period"] if daily_rows else await meta_get(db, "cbs_daily_period"),
        "tiers_period": tier_rows[0]["period"] if tier_rows else await meta_get(db, "cbs_tiers_period"),
        "daily_count": len(daily_rows) if daily_rows else None,
        "tiers_count": len(tier_rows) if tier_rows else None,
    }


async def load_cbs_seed(db: aiosqlite.Connection) -> int:
    """Insert bundled CBS rows without touching fetched_at, so live refresh still runs."""
    if not CBS_SEED_PATH.is_file():
        return 0
    seed = json.loads(CBS_SEED_PATH.read_text(encoding="utf-8"))
    count = 0
    for row in seed.get("daily", []):
        await db.execute(
            "INSERT OR IGNORE INTO cbs_daily(period, fuel_key, price, fetched_at) VALUES(?, ?, ?, 0)",
            (row["period"], row["fuel_key"], float(row["price"])),
        )
        count += 1
    for row in seed.get("tiers", []):
        await db.execute(
            """
            INSERT OR IGNORE INTO cbs_tiers(period, fuel_key, avg_price, highway, manned, unmanned, fetched_at)
            VALUES(?, ?, ?, ?, ?, ?, 0)
            """,
            (
                row["period"],
                row["fuel_key"],
                float(row["avg_price"]),
                row.get("highway"),
                row.get("manned"),
                row.get("unmanned"),
            ),
        )
        count += 1
    await db.commit()
    clear_cbs_mem_cache()
    return count


_WOB_COUNTRIES: dict[str, str] = {
    "Austria": "AT",
    "Belgium": "BE",
    "Croatia": "HR",
    "Denmark": "DK",
    "France": "FR",
    "Germany": "DE",
    "Greece": "GR",
    "Italy": "IT",
    "Luxembourg": "LU",
    "Netherlands": "NL",
    "Portugal": "PT",
    "Slovenia": "SI",
    "Spain": "ES",
}
# Sheet columns: B Euro-super 95, C automotive diesel, G LPG (EUR per 1000 L).
_WOB_COLUMNS: dict[str, str] = {"B": "e95", "C": "diesel", "G": "lpg"}


def parse_wob_xlsx(content: bytes) -> tuple[Optional[str], list[dict[str, Any]]]:
    """Parse the Oil Bulletin workbook with stdlib only → (week YYYYMMDD, rows EUR/L)."""
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        shared_xml = zf.read("xl/sharedStrings.xml").decode("utf-8")
        sheet_xml = zf.read("xl/worksheets/sheet1.xml").decode("utf-8")
    strings = [
        re.sub(r"<[^>]+>", "", m) for m in re.findall(r"<si>(.*?)</si>", shared_xml, re.S)
    ]
    week: Optional[str] = None
    out: list[dict[str, Any]] = []
    for row_xml in re.findall(r"<row[^>]*>(.*?)</row>", sheet_xml, re.S):
        cells: dict[str, str] = {}
        for attrs, body in re.findall(r"<c ([^>]*?)(?:/>|>(.*?)</c>)", row_xml, re.S):
            ref = re.search(r'r="([A-Z]+)\d+"', attrs)
            val = re.search(r"<v>(.*?)</v>", body or "")
            if not ref or not val:
                continue
            raw = val.group(1)
            cells[ref.group(1)] = strings[int(raw)] if 't="s"' in attrs else raw
        label = cells.get("A", "").strip()
        if week is None and re.fullmatch(r"\d{5}", label):
            week = (date(1899, 12, 30) + timedelta(days=int(label))).strftime("%Y%m%d")
            continue
        cc = _WOB_COUNTRIES.get(label)
        if not cc:
            continue
        for col, fuel_key in _WOB_COLUMNS.items():
            price = _float_or_none(cells.get(col))
            if price and 300 < price < 5000:
                out.append({"country": cc, "fuel_key": fuel_key, "price": round(price / 1000, 4)})
    return week, out


async def refresh_wob(
    db: aiosqlite.Connection,
    client: httpx.AsyncClient,
    *,
    force: bool = False,
) -> dict[str, Any]:
    """Refresh EU Weekly Oil Bulletin national averages (background only)."""
    fetched_at = float(await meta_get(db, "wob_fetched_at") or 0)
    if not force and time.time() - fetched_at < WOB_TTL_SEC:
        return {"refreshed": False, "week": await meta_get(db, "wob_week")}
    resp = await client.get(WOB_LATEST_URL, timeout=45.0, follow_redirects=True)
    resp.raise_for_status()
    week, rows = parse_wob_xlsx(resp.content)
    if not week or not any(r["country"] == "NL" for r in rows):
        return {"refreshed": False, "week": await meta_get(db, "wob_week")}
    now = time.time()
    for row in rows:
        await db.execute(
            """
            INSERT INTO wob_prices(country, fuel_key, price, week, fetched_at)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(country, fuel_key) DO UPDATE SET
                price = excluded.price, week = excluded.week, fetched_at = excluded.fetched_at
            """,
            (row["country"], row["fuel_key"], row["price"], week, now),
        )
    await meta_set(db, "wob_fetched_at", str(now))
    await meta_set(db, "wob_week", week)
    await db.commit()
    clear_cbs_mem_cache()
    replace_wob_mem(week, rows)
    return {"refreshed": True, "week": week, "count": len(rows)}


async def refresh_market_data(db: aiosqlite.Connection, client: httpx.AsyncClient) -> None:
    """One background tick: CBS + Oil Bulletin, each TTL-gated and failure-isolated."""
    for refresh in (refresh_cbs, refresh_wob):
        try:
            await refresh(db, client)
        except Exception:
            pass


async def _load_wob(
    db: aiosqlite.Connection, country: str
) -> tuple[Optional[str], dict[str, float]]:
    try:
        async with db.execute(
            "SELECT fuel_key, price, week FROM wob_prices WHERE country = ?", (country,)
        ) as cur:
            rows = await cur.fetchall()
    except Exception:
        return None, {}
    if not rows:
        return None, {}
    return str(rows[0]["week"]), {str(r["fuel_key"]): float(r["price"]) for r in rows}


def de_nl_ratios_from_wob(nl: dict[str, float], de: dict[str, float]) -> dict[str, float]:
    """Per-fuel DE/NL ratios from national averages; bad or missing values fall back to static."""
    pairs = {
        "e10": (de.get("e95", 0) - DE_E10_BELOW_E5_EUR, nl.get("e95")),
        "euro98": (de.get("e95"), nl.get("e95")),
        "diesel": (de.get("diesel"), nl.get("diesel")),
        "lpg": (de.get("lpg"), nl.get("lpg")),
    }
    out = dict(DE_NL_RATIO)
    lo, hi = WOB_RATIO_BOUNDS
    for fuel_key, (de_price, nl_price) in pairs.items():
        if de_price and nl_price and de_price > 0:
            ratio = de_price / nl_price
            if lo <= ratio <= hi:
                out[fuel_key] = round(ratio, 4)
    return out


_de_ratios: dict[str, float] = dict(DE_NL_RATIO)
_wob_mem: dict[str, dict[str, float]] = {}
_wob_mem_week: str = ""
# Highway stations sit above the national Oil Bulletin average. 1.07 matches the
# bundled check for a highway diesel estimate; manned stations use the average.
WOB_TIER_FACTORS: dict[str, float] = {"manned": 1.0, "highway": 1.07, "unmanned": 0.97}
WOB_BRAND_FACTORS: dict[str, float] = {"omv": 1.015}


def estimate_wob_price(
    *,
    country: Optional[str],
    fuel_key: str,
    brand: Optional[str],
    location_tier: Optional[str] = None,
) -> tuple[Optional[float], Optional[str]]:
    """National Oil Bulletin average, adjusted by tier and brand. Always an estimate."""
    cc = (country or "").strip().upper()
    if cc == "CN" or not cc:
        return None, None
    table = _wob_mem.get(cc) or {}
    bulletin_key = {"e10": "e95", "euro98": "e95", "diesel": "diesel", "lpg": "lpg"}.get(fuel_key)
    base = table.get(bulletin_key or "")
    if base is None:
        return None, None
    if fuel_key == "euro98":
        base *= NL_EURO98_FACTOR
    tier_factor = WOB_TIER_FACTORS.get(location_tier or "manned", 1.0)
    price = round(base * tier_factor * _lookup_brand_factor(brand, WOB_BRAND_FACTORS), 3)
    return price, f"estimate-wob:{_wob_mem_week}" if _wob_mem_week else "estimate-wob"


_world_mem: dict[str, dict[str, float]] = {}
_world_src: dict[str, dict[str, str]] = {}


def estimate_world_price(
    *,
    country: Optional[str],
    fuel_key: str,
    brand: Optional[str],
    location_tier: Optional[str] = None,
) -> tuple[Optional[float], Optional[str]]:
    """National average in EUR/L, same tier and brand adjustment as the Oil Bulletin.

    Always an estimate. The euro amount was not read off the pump.
    """
    cc = (country or "").strip().upper()
    if cc == "CN" or not cc:
        return None, None
    table = _world_mem.get(cc) or {}
    bulletin_key = {"e10": "e95", "euro98": "e95", "diesel": "diesel", "lpg": "lpg"}.get(fuel_key)
    base = table.get(bulletin_key or "")
    if base is None:
        return None, None
    if fuel_key == "euro98":
        base *= NL_EURO98_FACTOR
    tier_factor = WOB_TIER_FACTORS.get(location_tier or "manned", 1.0)
    price = round(base * tier_factor * _lookup_brand_factor(brand, WOB_BRAND_FACTORS), 3)
    label = (_world_src.get(cc) or {}).get(bulletin_key or "", "avg")
    return price, f"estimate-avg:{label}"


def load_world_avg_mem() -> None:
    """Bundled national averages already converted to EUR per litre."""
    global _world_mem, _world_src
    if not WORLD_AVG_PATH.is_file():
        return
    try:
        seed = json.loads(WORLD_AVG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return
    mem: dict[str, dict[str, float]] = {}
    src: dict[str, dict[str, str]] = {}
    for row in seed.get("rows") or []:
        cc = str(row.get("country") or "").upper()
        key = str(row.get("fuel_key") or "")
        if cc == "CN" or not cc or not key or row.get("price") is None:
            continue
        mem.setdefault(cc, {})[key] = float(row["price"])
        src.setdefault(cc, {})[key] = str(row.get("label") or "avg")
    if mem:
        _world_mem = mem
        _world_src = src


def feed_seed_countries() -> tuple[str, ...]:
    """Compact OSM seeds we can price. Unpriced countries stay out of SQLite."""
    priced = set(_wob_mem) | set(_world_mem)
    if not OSM_FUEL_DIR.is_dir():
        return ("BE", "FR", "ES", "IT")
    codes = []
    for path in sorted(OSM_FUEL_DIR.glob("*.json.gz")):
        cc = path.name[: -len(".json.gz")].upper()
        if len(cc) == 2 and cc.isalpha() and cc not in _SKIP_FEED_COUNTRIES and cc in priced:
            codes.append(cc)
    return tuple(codes) or ("BE", "FR", "ES", "IT")


def load_wob_seed_mem() -> None:
    """Fill in-memory Oil Bulletin prices from the bundled seed. DB refresh can replace them."""
    global _wob_mem, _wob_mem_week
    if not WOB_SEED_PATH.is_file():
        return
    try:
        seed = json.loads(WOB_SEED_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return
    mem: dict[str, dict[str, float]] = {}
    for row in seed.get("rows") or []:
        cc = str(row.get("country") or "").upper()
        key = str(row.get("fuel_key") or "")
        if cc and key and row.get("price") is not None:
            mem.setdefault(cc, {})[key] = float(row["price"])
    if mem:
        _wob_mem = mem
        _wob_mem_week = str(seed.get("week") or "")


def replace_wob_mem(week: str, rows: list[dict[str, Any]]) -> None:
    global _wob_mem, _wob_mem_week
    mem: dict[str, dict[str, float]] = {}
    for row in rows:
        cc = str(row.get("country") or "").upper()
        key = str(row.get("fuel_key") or "")
        if cc and key and row.get("price") is not None:
            mem.setdefault(cc, {})[key] = float(row["price"])
    if mem:
        _wob_mem = mem
        _wob_mem_week = week


def classify_osm_tags(tags: dict[str, str]) -> tuple[LocationTier, Optional[str], Optional[str], Optional[str]]:
    """Classify station from OSM tags."""
    brand = tags.get("brand") or tags.get("operator")
    name = tags.get("name")
    operator = tags.get("operator")

    tier: LocationTier = "manned"
    if tags.get("highway") in ("yes", "services", "rest_area"):
        tier = "highway"
    elif tags.get("self_service") == "yes" or tags.get("automated") == "yes":
        tier = "unmanned"
    elif tags.get("unmanned") == "yes":
        tier = "unmanned"

    return tier, brand, name, operator


def _lookup_brand_factor(brand: Optional[str], table: dict[str, float]) -> float:
    if not brand:
        return DEFAULT_BRAND_FACTOR
    key = brand.strip().lower()
    if key in table:
        return table[key]
    for pattern, factor in table.items():
        if pattern in key:
            return factor
    return DEFAULT_BRAND_FACTOR


def brand_factor(brand: Optional[str]) -> float:
    return _lookup_brand_factor(brand, BRAND_FACTORS)


def brand_factor_de(brand: Optional[str]) -> float:
    return _lookup_brand_factor(brand, DE_BRAND_FACTORS)


_cbs_mem: dict[str, tuple[tuple[Optional[float], Optional[str], dict[LocationTier, float]], float]] = {}
CBS_MEM_TTL_SEC = 300.0


def clear_cbs_mem_cache() -> None:
    _cbs_mem.clear()


async def load_cbs_baseline(
    db: aiosqlite.Connection, fuel_key: str
) -> tuple[Optional[float], Optional[str], dict[LocationTier, float]]:
    """Return (daily_price, period, tier_factors relative to daily baseline)."""
    now = time.time()
    hit = _cbs_mem.get(fuel_key)
    if hit and now - hit[1] < CBS_MEM_TTL_SEC:
        return hit[0]
    result = await _load_cbs_baseline_db(db, fuel_key)
    _cbs_mem[fuel_key] = (result, now)
    return result


async def load_cbs_baselines(
    db: aiosqlite.Connection,
    fuel_keys: tuple[str, ...] = ("e10", "diesel", "lpg"),
) -> dict[str, tuple[Optional[float], Optional[str], dict[LocationTier, float]]]:
    out: dict[str, tuple[Optional[float], Optional[str], dict[LocationTier, float]]] = {}
    for key in fuel_keys:
        out[key] = await load_cbs_baseline(db, key)
    return out


def estimate_from_baselines(
    *,
    fuel_key: str,
    brand: Optional[str],
    name: Optional[str],
    lat: float,
    lng: float,
    location_tier: Optional[str] = None,
    country: Optional[str] = None,
    cbs: dict[str, tuple[Optional[float], Optional[str], dict[LocationTier, float]]],
) -> tuple[Optional[float], Optional[str]]:
    """Pure estimate from preloaded CBS rows — no extra SQLite per station."""
    cc = country_for_point(lat, lng, tagged=country)
    tier = merge_location_tier(
        brand=brand,
        name=name,
        osm_tier=location_tier,
        location_tier=location_tier,
    )
    if cc == "DE":
        if fuel_key not in ("e10", "euro98", "diesel", "lpg"):
            return None, None
        baseline_key = "e10" if fuel_key == "euro98" else fuel_key
        daily, period, _tier_factors = cbs.get(baseline_key, (None, None, {}))
        if daily is None:
            return None, None
        return estimate_de_price(
            fuel_key=fuel_key,
            daily_baseline=daily,
            period=period,
            location_tier=tier,
            brand=brand,
        )
    if cc == "CN":
        return None, None
    if cc and cc != "NL":
        price, source = estimate_wob_price(
            country=cc, fuel_key=fuel_key, brand=brand, location_tier=tier
        )
        if price is not None:
            return price, source
        return estimate_world_price(
            country=cc, fuel_key=fuel_key, brand=brand, location_tier=tier
        )
    baseline_key = "e10" if fuel_key == "euro98" else fuel_key
    if baseline_key not in ("e10", "diesel", "lpg"):
        return None, None
    daily, period, tier_factors = cbs.get(baseline_key, (None, None, {}))
    if daily is None:
        return None, None
    baseline = daily * NL_EURO98_FACTOR if fuel_key == "euro98" else daily
    price = estimate_cbs_price(
        fuel_key=fuel_key,
        daily_baseline=baseline,
        tier_factors=tier_factors,
        location_tier=tier,
        brand=brand,
    )
    return price, f"estimate-cbs:{period}" if period else "estimate-cbs"


async def _load_cbs_baseline_db(
    db: aiosqlite.Connection, fuel_key: str
) -> tuple[Optional[float], Optional[str], dict[LocationTier, float]]:
    async with db.execute(
        """
        SELECT period, price FROM cbs_daily
        WHERE fuel_key = ?
        ORDER BY period DESC LIMIT 1
        """,
        (fuel_key,),
    ) as cur:
        daily_row = await cur.fetchone()

    nl_week, nl_wob = await _load_wob(db, "NL")
    de_week, de_wob = await _load_wob(db, "DE")
    if nl_wob and de_wob:
        _de_ratios.update(de_nl_ratios_from_wob(nl_wob, de_wob))

    if daily_row:
        daily_price: Optional[float] = float(daily_row["price"])
        period: Optional[str] = str(daily_row["period"])
    else:
        daily_price, period = CBS_FALLBACK_DAILY.get(fuel_key), "fallback"

    # CBS publishes daily rows weekly and its OData host is often down; when the
    # Oil Bulletin NL average is more than a week newer, it is the better baseline.
    wob_key = "e95" if fuel_key == "e10" else fuel_key
    if nl_week and wob_key in nl_wob:
        cbs_day = period if period and period.isdigit() else "00000000"
        if _days_between(cbs_day, nl_week) > 7:
            daily_price, period = nl_wob[wob_key], f"wob{nl_week}"

    if daily_price is None:
        return None, None, {}
    if not daily_row and not (period or "").startswith("wob"):
        return daily_price, period, dict(CBS_FALLBACK_TIERS)

    async with db.execute(
        """
        SELECT avg_price, highway, manned, unmanned FROM cbs_tiers
        WHERE fuel_key = ?
        ORDER BY period DESC LIMIT 1
        """,
        (fuel_key,),
    ) as cur:
        tier_row = await cur.fetchone()

    tier_factors: dict[LocationTier, float] = dict(CBS_FALLBACK_TIERS)
    if tier_row and tier_row["avg_price"]:
        avg = float(tier_row["avg_price"])
        if avg > 0:
            if tier_row["highway"]:
                tier_factors["highway"] = float(tier_row["highway"]) / avg
            if tier_row["manned"]:
                tier_factors["manned"] = float(tier_row["manned"]) / avg
            if tier_row["unmanned"]:
                tier_factors["unmanned"] = float(tier_row["unmanned"]) / avg

    # estimate = daily_baseline × tier_ratio × brand_factor (tier ratios from CBS monthly tiers)
    return daily_price, period, tier_factors


def _days_between(older: str, newer: str) -> int:
    try:
        a = date(int(older[:4]), int(older[4:6]), int(older[6:8]))
        b = date(int(newer[:4]), int(newer[4:6]), int(newer[6:8]))
    except ValueError:
        return 10_000
    return (b - a).days


def estimate_cbs_price(
    *,
    fuel_key: str,
    daily_baseline: float,
    tier_factors: dict[LocationTier, float],
    location_tier: LocationTier,
    brand: Optional[str],
) -> float:
    """estimate = daily_baseline * tier_factor * brand_factor"""
    tier_factor = tier_factors.get(location_tier, tier_factors.get("manned", 1.0))
    bf = brand_factor(brand)
    return round(daily_baseline * tier_factor * bf, 3)


def estimate_de_price(
    *,
    fuel_key: str,
    daily_baseline: float,
    period: Optional[str],
    location_tier: LocationTier,
    brand: Optional[str],
) -> tuple[float, str]:
    """DE fallback: NL CBS daily × DE/NL ratio (Oil Bulletin, weekly) × Autobahn/brand.

    Not live MTS-K prices. Typical indication band ~±€0.10–0.18/L.
    euro98 uses the CBS e10 baseline with the Super-E5 ratio.
    """
    ratio = _de_ratios.get(fuel_key) or _de_ratios["e10"]
    tier_factor = DE_DEFAULT_TIER_FACTORS.get(location_tier, 1.0)
    bf = brand_factor_de(brand)
    price = round(daily_baseline * ratio * tier_factor * bf, 3)
    source = f"estimate-de:{period}" if period else "estimate-de"
    return price, source


_HIGHWAY_NAME_RE = re.compile(
    r"(snelweg|highway|autobahn|autohof|raststätte|raststaette|raststaette|"
    r"\(\s*a\d{1,3}\b|"  # e.g. (A1 zz), (A12 nz), (A3)
    r"\ba\d{1,3}\s*[nzoew]\b|"  # A50 oz / A1 nz
    r"\ba\d{1,3}\s|"
    r"\bbab\s*\d)",
    re.IGNORECASE,
)


def name_suggests_highway(brand: Optional[str], name: Optional[str]) -> bool:
    combined = f"{brand or ''} {name or ''}"
    return bool(_HIGHWAY_NAME_RE.search(combined))


def classify_place_station(
    brand: Optional[str],
    name: Optional[str],
    *,
    location_tier: Optional[str] = None,
) -> LocationTier:
    """Heuristic classification for places (name/brand); OSM tier merged separately."""
    if location_tier == "highway" or name_suggests_highway(brand, name):
        return "highway"
    if location_tier in ("manned", "unmanned"):
        return location_tier  # type: ignore[return-value]

    combined = f"{brand or ''} {name or ''}".lower()
    unmanned_markers = (
        "unmanned",
        "onbemand",
        "unbemannt",
        "self-service",
        "self service",
        "automated",
        "sb-tank",
        "sb tank",
    )
    if any(m in combined for m in unmanned_markers):
        return "unmanned"
    # Discount unmanned networks — only when OSM did not already classify.
    if brand and brand.strip().lower() in ("tinq", "tango", "argos", "star", "firezone"):
        return "unmanned"
    return "manned"


def merge_location_tier(
    *,
    brand: Optional[str],
    name: Optional[str],
    osm_tier: Optional[str] = None,
    location_tier: Optional[str] = None,
) -> LocationTier:
    """Resolve tier: highway from name or OSM wins; else OSM; else brand/name heuristic.

    Brand multipliers are calibrated as residuals *after* this tier, so do not
    stack an extra unmanned discount on top of an already-cheap brand factor.
    """
    if location_tier == "highway" or osm_tier == "highway" or name_suggests_highway(brand, name):
        return "highway"
    if osm_tier in ("manned", "unmanned"):
        return osm_tier  # type: ignore[return-value]
    if location_tier in ("manned", "unmanned"):
        return location_tier  # type: ignore[return-value]
    return classify_place_station(brand, name)


NL_BBOX = (50.75, 3.2, 53.55, 7.2)  # south, west, north, east
DE_BBOX = (47.27, 5.87, 55.06, 15.04)
COUNTRY_BBOX: dict[str, tuple[float, float, float, float]] = {
    "NL": NL_BBOX,
    "DE": DE_BBOX,
}
# Fine grid keeps each Overpass query small — free hosts often 504 large tiles.
OSM_LAT_STEPS = 6
OSM_LNG_STEPS = 8
DE_LAT_STEPS = 8
DE_LNG_STEPS = 10
OSM_HTTP_TIMEOUT = 90.0
OSM_QUERY_TIMEOUT = 55
OSM_MAX_SUBDIV = 3
OSM_RETRIES_PER_URL = 2


def overpass_tile_query(
    south: float,
    west: float,
    north: float,
    east: float,
    *,
    amenity: str = "fuel",
) -> str:
    return f"""
[out:json][timeout:{OSM_QUERY_TIMEOUT}];
(
  node["amenity"="{amenity}"]({south},{west},{north},{east});
  way["amenity"="{amenity}"]({south},{west},{north},{east});
);
out center tags;
"""


def _parse_osm_elements(data: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for el in data.get("elements") or []:
        tags = el.get("tags") or {}
        osm_type = el.get("type", "node")
        osm_id = el.get("id")
        if osm_id is None:
            continue
        if osm_type == "node":
            lat, lng = el.get("lat"), el.get("lon")
        else:
            center = el.get("center") or {}
            lat, lng = center.get("lat"), center.get("lon")
        if lat is None or lng is None:
            continue
        tier, brand, name, operator = classify_osm_tags(tags)
        cc = _country_from_tags(tags)
        out.append(
            {
                "id": f"osm-{osm_type}-{osm_id}",
                "osm_type": osm_type,
                "osm_id": int(osm_id),
                "lat": float(lat),
                "lng": float(lng),
                "brand": brand,
                "name": name,
                "operator": operator,
                "location_tier": tier,
                "country": cc,
                "tags_json": json.dumps(tags, ensure_ascii=False),
            }
        )
    return out


def _country_from_tags(tags: dict[str, Any]) -> Optional[str]:
    raw = str(tags.get("addr:country") or tags.get("is_in:country") or "").strip().upper()
    if raw in ("NL", "NLD", "NETHERLANDS", "NEDERLAND"):
        return "NL"
    if raw in ("DE", "DEU", "GERMANY", "DEUTSCHLAND"):
        return "DE"
    if raw in ("BE", "BEL", "BELGIUM", "BELGIË", "BELGIE"):
        return "BE"
    return None


async def overpass_tile_json(
    client: httpx.AsyncClient,
    south: float,
    west: float,
    north: float,
    east: float,
    *,
    amenity: str = "fuel",
) -> dict[str, Any]:
    """Hit Overpass mirrors with short retries; return raw JSON. Raise last error if all fail."""
    query = overpass_tile_query(south, west, north, east, amenity=amenity)
    last_exc: Optional[Exception] = None
    for url in OVERPASS_URLS:
        for attempt in range(OSM_RETRIES_PER_URL):
            try:
                resp = await client.post(
                    url,
                    data={"data": query},
                    timeout=OSM_HTTP_TIMEOUT,
                    headers=OVERPASS_HEADERS,
                )
                if resp.status_code in (429, 502, 503, 504):
                    last_exc = httpx.HTTPStatusError(
                        f"{resp.status_code} from {url}",
                        request=resp.request,
                        response=resp,
                    )
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
                resp.raise_for_status()
                data = resp.json()
                if not isinstance(data, dict):
                    raise RuntimeError("Overpass returned non-object JSON")
                return data
            except Exception as exc:
                last_exc = exc
                await asyncio.sleep(0.8 * (attempt + 1))
    raise last_exc or RuntimeError("Overpass tile failed")


async def _overpass_tile_once(
    client: httpx.AsyncClient,
    south: float,
    west: float,
    north: float,
    east: float,
) -> list[dict[str, Any]]:
    data = await overpass_tile_json(client, south, west, north, east, amenity="fuel")
    return _parse_osm_elements(data)


async def _fetch_osm_bbox(
    client: httpx.AsyncClient,
    south: float,
    west: float,
    north: float,
    east: float,
    *,
    depth: int = 0,
) -> list[dict[str, Any]]:
    """Fetch one bbox; on timeout/errors subdivide into 4 until max depth."""
    try:
        stations = await _overpass_tile_once(client, south, west, north, east)
        await asyncio.sleep(0.35)  # be kind to public Overpass mirrors
        return stations
    except Exception:
        if depth >= OSM_MAX_SUBDIV:
            raise
        mid_lat = (south + north) / 2
        mid_lng = (west + east) / 2
        parts = [
            (south, west, mid_lat, mid_lng),
            (south, mid_lng, mid_lat, east),
            (mid_lat, west, north, mid_lng),
            (mid_lat, mid_lng, north, east),
        ]
        merged: dict[str, dict[str, Any]] = {}
        for s, w, n, e in parts:
            for station in await _fetch_osm_bbox(
                client, s, w, n, e, depth=depth + 1
            ):
                merged[station["id"]] = station
        return list(merged.values())


async def fetch_osm_country_stations(
    client: httpx.AsyncClient,
    country: str,
    *,
    lat_steps: Optional[int] = None,
    lng_steps: Optional[int] = None,
) -> list[dict[str, Any]]:
    """Fetch fuel stations for a country bbox in lat/lng tiles."""
    cc = country.upper()
    bbox = COUNTRY_BBOX.get(cc)
    if not bbox:
        raise ValueError(f"Unsupported OSM country: {cc}")
    if cc == "DE":
        lat_steps = lat_steps or DE_LAT_STEPS
        lng_steps = lng_steps or DE_LNG_STEPS
    else:
        lat_steps = lat_steps or OSM_LAT_STEPS
        lng_steps = lng_steps or OSM_LNG_STEPS
    south, west, north, east = bbox
    lat_delta = (north - south) / lat_steps
    lng_delta = (east - west) / lng_steps
    merged: dict[str, dict[str, Any]] = {}

    for i in range(lat_steps):
        for j in range(lng_steps):
            tile_s = south + i * lat_delta
            tile_n = south + (i + 1) * lat_delta
            tile_w = west + j * lng_delta
            tile_e = west + (j + 1) * lng_delta
            for station in await _fetch_osm_bbox(
                client, tile_s, tile_w, tile_n, tile_e
            ):
                station["country"] = station.get("country") or cc
                merged[station["id"]] = station

    return list(merged.values())


async def fetch_osm_nl_stations(client: httpx.AsyncClient) -> list[dict[str, Any]]:
    """Fetch NL fuel stations in lat/lng tiles to avoid Overpass timeouts."""
    return await fetch_osm_country_stations(client, "NL")


async def fetch_osm_de_stations(client: httpx.AsyncClient) -> list[dict[str, Any]]:
    return await fetch_osm_country_stations(client, "DE")


_SEED_HIGHWAY_IDS: Optional[set[str]] = None


def seed_highway_ids() -> set[str]:
    """OSM ids marked highway in the bundled seed (Overpass often lacks highway tags)."""
    global _SEED_HIGHWAY_IDS
    if _SEED_HIGHWAY_IDS is not None:
        return _SEED_HIGHWAY_IDS
    ids: set[str] = set()
    for path in (OSM_SEED_PATH, OSM_DE_SEED_PATH):
        if not path.is_file():
            continue
        try:
            raw = gzip.decompress(path.read_bytes())
            stations = json.loads(raw.decode("utf-8"))
            if isinstance(stations, list):
                for s in stations:
                    if s.get("location_tier") == "highway" and s.get("id"):
                        ids.add(str(s["id"]))
        except Exception:
            continue
    _SEED_HIGHWAY_IDS = ids
    return ids


async def store_osm_stations(db: aiosqlite.Connection, stations: list[dict[str, Any]]) -> int:
    now = time.time()
    hw_ids = seed_highway_ids()
    rows: list[tuple[Any, ...]] = []
    by_country: dict[str, int] = {}
    for s in stations:
        tier = s.get("location_tier") or "manned"
        if str(s.get("id")) in hw_ids or name_suggests_highway(s.get("brand"), s.get("name")):
            tier = "highway"
        country = infer_station_country(s)
        rows.append(
            (
                s["id"],
                s["osm_type"],
                s["osm_id"],
                s["lat"],
                s["lng"],
                s.get("brand"),
                s.get("name"),
                s.get("operator"),
                tier,
                s.get("tags_json"),
                now,
                country,
            )
        )
        by_country[country] = by_country.get(country, 0) + 1
    await db.execute("DELETE FROM osm_stations")
    await db.executemany(
        """
        INSERT INTO osm_stations(
            id, osm_type, osm_id, lat, lng, brand, name, operator,
            location_tier, tags_json, fetched_at, country
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    await meta_set(db, "osm_fetched_at", str(now))
    await meta_set(db, "osm_count", str(len(stations)))
    await meta_set(db, "osm_by_country", json.dumps(by_country))
    await db.commit()
    return len(stations)


def infer_station_country(station: dict[str, Any]) -> str:
    """Country from OSM addr:country tags + NL/DE bbox — ignore fetch-default."""
    tags: dict[str, Any] = {}
    raw = station.get("tags_json")
    if isinstance(raw, str) and raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                tags = parsed
        except Exception:
            tags = {}
    elif isinstance(raw, dict):
        tags = raw
    tagged = _country_from_tags(tags)
    if not tagged:
        existing = str(station.get("country") or "").strip().upper()
        if len(existing) == 2 and existing.isalpha():
            tagged = existing
    return country_for_point(float(station["lat"]), float(station["lng"]), tagged=tagged)


_COMPACT_TIER = {"h": "highway", "m": "manned", "u": "unmanned"}


def parse_compact_seed(raw: bytes, country: str) -> list[tuple[Any, ...]]:
    """Compact seed rows: [id, lat, lng, brand, name, tier-code]."""
    stations = json.loads(gzip.decompress(raw).decode("utf-8"))
    cc = country.upper()
    rows: list[tuple[Any, ...]] = []
    for item in stations:
        sid, lat, lng, brand, name, tier_code = item[:6]
        parts = str(sid).split("-")
        osm_type = parts[1] if len(parts) >= 3 else "node"
        osm_id = int(parts[-1]) if str(parts[-1]).isdigit() else 0
        tier = _COMPACT_TIER.get(str(tier_code or "m"), "manned")
        rows.append(
            (sid, osm_type, osm_id, float(lat), float(lng), brand, name or brand, None, tier, None, 0, cc)
        )
    return rows


def _read_seed_file(path: Path, default_country: str) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    raw = gzip.decompress(path.read_bytes())
    stations = json.loads(raw.decode("utf-8"))
    if not isinstance(stations, list) or not stations:
        return []
    for s in stations:
        s.setdefault("tags_json", None)
        s["country"] = infer_station_country(s) or (s.get("country") or default_country).upper()
    return stations


async def load_osm_seed(db: aiosqlite.Connection) -> int:
    """Load bundled NL + DE OSM stations when Overpass is unavailable."""
    by_id: dict[str, dict[str, Any]] = {}
    for station in _read_seed_file(OSM_SEED_PATH, "NL"):
        by_id[station["id"]] = station
    # DE seed adds German stations. Do not clobber NL IDs unless the point is DE.
    for station in _read_seed_file(OSM_DE_SEED_PATH, "DE"):
        if station["country"] != "DE" and station["id"] in by_id:
            continue
        if station["country"] == "DE" or station["id"] not in by_id:
            by_id[station["id"]] = station
    if not by_id:
        return 0
    return await store_osm_stations(db, list(by_id.values()))


async def load_feed_country_seeds(db: aiosqlite.Connection) -> dict[str, int]:
    """Insert bundled BE/FR/ES/IT station locations without deleting NL or DE."""
    loaded: dict[str, int] = {}
    for cc in feed_seed_countries():
        path = OSM_FUEL_DIR / f"{cc.lower()}.json.gz"
        if not path.is_file():
            continue
        rows = parse_compact_seed(path.read_bytes(), cc)
        if not rows:
            continue
        # Border stations from the NL/DE Overpass pull share these OSM ids
        # and are often labeled DE. Delete by id or the insert hits UNIQUE.
        await db.execute("DELETE FROM osm_stations WHERE country = ?", (cc,))
        await db.executemany("DELETE FROM osm_stations WHERE id = ?", [(row[0],) for row in rows])
        await db.executemany(
            """
            INSERT INTO osm_stations(
                id, osm_type, osm_id, lat, lng, brand, name, operator,
                location_tier, tags_json, fetched_at, country
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        loaded[cc] = len(rows)
    if loaded:
        await db.commit()
    return loaded


async def ensure_osm_bootstrap(db: aiosqlite.Connection) -> int:
    """Ensure osm_stations is non-empty — prefer existing rows, else bundled seed.

    BE/FR/ES/IT seeds are merged afterwards and never replace NL or DE rows.
    """
    async with db.execute("SELECT COUNT(*) AS c FROM osm_stations") as cur:
        count = int((await cur.fetchone())["c"])
    if count == 0:
        count = await load_osm_seed(db)
    await load_feed_country_seeds(db)
    async with db.execute("SELECT COUNT(*) AS c FROM osm_stations") as cur:
        return int((await cur.fetchone())["c"]) or count


load_wob_seed_mem()
load_world_avg_mem()


async def _fetch_country_with_retry(
    client: httpx.AsyncClient, country: str
) -> tuple[list[dict[str, Any]], Optional[Exception]]:
    last_exc: Optional[Exception] = None
    for attempt in range(2):
        try:
            return await fetch_osm_country_stations(client, country), None
        except Exception as exc:
            last_exc = exc
            await asyncio.sleep(2.0 * (attempt + 1))
    return [], last_exc


async def refresh_osm(
    db: aiosqlite.Connection,
    client: httpx.AsyncClient,
    *,
    force: bool = False,
) -> dict[str, Any]:
    now = time.time()
    osm_at = float(await meta_get(db, "osm_fetched_at") or 0)
    if not force and now - osm_at < OSM_TTL_SEC:
        count = await meta_get(db, "osm_count")
        existing = int(count) if count else 0
        if existing > 0:
            await load_feed_country_seeds(db)
            return {"refreshed": False, "count": existing}

    errors: dict[str, str] = {}
    by_id: dict[str, dict[str, Any]] = {}

    # Keep whatever we already have as a base (per-country Overpass may fail).
    async with db.execute(
        """
        SELECT id, osm_type, osm_id, lat, lng, brand, name, operator,
               location_tier, tags_json, country
        FROM osm_stations
        """
    ) as cur:
        for row in await cur.fetchall():
            by_id[str(row["id"])] = {
                "id": str(row["id"]),
                "osm_type": row["osm_type"],
                "osm_id": row["osm_id"],
                "lat": row["lat"],
                "lng": row["lng"],
                "brand": row["brand"],
                "name": row["name"],
                "operator": row["operator"],
                "location_tier": row["location_tier"],
                "tags_json": row["tags_json"],
                "country": row["country"] or "NL",
            }

    nl_rows, nl_err = await _fetch_country_with_retry(client, "NL")
    if nl_err:
        errors["NL"] = str(nl_err)
    for station in nl_rows:
        by_id[station["id"]] = station

    de_rows, de_err = await _fetch_country_with_retry(client, "DE")
    if de_err:
        errors["DE"] = str(de_err)
    for station in de_rows:
        by_id[station["id"]] = station

    if nl_rows or de_rows:
        count = await store_osm_stations(db, list(by_id.values()))
        feeds = await load_feed_country_seeds(db)
        return {
            "refreshed": True,
            "count": count + sum(feeds.values()),
            "source": "overpass",
            "nl": len(nl_rows),
            "de": len(de_rows),
            "feeds": feeds,
            "overpass_error": errors or None,
        }

    if by_id:
        return {
            "refreshed": False,
            "count": len(by_id),
            "source": "cache",
            "overpass_error": errors or None,
        }
    seeded = await load_osm_seed(db)
    if seeded > 0:
        return {
            "refreshed": True,
            "count": seeded,
            "source": "seed",
            "overpass_error": errors or None,
        }
    raise RuntimeError("OSM refresh failed and no seed available")


async def sync_places_from_osm(db: aiosqlite.Connection) -> int:
    """Replace the places cache with NL and DE OSM stations.

    BE/FR/ES/IT stay in osm_stations. Copying them into places hides the
    labeled estimate (Belgium) or the official live price (France, Spain, Italy).
    """
    now = time.time()
    async with db.execute(
        """
        SELECT id, lat, lng, brand, name, country FROM osm_stations
        WHERE country IN ('NL', 'DE')
        """
    ) as cur:
        rows = await cur.fetchall()
    if not rows:
        return 0
    await db.execute("DELETE FROM places")
    await db.executemany(
        """
        INSERT INTO places(id, lat, lng, brand, name, city, country)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                str(r["id"]),
                float(r["lat"]),
                float(r["lng"]),
                r["brand"],
                r["name"],
                None,
                (r["country"] or "NL").upper(),
            )
            for r in rows
        ],
    )
    by_country: dict[str, int] = {}
    for r in rows:
        cc = (r["country"] or "NL").upper()
        by_country[cc] = by_country.get(cc, 0) + 1
    await meta_set(db, "places_fetched_at", str(now))
    await meta_set(db, "places_countries", json.dumps(sorted(by_country)))
    await meta_set(db, "places_by_country", json.dumps(by_country))
    await db.commit()
    return len(rows)


async def osm_nearby(
    db: aiosqlite.Connection,
    lat: float,
    lon: float,
    radius_km: float,
) -> list[tuple[float, aiosqlite.Row]]:
    dlat = radius_km / 111.0
    dlng = radius_km / max(111.0 * math.cos(math.radians(lat)), 0.01)
    async with db.execute(
        """
        SELECT id, lat, lng, brand, name, operator, location_tier, country
        FROM osm_stations
        WHERE lat BETWEEN ? AND ? AND lng BETWEEN ? AND ?
        """,
        (lat - dlat, lat + dlat, lon - dlng, lon + dlng),
    ) as cur:
        rows = await cur.fetchall()
    out: list[tuple[float, aiosqlite.Row]] = []
    for row in rows:
        dist = haversine_km(lat, lon, float(row["lat"]), float(row["lng"]))
        if dist <= radius_km:
            out.append((dist, row))
    out.sort(key=lambda x: x[0])
    return out


def osm_matches_place(
    osm_lat: float,
    osm_lng: float,
    place_lat: float,
    place_lng: float,
    *,
    max_m: float = 120.0,
) -> bool:
    return haversine_km(osm_lat, osm_lng, place_lat, place_lng) * 1000 <= max_m


async def lookup_osm_tier_for_place(
    db: aiosqlite.Connection,
    lat: float,
    lng: float,
) -> Optional[LocationTier]:
    """Find nearest OSM station within 120m for tier/brand enrichment."""
    dlat = 0.002
    dlng = 0.003
    async with db.execute(
        """
        SELECT lat, lng, location_tier, brand FROM osm_stations
        WHERE lat BETWEEN ? AND ? AND lng BETWEEN ? AND ?
        LIMIT 20
        """,
        (lat - dlat, lat + dlat, lng - dlng, lng + dlng),
    ) as cur:
        rows = await cur.fetchall()
    best: Optional[LocationTier] = None
    best_dist = 999.0
    for row in rows:
        dist = haversine_km(lat, lng, float(row["lat"]), float(row["lng"]))
        if dist * 1000 <= 120 and dist < best_dist:
            best_dist = dist
            best = row["location_tier"]  # type: ignore[assignment]
    return best


async def estimate_status(db: aiosqlite.Connection) -> dict[str, Any]:
    async with db.execute("SELECT COUNT(*) AS c FROM osm_stations") as cur:
        osm_count = int((await cur.fetchone())["c"])
    by_country: dict[str, int] = {}
    try:
        async with db.execute(
            "SELECT country, COUNT(*) AS c FROM osm_stations GROUP BY country"
        ) as cur:
            for row in await cur.fetchall():
                by_country[str(row["country"] or "?")] = int(row["c"])
    except Exception:
        by_country = {"NL": osm_count}
    return {
        "cbs_daily_period": await meta_get(db, "cbs_daily_period"),
        "cbs_tiers_period": await meta_get(db, "cbs_tiers_period"),
        "cbs_daily_fetched_at": await meta_get(db, "cbs_daily_fetched_at"),
        "cbs_tiers_fetched_at": await meta_get(db, "cbs_tiers_fetched_at"),
        "cbs_latest_daily_period": await _latest_cbs_daily_period(db),
        "wob_week": await meta_get(db, "wob_week"),
        "wob_fetched_at": await meta_get(db, "wob_fetched_at"),
        "de_nl_ratios": dict(_de_ratios),
        "osm_count": osm_count,
        "osm_by_country": by_country,
        "osm_fetched_at": await meta_get(db, "osm_fetched_at"),
        "attribution": {
            "cbs": "CC BY 4.0 — Centraal Bureau voor de Statistiek",
            "osm": "© OpenStreetMap contributors (ODbL)",
            "tankerkoenig": "CC BY 4.0 — MTS-K / Tankerkönig (live DE, server-side)",
            "wob": "European Commission — Weekly Oil Bulletin (reuse permitted, © European Union)",
        },
        "de_estimate_note": (
            "Without TANKERKOENIG_API_KEY, DE prices are CBS × DE/NL ratio (Weekly Oil Bulletin) "
            f"indications (~±€{DE_ESTIMATE_ERROR_BAND_EUR[0]:.2f}–{DE_ESTIMATE_ERROR_BAND_EUR[1]:.2f}/L)"
        ),
    }


async def _latest_cbs_daily_period(db: aiosqlite.Connection) -> Optional[str]:
    async with db.execute("SELECT MAX(period) AS p FROM cbs_daily") as cur:
        row = await cur.fetchone()
    return str(row["p"]) if row and row["p"] else None


# Coarse NL outline (lat, lng), ~1–3 km accuracy along the DE border; sea side is padded.
# The NL bbox reaches 7.2°E and swallows Aachen/Köln/Düsseldorf/Kleve, so the border
# stretch matters more than the tag stored at seed time.
NL_POLYGON: tuple[tuple[float, float], ...] = (
    (51.37, 3.20), (53.70, 4.40), (53.52, 6.75), (53.32, 7.22), (53.18, 7.23),
    (53.00, 7.215), (52.87, 7.08),
    (52.64, 7.05), (52.43, 6.99), (52.22, 7.00), (52.08, 6.76), (51.97, 6.82),
    (51.86, 6.50), (51.85, 6.10), (51.76, 5.96), (51.68, 6.03), (51.48, 6.21),
    (51.35, 6.22), (51.20, 6.12), (51.08, 6.07), (51.03, 5.90), (50.99, 5.95),
    (50.90, 6.07), (50.87, 6.085), (50.76, 6.02), (50.75, 5.69), (50.85, 5.64), (51.00, 5.76),
    (51.15, 5.85), (51.25, 5.55), (51.30, 5.24), (51.43, 5.08), (51.46, 4.80),
    (51.50, 4.45), (51.38, 4.25), (51.35, 3.95), (51.27, 3.60),
)


def _in_polygon(lat: float, lng: float, poly: tuple[tuple[float, float], ...]) -> bool:
    inside = False
    j = len(poly) - 1
    for i in range(len(poly)):
        yi, xi = poly[i]
        yj, xj = poly[j]
        if (yi > lat) != (yj > lat) and lng < (xj - xi) * (lat - yi) / (yj - yi) + xi:
            inside = not inside
        j = i
    return inside


def country_for_point(
    lat: float, lng: float, *, tagged: Optional[str] = None
) -> str:
    """Explicit country tag wins, including Brussels inside the NL bounding box.

    Untagged points only become NL or DE when they sit in those boxes.
    Anywhere else stays unknown instead of being labeled as the Netherlands.
    """
    cc = (tagged or "").strip().upper()
    if len(cc) == 2 and cc.isalpha():
        return cc
    in_de = DE_BBOX[0] <= lat <= DE_BBOX[2] and DE_BBOX[1] <= lng <= DE_BBOX[3]
    in_nl = NL_BBOX[0] <= lat <= NL_BBOX[2] and NL_BBOX[1] <= lng <= NL_BBOX[3]
    if in_nl and in_de:
        return "NL" if _in_polygon(lat, lng, NL_POLYGON) else "DE"
    if in_de:
        return "DE"
    if in_nl:
        return "NL"
    return ""
