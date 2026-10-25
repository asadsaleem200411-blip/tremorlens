"""
TremorLens Event Reconciliation Pipeline.

Builds the unified, deduplicated `events_clean` table from `usgs_events` and `emsc_events`.

Matching criteria:
- Origin times differ by at most 60 seconds (MAX_TIME_DIFF_SECONDS)
- Epicentres are within 100 km (MAX_DISTANCE_KM, Haversine formula)
- Magnitudes differ by at most 0.5 (MAX_MAG_DIFF)
- One-to-one matching, prioritizing the closest candidate by time then distance.

Output:
- Matched pairs: source='both', USGS values preferred, USGS felt/cdi/mmi/sig kept,
  matched_usgs_id and matched_emsc_id populated.
- Unmatched events: source='usgs_only' or source='emsc_only'.
- Adds boolean column depth_is_default (depth_km = 10 exactly) to events_clean via
  `ALTER TABLE events_clean ADD COLUMN IF NOT EXISTS depth_is_default BOOLEAN;`.
- Re-runnable via PostgreSQL ON CONFLICT (event_id) DO UPDATE.
- Quality report generated and saved to reports/quality_report.csv.
"""

import argparse
import csv
import datetime
import math
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union
import bisect
import logging

import numpy as np
from sqlalchemy import Engine, text

# Ensure project root is available on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.db import get_engine

# =============================================================================
# Matching Threshold Constants
# =============================================================================
MAX_TIME_DIFF_SECONDS: float = 60.0  # Origin times differ by at most 60 seconds
MAX_DISTANCE_KM: float = 100.0       # Epicentres are within 100 km (haversine)
MAX_MAG_DIFF: float = 0.5           # Magnitudes differ by at most 0.5

# Alias for time window constant
TIME_WINDOW_SECONDS: float = MAX_TIME_DIFF_SECONDS

# Standard seismological default depth (km)
DEFAULT_DEPTH_KM: float = 10.0

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("etl.reconcile")

# =============================================================================
# SQL Queries
# =============================================================================
ALTER_TABLE_ADD_DEFAULT_DEPTH_SQL = text("""
ALTER TABLE events_clean ADD COLUMN IF NOT EXISTS depth_is_default BOOLEAN;
""")

def fetch_events(
    engine: Engine,
    start_time: Optional[datetime.datetime] = None,
    end_time: Optional[datetime.datetime] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Queries `usgs_events` and `emsc_events` from the database within the optional date bounds.
    """
    params: Dict[str, Any] = {}
    where_conditions = []
    if start_time is not None:
        where_conditions.append("event_time >= :start_time")
        params["start_time"] = start_time
    if end_time is not None:
        where_conditions.append("event_time <= :end_time")
        params["end_time"] = end_time

    where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""

    usgs_query = text(f"""
    SELECT
        event_id,
        event_time,
        latitude,
        longitude,
        depth_km,
        magnitude,
        mag_type,
        place,
        felt,
        cdi,
        mmi,
        sig
    FROM usgs_events
    {where_clause}
    ORDER BY event_time ASC;
    """)

    emsc_query = text(f"""
    SELECT
        event_id,
        event_time,
        latitude,
        longitude,
        depth_km,
        magnitude,
        mag_type,
        place,
        felt,
        cdi,
        mmi,
        sig,
        auth
    FROM emsc_events
    {where_clause}
    ORDER BY event_time ASC;
    """)

    logger.info("Fetching USGS events (start=%s, end=%s)...", start_time, end_time)
    with engine.connect() as conn:
        usgs_rows = conn.execute(usgs_query, params).mappings().all()
        usgs_events = [dict(r) for r in usgs_rows]

    logger.info("Fetched %d USGS events.", len(usgs_events))

    logger.info("Fetching EMSC events (start=%s, end=%s)...", start_time, end_time)
    with engine.connect() as conn:
        emsc_rows = conn.execute(emsc_query, params).mappings().all()
        emsc_events = [dict(r) for r in emsc_rows]

    logger.info("Fetched %d EMSC events.", len(emsc_events))

    return usgs_events, emsc_events

UPSERT_CLEAN_EVENT_SQL = text("""
INSERT INTO events_clean (
    event_id,
    source,
    event_time,
    latitude,
    longitude,
    depth_km,
    magnitude,
    mag_type,
    place,
    felt,
    cdi,
    mmi,
    sig,
    matched_usgs_id,
    matched_emsc_id,
    depth_is_default,
    ingested_at
) VALUES (
    :event_id,
    :source,
    :event_time,
    :latitude,
    :longitude,
    :depth_km,
    :magnitude,
    :mag_type,
    :place,
    :felt,
    :cdi,
    :mmi,
    :sig,
    :matched_usgs_id,
    :matched_emsc_id,
    :depth_is_default,
    :ingested_at
)
ON CONFLICT (event_id) DO UPDATE SET
    source = EXCLUDED.source,
    event_time = EXCLUDED.event_time,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    depth_km = EXCLUDED.depth_km,
    magnitude = EXCLUDED.magnitude,
    mag_type = EXCLUDED.mag_type,
    place = EXCLUDED.place,
    felt = EXCLUDED.felt,
    cdi = EXCLUDED.cdi,
    mmi = EXCLUDED.mmi,
    sig = EXCLUDED.sig,
    matched_usgs_id = EXCLUDED.matched_usgs_id,
    matched_emsc_id = EXCLUDED.matched_emsc_id,
    depth_is_default = EXCLUDED.depth_is_default,
    ingested_at = EXCLUDED.ingested_at;
""")


# =============================================================================
# Helper & Math Functions
# =============================================================================
def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Computes the great-circle distance between two points on Earth using the
    Haversine formula in kilometers (spherical Earth radius = 6371.0 km).
    """
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * (math.sin(delta_lambda / 2.0) ** 2)
    )
    a = min(1.0, max(0.0, a))
    c = 2.0 * math.asin(math.sqrt(a))
    return 6371.0 * c


def parse_to_utc_datetime(dt_val: Any) -> datetime.datetime:
    """
    Normalizes a datetime representation (datetime object or ISO string) to a UTC datetime.
    """
    if isinstance(dt_val, datetime.datetime):
        if dt_val.tzinfo is None:
            return dt_val.replace(tzinfo=datetime.timezone.utc)
        return dt_val.astimezone(datetime.timezone.utc)

    if isinstance(dt_val, (int, float)):
        # Epoch seconds or milliseconds
        if dt_val > 1e11:
            return datetime.datetime.fromtimestamp(dt_val / 1000.0, tz=datetime.timezone.utc)
        return datetime.datetime.fromtimestamp(dt_val, tz=datetime.timezone.utc)

    if isinstance(dt_val, str):
        cleaned = dt_val.strip().replace("Z", "+00:00")
        dt = datetime.datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)

    raise ValueError(f"Cannot parse datetime value: {dt_val}")


def is_default_depth(depth_km: Optional[float]) -> bool:
    """
    Returns True if the earthquake depth is fixed at the standard default 10.0 km depth.
    """
    if depth_km is None:
        return False
    return abs(float(depth_km) - DEFAULT_DEPTH_KM) < 1e-5


def get_decade_label(year: int) -> str:
    """
    Returns decade label string, e.g. 2014 -> '2010s'.
    """
    return f"{(year // 10) * 10}s"


# =============================================================================
# Core Reconciliation Algorithm
# =============================================================================
def match_events(
    usgs_events: Sequence[Dict[str, Any]],
    emsc_events: Sequence[Dict[str, Any]],
    max_time_diff: float = MAX_TIME_DIFF_SECONDS,
    max_distance_km: float = MAX_DISTANCE_KM,
    max_mag_diff: float = MAX_MAG_DIFF,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Reconciles USGS and EMSC earthquake events.

    Matching constraints:
    - Origin times differ by at most `max_time_diff` seconds.
    - Haversine distance between epicenters is at most `max_distance_km` km.
    - Magnitudes differ by at most `max_mag_diff`.
    - One-to-one matching, choosing the closest candidate by time then distance.

    Args:
        usgs_events: List of USGS event dicts.
        emsc_events: List of EMSC event dicts.
        max_time_diff: Maximum allowed origin time difference in seconds.
        max_distance_km: Maximum allowed epicentral distance in kilometers.
        max_mag_diff: Maximum allowed magnitude difference.

    Returns:
        A tuple of:
        - matches: List of dicts representing matched pairs with difference statistics.
        - unmatched_usgs: List of unmatched USGS event dicts.
        - unmatched_emsc: List of unmatched EMSC event dicts.
    """
    if not usgs_events and not emsc_events:
        return [], [], []

    if not usgs_events:
        return [], [], [dict(e) for e in emsc_events]

    if not emsc_events:
        return [], [dict(u) for u in usgs_events], []

    # Prepare normalized copies with epoch seconds for fast comparison
    usgs_list: List[Dict[str, Any]] = []
    for u in usgs_events:
        u_copy = dict(u)
        dt = parse_to_utc_datetime(u_copy["event_time"])
        u_copy["_dt"] = dt
        u_copy["_epoch"] = dt.timestamp()
        usgs_list.append(u_copy)

    emsc_list: List[Dict[str, Any]] = []
    for e in emsc_events:
        e_copy = dict(e)
        dt = parse_to_utc_datetime(e_copy["event_time"])
        e_copy["_dt"] = dt
        e_copy["_epoch"] = dt.timestamp()
        emsc_list.append(e_copy)

    # Sort both lists chronologically by epoch timestamp
    usgs_list.sort(key=lambda x: x["_epoch"])
    emsc_list.sort(key=lambda x: x["_epoch"])

    # Extract EMSC timestamps array for binary search sliding window
    emsc_epochs = [e["_epoch"] for e in emsc_list]

    # Find all candidate pairs within the 3 thresholds
    # Candidate tuple: (time_diff, dist_km, mag_diff, u_idx, e_idx)
    candidates: List[Tuple[float, float, float, int, int]] = []

    for u_idx, u in enumerate(usgs_list):
        t_u = u["_epoch"]
        left_idx = bisect.bisect_left(emsc_epochs, t_u - max_time_diff)
        right_idx = bisect.bisect_right(emsc_epochs, t_u + max_time_diff)

        u_lat = float(u["latitude"])
        u_lon = float(u["longitude"])
        u_mag = u.get("magnitude")

        if u_mag is None:
            continue
        u_mag_float = float(u_mag)

        for e_idx in range(left_idx, right_idx):
            e = emsc_list[e_idx]
            time_diff = abs(t_u - e["_epoch"])
            if time_diff > max_time_diff:
                continue

            e_mag = e.get("magnitude")
            if e_mag is None:
                continue
            mag_diff = abs(u_mag_float - float(e_mag))
            if mag_diff > max_mag_diff:
                continue

            dist_km = haversine_distance(u_lat, u_lon, float(e["latitude"]), float(e["longitude"]))
            if dist_km > max_distance_km:
                continue

            candidates.append((time_diff, dist_km, mag_diff, u_idx, e_idx))

    # Sort candidate pairs to enforce:
    # 1. Closest candidate by time first
    # 2. Then closest candidate by distance
    # 3. Then closest by magnitude difference as tie-breaker
    # 4. Deterministic event ID tie-breakers
    candidates.sort(
        key=lambda c: (
            c[0],  # time_diff
            c[1],  # dist_km
            c[2],  # mag_diff
            usgs_list[c[3]]["event_id"],
            emsc_list[c[4]]["event_id"],
        )
    )

    # Perform greedy 1-to-1 matching
    matched_usgs_indices = set()
    matched_emsc_indices = set()
    matches: List[Dict[str, Any]] = []

    for time_diff, dist_km, mag_diff, u_idx, e_idx in candidates:
        if u_idx not in matched_usgs_indices and e_idx not in matched_emsc_indices:
            matched_usgs_indices.add(u_idx)
            matched_emsc_indices.add(e_idx)

            u_item = usgs_list[u_idx]
            e_item = emsc_list[e_idx]

            u_depth = u_item.get("depth_km")
            e_depth = e_item.get("depth_km")
            depth_diff = (
                abs(float(u_depth) - float(e_depth))
                if u_depth is not None and e_depth is not None
                else None
            )

            # Strip temporary keys
            u_clean = {k: v for k, v in u_item.items() if not k.startswith("_")}
            e_clean = {k: v for k, v in e_item.items() if not k.startswith("_")}

            matches.append({
                "usgs": u_clean,
                "emsc": e_clean,
                "time_diff_seconds": time_diff,
                "distance_km": dist_km,
                "magnitude_diff": mag_diff,
                "depth_diff_km": depth_diff,
            })

    unmatched_usgs = [
        {k: v for k, v in u.items() if not k.startswith("_")}
        for idx, u in enumerate(usgs_list)
        if idx not in matched_usgs_indices
    ]

    unmatched_emsc = [
        {k: v for k, v in e.items() if not k.startswith("_")}
        for idx, e in enumerate(emsc_list)
        if idx not in matched_emsc_indices
    ]

    return matches, unmatched_usgs, unmatched_emsc


def build_clean_records(
    matches: List[Dict[str, Any]],
    unmatched_usgs: List[Dict[str, Any]],
    unmatched_emsc: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Constructs unified database records for `events_clean`:
    - Matched pairs: source='both', USGS values preferred, USGS felt/cdi/mmi/sig kept,
      matched_usgs_id and matched_emsc_id populated.
    - Unmatched USGS: source='usgs_only', matched_usgs_id=None, matched_emsc_id=None.
    - Unmatched EMSC: source='emsc_only', matched_usgs_id=None, matched_emsc_id=None.
    - depth_is_default: True if depth_km == 10 exactly.
    """
    records: List[Dict[str, Any]] = []
    now_utc = datetime.datetime.now(datetime.timezone.utc)

    # 1. Matched pairs -> source='both'
    for match in matches:
        u = match["usgs"]
        e = match["emsc"]

        depth = u.get("depth_km") if u.get("depth_km") is not None else e.get("depth_km")
        mag = u.get("magnitude") if u.get("magnitude") is not None else e.get("magnitude")
        mag_type = u.get("mag_type") if u.get("mag_type") is not None else e.get("mag_type")
        place = u.get("place") if u.get("place") is not None else e.get("place")
        lat = u.get("latitude") if u.get("latitude") is not None else e.get("latitude")
        lon = u.get("longitude") if u.get("longitude") is not None else e.get("longitude")

        records.append({
            "event_id": str(u["event_id"]),
            "source": "both",
            "event_time": parse_to_utc_datetime(u["event_time"]),
            "latitude": float(lat) if lat is not None else 0.0,
            "longitude": float(lon) if lon is not None else 0.0,
            "depth_km": float(depth) if depth is not None else None,
            "magnitude": float(mag) if mag is not None else None,
            "mag_type": str(mag_type) if mag_type is not None else None,
            "place": str(place) if place is not None else None,
            "felt": int(u["felt"]) if u.get("felt") is not None else None,
            "cdi": float(u["cdi"]) if u.get("cdi") is not None else None,
            "mmi": float(u["mmi"]) if u.get("mmi") is not None else None,
            "sig": int(u["sig"]) if u.get("sig") is not None else None,
            "matched_usgs_id": str(u["event_id"]),
            "matched_emsc_id": str(e["event_id"]),
            "depth_is_default": is_default_depth(depth),
            "ingested_at": now_utc,
        })

    # 2. Unmatched USGS -> source='usgs_only'
    for u in unmatched_usgs:
        depth = u.get("depth_km")
        mag = u.get("magnitude")
        lat = u.get("latitude")
        lon = u.get("longitude")
        records.append({
            "event_id": str(u["event_id"]),
            "source": "usgs_only",
            "event_time": parse_to_utc_datetime(u["event_time"]),
            "latitude": float(lat) if lat is not None else 0.0,
            "longitude": float(lon) if lon is not None else 0.0,
            "depth_km": float(depth) if depth is not None else None,
            "magnitude": float(mag) if mag is not None else None,
            "mag_type": str(u["mag_type"]) if u.get("mag_type") is not None else None,
            "place": str(u["place"]) if u.get("place") is not None else None,
            "felt": int(u["felt"]) if u.get("felt") is not None else None,
            "cdi": float(u["cdi"]) if u.get("cdi") is not None else None,
            "mmi": float(u["mmi"]) if u.get("mmi") is not None else None,
            "sig": int(u["sig"]) if u.get("sig") is not None else None,
            "matched_usgs_id": None,
            "matched_emsc_id": None,
            "depth_is_default": is_default_depth(depth),
            "ingested_at": now_utc,
        })

    # 3. Unmatched EMSC -> source='emsc_only'
    for e in unmatched_emsc:
        depth = e.get("depth_km")
        mag = e.get("magnitude")
        lat = e.get("latitude")
        lon = e.get("longitude")
        records.append({
            "event_id": str(e["event_id"]),
            "source": "emsc_only",
            "event_time": parse_to_utc_datetime(e["event_time"]),
            "latitude": float(lat) if lat is not None else 0.0,
            "longitude": float(lon) if lon is not None else 0.0,
            "depth_km": float(depth) if depth is not None else None,
            "magnitude": float(mag) if mag is not None else None,
            "mag_type": str(e["mag_type"]) if e.get("mag_type") is not None else None,
            "place": str(e["place"]) if e.get("place") is not None else None,
            "felt": int(e["felt"]) if e.get("felt") is not None else None,
            "cdi": float(e["cdi"]) if e.get("cdi") is not None else None,
            "mmi": float(e["mmi"]) if e.get("mmi") is not None else None,
            "sig": int(e["sig"]) if e.get("sig") is not None else None,
            "matched_usgs_id": None,
            "matched_emsc_id": None,
            "depth_is_default": is_default_depth(depth),
            "ingested_at": now_utc,
        })

    return records


# =============================================================================
# Quality Report Generation
# =============================================================================
def compute_stats(values: List[float]) -> Tuple[float, float, float, float, float, float]:
    """
    Computes summary metrics (mean, std, median, iqr, min, max) for a list of floats.
    """
    if not values:
        return 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
    arr = np.array(values, dtype=float)
    mean = float(np.mean(arr))
    std = float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0
    med = float(np.median(arr))
    q75, q25 = np.percentile(arr, [75, 25])
    iqr = float(q75 - q25)
    min_v = float(np.min(arr))
    max_v = float(np.max(arr))
    return mean, std, med, iqr, min_v, max_v


def generate_quality_report(
    matches: List[Dict[str, Any]],
    unmatched_usgs: List[Dict[str, Any]],
    unmatched_emsc: List[Dict[str, Any]],
    clean_records: List[Dict[str, Any]],
    raw_emsc_events: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Generates a structured quality report meeting all user specifications:
    1. Counts of both, usgs_only, emsc_only.
    2. Match rate per year.
    3. Headline cross-source statistics restricted to 2005 onward, with a note
       that EMSC coverage is incomplete before 2005.
    4. Match rate split by EMSC auth = 'NEIC' vs all other authors (NEIC events
       are USGS's own data, acting as a sanity check).
    5. For matched pairs, the mean and spread of magnitude, depth and time differences by decade.
    6. Share of depth_is_default events per source and per decade.
    """
    report_rows: List[Dict[str, Any]] = []

    both_count = len(matches)
    usgs_only_count = len(unmatched_usgs)
    emsc_only_count = len(unmatched_emsc)
    total_clean = both_count + usgs_only_count + emsc_only_count

    # -------------------------------------------------------------------------
    # Section 1: Counts of both, usgs_only and emsc_only
    # -------------------------------------------------------------------------
    sec1 = "1. Event Counts by Source"
    for cat, cnt in [
        ("both", both_count),
        ("usgs_only", usgs_only_count),
        ("emsc_only", emsc_only_count),
        ("total_clean", total_clean),
    ]:
        rate = round(cnt / total_clean, 4) if total_clean > 0 else 0.0
        report_rows.append({
            "section": sec1,
            "category": cat,
            "decade_or_year": "all",
            "metric": "count",
            "value": cnt,
            "count": cnt,
            "total": total_clean,
            "share_or_rate": rate,
            "spread_std": "",
            "notes": f"Share of total unified events ({rate * 100:.2f}%)",
        })

    # -------------------------------------------------------------------------
    # Section 2: Match rate per year
    # -------------------------------------------------------------------------
    sec2 = "2. Match Rate Per Year"
    # Aggregate counts by year
    years_dict: Dict[int, Dict[str, int]] = {}

    def register_event_year(dt: datetime.datetime, source: str) -> None:
        yr = dt.year
        if yr not in years_dict:
            years_dict[yr] = {"both": 0, "usgs_only": 0, "emsc_only": 0}
        years_dict[yr][source] += 1

    for m in matches:
        dt = parse_to_utc_datetime(m["usgs"]["event_time"])
        register_event_year(dt, "both")

    for u in unmatched_usgs:
        dt = parse_to_utc_datetime(u["event_time"])
        register_event_year(dt, "usgs_only")

    for e in unmatched_emsc:
        dt = parse_to_utc_datetime(e["event_time"])
        register_event_year(dt, "emsc_only")

    for yr in sorted(years_dict.keys()):
        counts = years_dict[yr]
        b_cnt = counts["both"]
        u_cnt = counts["usgs_only"]
        e_cnt = counts["emsc_only"]
        yr_total = b_cnt + u_cnt + e_cnt
        m_rate = round(b_cnt / yr_total, 4) if yr_total > 0 else 0.0

        report_rows.append({
            "section": sec2,
            "category": "annual",
            "decade_or_year": str(yr),
            "metric": "match_rate",
            "value": m_rate,
            "count": b_cnt,
            "total": yr_total,
            "share_or_rate": m_rate,
            "spread_std": "",
            "notes": f"both={b_cnt}, usgs_only={u_cnt}, emsc_only={e_cnt}",
        })

    # -------------------------------------------------------------------------
    # Section 3: Headline cross-source statistics restricted to 2005 onward
    # -------------------------------------------------------------------------
    sec3 = "3. Headline Cross-Source Statistics (2005 Onward)"
    incomplete_note = (
        "Note: EMSC coverage is incomplete before 2005; headline statistics restricted to 2005 onward."
    )

    matches_2005 = [
        m for m in matches
        if parse_to_utc_datetime(m["usgs"]["event_time"]).year >= 2005
    ]
    u_2005 = [
        u for u in unmatched_usgs
        if parse_to_utc_datetime(u["event_time"]).year >= 2005
    ]
    e_2005 = [
        e for e in unmatched_emsc
        if parse_to_utc_datetime(e["event_time"]).year >= 2005
    ]

    both_2005_cnt = len(matches_2005)
    u_2005_cnt = len(u_2005)
    e_2005_cnt = len(e_2005)
    clean_2005_total = both_2005_cnt + u_2005_cnt + e_2005_cnt
    match_rate_2005 = (
        round(both_2005_cnt / clean_2005_total, 4) if clean_2005_total > 0 else 0.0
    )

    t_diffs_2005 = [m["time_diff_seconds"] for m in matches_2005]
    m_diffs_2005 = [m["magnitude_diff"] for m in matches_2005]
    d_diffs_2005 = [
        m["depth_diff_km"] for m in matches_2005 if m["depth_diff_km"] is not None
    ]
    dist_2005 = [m["distance_km"] for m in matches_2005]

    t_mean, t_std, _, _, _, _ = compute_stats(t_diffs_2005)
    m_mean, m_std, _, _, _, _ = compute_stats(m_diffs_2005)
    d_mean, d_std, _, _, _, _ = compute_stats(d_diffs_2005)
    dist_mean, dist_std, _, _, _, _ = compute_stats(dist_2005)

    headline_metrics = [
        ("matched_pairs_count", both_2005_cnt, both_2005_cnt, clean_2005_total, match_rate_2005, "", f"{both_2005_cnt} matched pairs (>=2005)"),
        ("usgs_only_count", u_2005_cnt, u_2005_cnt, clean_2005_total, round(u_2005_cnt / clean_2005_total, 4) if clean_2005_total else 0.0, "", f"{u_2005_cnt} unmatched USGS events"),
        ("emsc_only_count", e_2005_cnt, e_2005_cnt, clean_2005_total, round(e_2005_cnt / clean_2005_total, 4) if clean_2005_total else 0.0, "", f"{e_2005_cnt} unmatched EMSC events"),
        ("cross_source_match_rate", match_rate_2005, both_2005_cnt, clean_2005_total, match_rate_2005, "", f"Overall match rate {match_rate_2005*100:.2f}% (>=2005)"),
        ("mean_time_diff_seconds", round(t_mean, 3), len(t_diffs_2005), len(t_diffs_2005), "", round(t_std, 3), "Mean origin time difference in seconds"),
        ("mean_magnitude_diff", round(m_mean, 3), len(m_diffs_2005), len(m_diffs_2005), "", round(m_std, 3), "Mean magnitude difference"),
        ("mean_depth_diff_km", round(d_mean, 3), len(d_diffs_2005), len(d_diffs_2005), "", round(d_std, 3), "Mean depth difference in km"),
        ("mean_distance_km", round(dist_mean, 3), len(dist_2005), len(dist_2005), "", round(dist_std, 3), "Mean epicentral distance in km"),
    ]

    for m_name, val, cnt, tot, rate, std_v, note_text in headline_metrics:
        report_rows.append({
            "section": sec3,
            "category": "headline_2005_onward",
            "decade_or_year": "2005+",
            "metric": m_name,
            "value": val,
            "count": cnt,
            "total": tot,
            "share_or_rate": rate,
            "spread_std": std_v,
            "notes": f"{incomplete_note} | {note_text}",
        })

    # -------------------------------------------------------------------------
    # Section 4: Match rate split by EMSC auth = 'NEIC' vs all other authors
    # -------------------------------------------------------------------------
    sec4 = "4. Match Rate by EMSC Author (NEIC vs Other)"
    matched_emsc_ids = {m["emsc"]["event_id"] for m in matches}

    neic_total = 0
    neic_matched = 0
    other_total = 0
    other_matched = 0

    for e in raw_emsc_events:
        auth_val = str(e.get("auth") or "").strip().upper()
        eid = e.get("event_id")
        is_matched = eid in matched_emsc_ids

        if auth_val == "NEIC":
            neic_total += 1
            if is_matched:
                neic_matched += 1
        else:
            other_total += 1
            if is_matched:
                other_matched += 1

    neic_rate = round(neic_matched / neic_total, 4) if neic_total > 0 else 0.0
    other_rate = round(other_matched / other_total, 4) if other_total > 0 else 0.0

    report_rows.append({
        "section": sec4,
        "category": "NEIC",
        "decade_or_year": "all",
        "metric": "match_rate",
        "value": neic_rate,
        "count": neic_matched,
        "total": neic_total,
        "share_or_rate": neic_rate,
        "spread_std": "",
        "notes": "NEIC events are USGS own data; acts as sanity check for matching thresholds",
    })

    report_rows.append({
        "section": sec4,
        "category": "OTHER",
        "decade_or_year": "all",
        "metric": "match_rate",
        "value": other_rate,
        "count": other_matched,
        "total": other_total,
        "share_or_rate": other_rate,
        "spread_std": "",
        "notes": "All other EMSC contributing authors (regional networks, etc.)",
    })

    # -------------------------------------------------------------------------
    # Section 5: Mean and spread of magnitude, depth and time differences by decade
    # -------------------------------------------------------------------------
    sec5 = "5. Matched Differences by Decade"
    decade_matches: Dict[str, List[Dict[str, Any]]] = {}

    for m in matches:
        yr = parse_to_utc_datetime(m["usgs"]["event_time"]).year
        dec = get_decade_label(yr)
        if dec not in decade_matches:
            decade_matches[dec] = []
        decade_matches[dec].append(m)

    for dec in sorted(decade_matches.keys()):
        m_list = decade_matches[dec]
        n_pairs = len(m_list)

        t_vals = [item["time_diff_seconds"] for item in m_list]
        m_vals = [item["magnitude_diff"] for item in m_list]
        d_vals = [item["depth_diff_km"] for item in m_list if item["depth_diff_km"] is not None]

        # Time diff
        t_mean, t_std, t_med, t_iqr, t_min, t_max = compute_stats(t_vals)
        report_rows.append({
            "section": sec5,
            "category": "matched_pairs",
            "decade_or_year": dec,
            "metric": "time_diff_seconds",
            "value": round(t_mean, 3),
            "count": n_pairs,
            "total": n_pairs,
            "share_or_rate": "",
            "spread_std": round(t_std, 3),
            "notes": f"median={round(t_med, 3)}, iqr={round(t_iqr, 3)}, min={round(t_min, 3)}, max={round(t_max, 3)}",
        })

        # Magnitude diff
        m_mean, m_std, m_med, m_iqr, m_min, m_max = compute_stats(m_vals)
        report_rows.append({
            "section": sec5,
            "category": "matched_pairs",
            "decade_or_year": dec,
            "metric": "magnitude_diff",
            "value": round(m_mean, 3),
            "count": n_pairs,
            "total": n_pairs,
            "share_or_rate": "",
            "spread_std": round(m_std, 3),
            "notes": f"median={round(m_med, 3)}, iqr={round(m_iqr, 3)}, min={round(m_min, 3)}, max={round(m_max, 3)}",
        })

        # Depth diff
        d_mean, d_std, d_med, d_iqr, d_min, d_max = compute_stats(d_vals)
        report_rows.append({
            "section": sec5,
            "category": "matched_pairs",
            "decade_or_year": dec,
            "metric": "depth_diff_km",
            "value": round(d_mean, 3),
            "count": len(d_vals),
            "total": n_pairs,
            "share_or_rate": round(len(d_vals) / n_pairs, 4) if n_pairs > 0 else 0.0,
            "spread_std": round(d_std, 3),
            "notes": f"median={round(d_med, 3)}, iqr={round(d_iqr, 3)}, min={round(d_min, 3)}, max={round(d_max, 3)}",
        })

    # -------------------------------------------------------------------------
    # Section 6: Share of depth_is_default events per source and per decade
    # -------------------------------------------------------------------------
    sec6 = "6. Default Depth Share by Source and Decade"
    # Group clean records by (decade, source)
    default_depth_stats: Dict[Tuple[str, str], Dict[str, int]] = {}

    for rec in clean_records:
        yr = rec["event_time"].year
        dec = get_decade_label(yr)
        src = rec["source"]
        key = (dec, src)
        if key not in default_depth_stats:
            default_depth_stats[key] = {"total": 0, "default_depth": 0}

        default_depth_stats[key]["total"] += 1
        if rec.get("depth_is_default"):
            default_depth_stats[key]["default_depth"] += 1

    for (dec, src) in sorted(default_depth_stats.keys()):
        stats = default_depth_stats[(dec, src)]
        tot = stats["total"]
        def_cnt = stats["default_depth"]
        share = round(def_cnt / tot, 4) if tot > 0 else 0.0

        report_rows.append({
            "section": sec6,
            "category": src,
            "decade_or_year": dec,
            "metric": "depth_is_default_share",
            "value": share,
            "count": def_cnt,
            "total": tot,
            "share_or_rate": share,
            "spread_std": "",
            "notes": f"{def_cnt} of {tot} events have depth_km == 10 exactly ({share*100:.2f}%)",
        })

    return report_rows


def save_quality_report(
    report_rows: List[Dict[str, Any]],
    output_path: Union[str, Path] = "reports/quality_report.csv",
) -> Path:
    """
    Saves the quality report to CSV at the specified file path.
    Ensures parent directories exist.
    """
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "section",
        "category",
        "decade_or_year",
        "metric",
        "value",
        "count",
        "total",
        "share_or_rate",
        "spread_std",
        "notes",
    ]

    with open(target, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(report_rows)

    logger.info("Quality report saved successfully to %s (%d rows)", target, len(report_rows))
    return target


# =============================================================================
# Database ETL Pipeline Functions
# =============================================================================
def ensure_schema(engine: Engine) -> None:
    """
    Ensures `events_clean` table has the boolean column `depth_is_default`.
    Executes 'ALTER TABLE events_clean ADD COLUMN IF NOT EXISTS depth_is_default BOOLEAN;'
    on PostgreSQL, with safe inspection fallback for other dialects.
    """
    dialect = engine.dialect.name
    logger.info("Ensuring schema on dialect '%s'...", dialect)
    if dialect == "postgresql":
        with engine.begin() as conn:
            conn.execute(ALTER_TABLE_ADD_DEFAULT_DEPTH_SQL)
    else:
        # Fallback inspection for SQLite or mock test engines
        from sqlalchemy import inspect
        insp = inspect(engine)
        columns = [c["name"] for c in insp.get_columns("events_clean")]
        if "depth_is_default" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE events_clean ADD COLUMN depth_is_default BOOLEAN;"))
    logger.info("Schema verified: 'events_clean.depth_is_default' column is present.")



def upsert_clean_records(
    engine: Engine,
    records: List[Dict[str, Any]],
    batch_size: int = 5000,
) -> int:
    """
    Upserts unified records into `events_clean` in batches.
    """
    if not records:
        logger.info("No records to upsert.")
        return 0

    total_records = len(records)
    logger.info("Starting upsert of %d records into `events_clean` (batch_size=%d)...", total_records, batch_size)

    total_loaded = 0
    with engine.begin() as conn:
        for i in range(0, total_records, batch_size):
            batch = records[i : i + batch_size]
            conn.execute(UPSERT_CLEAN_EVENT_SQL, batch)
            total_loaded += len(batch)
            logger.info("Upserted batch %d/%d (%d rows total)", min(i + batch_size, total_records), total_records, total_loaded)

    logger.info("Successfully upserted all %d records into `events_clean`.", total_loaded)
    return total_loaded


def replace_clean_records(
    engine: Engine,
    records: List[Dict[str, Any]],
    start_time: Optional[datetime.datetime] = None,
    end_time: Optional[datetime.datetime] = None,
    batch_size: int = 5000,
) -> int:
    """
    Atomically replaces records in `events_clean` within the given date range:
    Deletes all existing `events_clean` rows in the date range [start_time, end_time]
    and inserts the new records within a single database transaction.
    """
    total_records = len(records)
    logger.info(
        "Atomically replacing records in `events_clean` for date range [%s, %s] (batch_size=%d)...",
        start_time,
        end_time,
        batch_size,
    )

    params: Dict[str, Any] = {}
    where_conditions = []
    if start_time is not None:
        where_conditions.append("event_time >= :start_time")
        params["start_time"] = start_time
    if end_time is not None:
        where_conditions.append("event_time <= :end_time")
        params["end_time"] = end_time

    delete_sql = (
        f"DELETE FROM events_clean WHERE {' AND '.join(where_conditions)};"
        if where_conditions
        else "DELETE FROM events_clean;"
    )

    total_loaded = 0
    with engine.begin() as conn:
        del_result = conn.execute(text(delete_sql), params)
        deleted_count = del_result.rowcount
        logger.info(
            "Cleaned date range: deleted %s existing rows from `events_clean`.",
            deleted_count if deleted_count is not None and deleted_count >= 0 else "matching",
        )

        for i in range(0, total_records, batch_size):
            batch = records[i : i + batch_size]
            conn.execute(UPSERT_CLEAN_EVENT_SQL, batch)
            total_loaded += len(batch)
            logger.info(
                "Inserted batch %d/%d (%d rows total)",
                min(i + batch_size, total_records),
                total_records,
                total_loaded,
            )

    logger.info("Successfully replaced %d clean records in `events_clean`.", total_loaded)
    return total_loaded


def run_reconcile(
    engine: Optional[Engine] = None,
    start: Optional[Union[str, datetime.datetime]] = None,
    end: Optional[Union[str, datetime.datetime]] = None,
    report_path: Union[str, Path] = "reports/quality_report.csv",
    dry_run: bool = False,
    batch_size: int = 5000,
) -> Dict[str, Any]:
    """
    Runs the full earthquake reconciliation pipeline:
    1. Fetches events from `usgs_events` and `emsc_events`.
    2. Performs one-to-one matching inside the 60s/100km/0.5mag window.
    3. Builds `events_clean` records.
    4. Replaces records cleanly into `events_clean` in one transaction (unless dry_run is True).
    5. Computes quality statistics and saves reports/quality_report.csv.

    Returns summary execution dictionary.
    """
    started_at = datetime.datetime.now(datetime.timezone.utc)
    target_engine = engine or get_engine()

    # Parse date filters if provided
    start_dt = parse_to_utc_datetime(start) if start else None
    end_dt = parse_to_utc_datetime(end) if end else None

    logger.info("=== Starting Earthquake Reconciliation Pipeline ===")
    logger.info("Filters: start=%s, end=%s | dry_run=%s", start_dt, end_dt, dry_run)
    logger.info(
        "Thresholds: MAX_TIME_DIFF=%.1fs, MAX_DISTANCE=%.1fkm, MAX_MAG_DIFF=%.1f",
        MAX_TIME_DIFF_SECONDS,
        MAX_DISTANCE_KM,
        MAX_MAG_DIFF,
    )

    # 1. Ensure table schema has depth_is_default
    if not dry_run:
        ensure_schema(target_engine)

    # 2. Fetch events from database
    usgs_events, emsc_events = fetch_events(target_engine, start_time=start_dt, end_time=end_dt)

    # 3. Match events
    logger.info("Executing one-to-one event matching...")
    matches, unmatched_usgs, unmatched_emsc = match_events(
        usgs_events=usgs_events,
        emsc_events=emsc_events,
        max_time_diff=MAX_TIME_DIFF_SECONDS,
        max_distance_km=MAX_DISTANCE_KM,
        max_mag_diff=MAX_MAG_DIFF,
    )

    logger.info(
        "Matching completed: matches(both)=%d, usgs_only=%d, emsc_only=%d",
        len(matches),
        len(unmatched_usgs),
        len(unmatched_emsc),
    )

    # 4. Build records for events_clean
    clean_records = build_clean_records(matches, unmatched_usgs, unmatched_emsc)
    logger.info("Constructed %d clean records.", len(clean_records))

    # 5. Cleanly replace into database in a single transaction
    rows_upserted = 0
    if not dry_run:
        rows_upserted = replace_clean_records(
            target_engine,
            clean_records,
            start_time=start_dt,
            end_time=end_dt,
            batch_size=batch_size,
        )
    else:
        logger.info("[Dry Run] Skipped writing to database.")

    # 6. Generate and save quality report
    logger.info("Generating quality report...")
    report_rows = generate_quality_report(
        matches=matches,
        unmatched_usgs=unmatched_usgs,
        unmatched_emsc=unmatched_emsc,
        clean_records=clean_records,
        raw_emsc_events=emsc_events,
    )
    saved_report_file = save_quality_report(report_rows, report_path)

    finished_at = datetime.datetime.now(datetime.timezone.utc)
    duration = (finished_at - started_at).total_seconds()
    logger.info("Reconciliation pipeline finished in %.2f seconds.", duration)

    return {
        "status": "success",
        "matches_count": len(matches),
        "usgs_only_count": len(unmatched_usgs),
        "emsc_only_count": len(unmatched_emsc),
        "total_clean_records": len(clean_records),
        "rows_upserted": rows_upserted,
        "report_path": str(saved_report_file),
        "duration_seconds": duration,
    }


# =============================================================================
# CLI Entrypoint
# =============================================================================
def parse_args(args: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reconcile USGS and EMSC earthquake events into events_clean table."
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Start timestamp / date filter (e.g. 2026-01-01 or 2026-01-01T00:00:00Z)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End timestamp / date filter (e.g. 2026-02-01 or 2026-02-01T00:00:00Z)",
    )
    parser.add_argument(
        "--report-path",
        type=str,
        default="reports/quality_report.csv",
        help="Path where quality report CSV should be saved (default: reports/quality_report.csv)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Batch size for database upserts (default: 5000)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run reconciliation and generate report without writing to database.",
    )
    return parser.parse_args(args)


def main() -> None:
    cli_args = parse_args()
    run_reconcile(
        start=cli_args.start,
        end=cli_args.end,
        report_path=cli_args.report_path,
        batch_size=cli_args.batch_size,
        dry_run=cli_args.dry_run,
    )


if __name__ == "__main__":
    main()
