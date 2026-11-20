"""TremorLens read-only API.
This service is an adapter for the existing PostgreSQL catalog. It does not
run ETL, alter tables, reconcile events, or write to the database.
"""
from __future__ import annotations
import os
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))
REGIONS = {
    "Pakistan": {"lat_min": 20, "lat_max": 38, "lon_min": 60, "lon_max": 78},
    "Hindu Kush": {"lat_min": 32, "lat_max": 38, "lon_min": 68, "lon_max": 75},
    "South Asia": {"lat_min": -5, "lat_max": 40, "lon_min": 55, "lon_max": 105},
    "Japan": {"lat_min": 24, "lat_max": 46, "lon_min": 123, "lon_max": 150},
    "California": {"lat_min": 32, "lat_max": 42, "lon_min": -125, "lon_max": -114},
    "World": None,
}
app = FastAPI(title="TremorLens API", version="1.0.0", description="Read-only API for the TremorLens earthquake catalog")
# CORS is registered once. Localhost and private-LAN origins are supported
# for laptop/mobile development. For a public deployment, set CORS_ORIGINS on
# the backend to the exact frontend origin(s), comma-separated, e.g.
# CORS_ORIGINS=https://your-dashboard.example.com
_default_origins = "http://localhost:5173,http://127.0.0.1:5173"
_allowed_origins = [
    origin.strip().rstrip("/")
    for origin in os.getenv("CORS_ORIGINS", _default_origins).split(",")
    if origin.strip()
]
_LOCAL_ORIGIN_REGEX = (
    r"https?://(localhost|127\.0\.0\.1|10(?:\.\d{1,3}){3}|"
    r"192\.168(?:\.\d{1,3}){2}|172\.(?:1[6-9]|2\d|3[01])(?:\.\d{1,3}){2})(?::\d+)?"
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_origin_regex=_LOCAL_ORIGIN_REGEX,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@lru_cache(maxsize=1)
def get_engine() -> Engine:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not configured. Add it to the project .env file.")
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    if url.startswith("postgresql://"):
        url = "postgresql+psycopg://" + url[len("postgresql://"):]
    return create_engine(url, pool_pre_ping=True, pool_recycle=1800)
def fetch_all(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    try:
        with get_engine().connect() as conn:
            rows = conn.execute(text(sql), params or {}).mappings().all()
            return [dict(row) for row in rows]
    except Exception as exc:
        message = str(exc)
        if "DATABASE_URL" in message or "configured" in message:
            raise HTTPException(status_code=503, detail=message) from exc
        raise HTTPException(status_code=503, detail="Database is temporarily unavailable. Check the database connection and schema.") from exc
def clean(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def convert(value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        if hasattr(value, "item"):
            return value.item()
        return value
    return [{k: convert(v) for k, v in row.items()} for row in rows]
@app.get("/api/health")
def health():
    try:
        fetch_all("SELECT 1 AS ok")
        return {"status": "ok", "database": "connected", "checked_at": datetime.now(timezone.utc).isoformat()}
    except HTTPException as exc:
        return {"status": "degraded", "database": "unavailable", "detail": exc.detail}
@app.get("/api/overview")
def overview():
    rows = fetch_all("""SELECT
      (SELECT COUNT(*) FROM usgs_events) AS usgs,
      (SELECT COUNT(*) FROM emsc_events) AS emsc,
      (SELECT COUNT(*) FROM events_clean) AS clean,
      (SELECT COUNT(*) FROM events_clean WHERE source='both') AS both,
      (SELECT COUNT(*) FROM rejected_records) AS rejected,
      (SELECT COUNT(*) FROM quality_issues) AS warnings,
      (SELECT MAX(event_time) FROM usgs_events) AS latest_event,
      (SELECT MAX(finished_at) FROM ingest_runs) AS last_run""")
    runs = fetch_all("SELECT id, source, started_at, finished_at, rows_fetched, rows_loaded, rows_rejected, status FROM ingest_runs ORDER BY id DESC LIMIT 10")
    return {"metrics": clean(rows)[0], "runs": clean(runs)}
@app.get("/api/yearly")
def yearly():
    rows = fetch_all("""SELECT CAST(EXTRACT(YEAR FROM event_time) AS INTEGER) AS year, source, COUNT(*) AS count
      FROM events_clean GROUP BY 1,2 ORDER BY 1""")
    years: dict[int, dict[str, Any]] = {}
    for row in rows:
        item = years.setdefault(int(row["year"]), {"year": int(row["year"]), "both": 0, "usgs_only": 0, "emsc_only": 0})
        item[row["source"]] = int(row["count"])
    result = []
    for item in years.values():
        item["usgs_total"] = item["both"] + item["usgs_only"]
        item["emsc_total"] = item["both"] + item["emsc_only"]
        item["usgs_match_rate"] = item["both"] / max(1, item["usgs_total"])
        item["emsc_match_rate"] = item["both"] / max(1, item["emsc_total"])
        result.append(item)
    return result
@app.get("/api/events")
def events(
    year_from: int = Query(2000, ge=1900, le=2100), year_to: int = Query(2100, ge=1900, le=2100),
    min_magnitude: float = Query(0, ge=-2, le=10), max_magnitude: float = Query(10, ge=-2, le=10),
    region: str = Query("World"), limit: int = Query(3000, ge=1, le=10000),
):
    if year_from > year_to:
        raise HTTPException(422, "year_from must be less than or equal to year_to")
    if region not in REGIONS:
        raise HTTPException(422, "Unknown region")
    params: dict[str, Any] = {"year_from": year_from, "year_to": year_to, "min_mag": min_magnitude, "max_mag": max_magnitude, "limit": limit}
    where = ["event_time >= make_date(:year_from,1,1)", "event_time < make_date(:year_to + 1,1,1)", "magnitude BETWEEN :min_mag AND :max_mag"]
    bounds = REGIONS[region]
    if bounds:
        params.update(bounds)
        where += ["latitude BETWEEN :lat_min AND :lat_max", "longitude BETWEEN :lon_min AND :lon_max"]
    sql = f"SELECT event_id,event_time,latitude,longitude,depth_km,magnitude,mag_type,place,source FROM events_clean WHERE {' AND '.join(where)} ORDER BY event_time DESC LIMIT :limit"
    return clean(fetch_all(sql, params))
@app.get("/api/events/paged")
def events_paged(
    year_from: int = Query(2000, ge=1900, le=2100),
    year_to: int = Query(2100, ge=1900, le=2100),
    min_magnitude: float = Query(0, ge=-2, le=10),
    max_magnitude: float = Query(10, ge=-2, le=10),
    region: str = Query("World"),
    limit: int = Query(2000, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """Return one page plus the exact count for the same filtered catalog query."""
    if year_from > year_to:
        raise HTTPException(422, "year_from must be less than or equal to year_to")
    if min_magnitude > max_magnitude:
        raise HTTPException(422, "min_magnitude must be less than or equal to max_magnitude")
    if region not in REGIONS:
        raise HTTPException(422, "Unknown region")

    params: dict[str, Any] = {
        "year_from": year_from,
        "year_to": year_to,
        "min_mag": min_magnitude,
        "max_mag": max_magnitude,
        "limit": limit,
        "offset": offset,
    }
    where = [
        "event_time >= make_date(:year_from,1,1)",
        "event_time < make_date(:year_to + 1,1,1)",
        "magnitude BETWEEN :min_mag AND :max_mag",
    ]
    bounds = REGIONS[region]
    if bounds:
        params.update(bounds)
        where += [
            "latitude BETWEEN :lat_min AND :lat_max",
            "longitude BETWEEN :lon_min AND :lon_max",
        ]
    where_sql = " AND ".join(where)
    count_rows = fetch_all(
        f"SELECT COUNT(*) AS total FROM events_clean WHERE {where_sql}",
        {k: v for k, v in params.items() if k not in ("limit", "offset")},
    )
    total = int(count_rows[0]["total"]) if count_rows else 0
    rows = fetch_all(
        f"""SELECT event_id,event_time,latitude,longitude,depth_km,magnitude,mag_type,place,source
        FROM events_clean WHERE {where_sql}
        ORDER BY event_time DESC, event_id
        LIMIT :limit OFFSET :offset""",
        params,
    )
    return {"items": clean(rows), "total": total, "limit": limit, "offset": offset}


@app.get("/api/live")
def live(feed: str = Query("all_day", pattern="^(all_hour|all_day|all_week|all_month)$")):
    url = f"https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/{feed}.geojson"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        features = response.json().get("features", [])
    except requests.RequestException as exc:
        raise HTTPException(502, "The live USGS feed is temporarily unavailable.") from exc
    events_out = []
    for feature in features:
        props = feature.get("properties") or {}
        coords = (feature.get("geometry") or {}).get("coordinates") or []
        if props.get("mag") is None or props.get("time") is None or len(coords) < 2:
            continue
        events_out.append({"event_id": feature.get("id"), "time": datetime.fromtimestamp(props["time"] / 1000, timezone.utc).isoformat(), "magnitude": props["mag"], "longitude": coords[0], "latitude": coords[1], "depth_km": coords[2] if len(coords) > 2 else None, "place": props.get("place") or "Unknown location", "url": props.get("url")})
    return {"feed": feed, "count": len(events_out), "events": events_out}
@app.get("/api/quality/summary")
def quality_summary():
    issues = fetch_all("SELECT issue,source,COUNT(*) AS count FROM quality_issues GROUP BY issue,source ORDER BY count DESC")
    rejected = fetch_all("SELECT id,source,error_message,rejected_at FROM rejected_records ORDER BY id DESC LIMIT 30")
    defaults = fetch_all("""SELECT CAST(EXTRACT(YEAR FROM event_time) AS INTEGER) AS year,
      SUM(CASE WHEN depth_km IN (10,30,33,35) THEN 1 ELSE 0 END) AS default_like,
      COUNT(*) AS total FROM usgs_events GROUP BY 1 ORDER BY 1""")
    yearly = fetch_all("""SELECT CAST(EXTRACT(YEAR FROM event_time) AS INTEGER) AS year,
      COUNT(*) FILTER (WHERE source='both') AS both,
      COUNT(*) FILTER (WHERE source='usgs_only') AS usgs_only,
      COUNT(*) FILTER (WHERE source='emsc_only') AS emsc_only
      FROM events_clean GROUP BY 1 ORDER BY 1""")
    rates = []
    for row in yearly:
        usgs_total = int(row['both'] or 0) + int(row['usgs_only'] or 0)
        emsc_total = int(row['both'] or 0) + int(row['emsc_only'] or 0)
        rates.append({"year": row["year"], "usgs_match_rate": int(row['both'] or 0) / max(1, usgs_total),
          "emsc_match_rate": int(row['both'] or 0) / max(1, emsc_total)})
    return {"issues": clean(issues), "rejected": clean(rejected), "default_depths": clean(defaults),
      "yearly_match_rates": clean(rates)}
@app.get("/api/compare/summary")
def compare_summary():
    rows = fetch_all("""SELECT u.magnitude AS mag_usgs,e.magnitude AS mag_emsc,
      u.depth_km AS depth_usgs,e.depth_km AS depth_emsc
      FROM events_clean c JOIN usgs_events u ON u.event_id=c.matched_usgs_id
      JOIN emsc_events e ON e.event_id=c.matched_emsc_id WHERE c.source='both'
      AND u.magnitude IS NOT NULL AND e.magnitude IS NOT NULL LIMIT 50000""")
    n = len(rows)
    diffs = [float(x["mag_emsc"])-float(x["mag_usgs"]) for x in rows]
    abs_depth = [abs(float(x["depth_emsc"])-float(x["depth_usgs"])) for x in rows if x["depth_emsc"] is not None and x["depth_usgs"] is not None]
    diffs_sorted = sorted(diffs)
    median = (diffs_sorted[(n-1)//2] + diffs_sorted[n//2]) / 2 if n else None
    mean = sum(diffs)/n if n else None
    return {"matched_count": n, "mean_magnitude_difference": mean, "median_magnitude_difference": median,
      "within_0_1": sum(abs(x)<=0.1 for x in diffs)/n if n else None,
      "within_0_3": sum(abs(x)<=0.3 for x in diffs)/n if n else None,
      "mean_absolute_depth_difference_km": sum(abs_depth)/len(abs_depth) if abs_depth else None,
      "magnitude_differences": diffs[::max(1,n//2000)],
      "magnitude_pairs": [{"usgs": float(x["mag_usgs"]), "emsc": float(x["mag_emsc"])} for x in rows[::max(1,n//1800)]],
      "depth_pairs": [{"usgs": float(x["depth_usgs"]), "emsc": float(x["depth_emsc"])} for x in rows if x["depth_usgs"] is not None and x["depth_emsc"] is not None][::max(1,len(abs_depth)//1800)]}
@app.get("/api/aftershocks/mainshocks")
def mainshocks():
    return clean(fetch_all("SELECT event_id,event_time,latitude,longitude,depth_km,magnitude,mag_type,place FROM usgs_events WHERE magnitude>=7.5 ORDER BY magnitude DESC LIMIT 60"))
@app.get("/api/aftershocks/window")
def aftershock_window(
    event_id: str = Query(..., min_length=1), days: int = Query(30, ge=7, le=90),
    radius_mode: str = Query("scaled", pattern="^(fixed|scaled)$"),
):
    """Explore M4.5+ events after a mainshock using the dashboard's distance rule."""
    main = fetch_all("SELECT event_id,event_time,latitude,longitude,magnitude,place FROM usgs_events WHERE event_id=:id", {"id": event_id})
    if not main:
        raise HTTPException(404, "Mainshock not found")
    m = main[0]
    import math
    magnitude = float(m["magnitude"])
    radius = 300.0 if radius_mode == "fixed" else max(50.0, 10 ** (-2.44 + 0.59 * magnitude))
    lat = float(m["latitude"])
    lon = float(m["longitude"])
    lat_delta = radius / 111.0
    cos_lat = max(0.05, abs(math.cos(math.radians(lat))))
    lon_delta = min(180.0, radius / (111.0 * cos_lat))
    params = {"id": event_id, "days": days, "lat_min": max(-90.0, lat-lat_delta), "lat_max": min(90.0, lat+lat_delta), "min_mag": 4.5}
    where_lon = ""
    if lon-lon_delta >= -180 and lon+lon_delta <= 180:
        where_lon = "AND longitude BETWEEN :lon_min AND :lon_max"
        params.update({"lon_min": lon-lon_delta, "lon_max": lon+lon_delta})
    rows = fetch_all(f"""SELECT event_id,event_time,latitude,longitude,depth_km,magnitude,place FROM usgs_events
      WHERE event_id<>:id AND magnitude>=:min_mag
      AND event_time>(SELECT event_time FROM usgs_events WHERE event_id=:id)
      AND event_time<=(SELECT event_time FROM usgs_events WHERE event_id=:id) + (:days * INTERVAL '1 day')
      AND latitude BETWEEN :lat_min AND :lat_max {where_lon}
      ORDER BY event_time LIMIT 20000""", params)
    selected = []
    for row in rows:
        dlat = math.radians(float(row["latitude"])-lat)
        dlon = math.radians(float(row["longitude"])-lon)
        a = math.sin(dlat/2)**2 + math.cos(math.radians(lat))*math.cos(math.radians(float(row["latitude"])))*math.sin(dlon/2)**2
        distance = 6371.0 * 2 * math.asin(min(1.0, math.sqrt(a)))
        elapsed = (row["event_time"]-m["event_time"]).total_seconds()/86400
        if distance <= radius and 0 < elapsed <= days:
            item = dict(row)
            item["days"] = elapsed
            item["distance_km"] = distance
            selected.append(item)
    return {"mainshock": clean(main)[0], "radius_km": radius, "days": days,
      "events": clean(selected), "first_day_count": sum(1 for x in selected if x["days"] <= 1)}
@app.get("/api/analysis")
def analysis(period: str = Query("recommended", pattern="^(recommended|early|all)$"), mc: float = Query(4.8, ge=4.5, le=5.5), hide_defaults: bool = Query(True)):
    """Aggregated EDA outputs based on the USGS source catalog."""
    bounds = {"recommended": (2005, 2026), "early": (2000, 2004), "all": (2000, 2026)}[period]
    mags_rows = fetch_all("""SELECT magnitude FROM usgs_events WHERE event_time >= make_date(:lo,1,1)
      AND event_time < make_date(:hi,1,1) AND magnitude IS NOT NULL AND magnitude >= :mc""",
      {"lo": bounds[0], "hi": bounds[1] + 1, "mc": mc})
    magnitudes = sorted(float(r["magnitude"]) for r in mags_rows if r["magnitude"] is not None)
    n = len(magnitudes)
    b_value = standard_error = None
    cumulative = []
    if n >= 50:
        mean = sum(magnitudes) / n
        denominator = mean - (mc - 0.05)
        if denominator > 0:
            import math
            b_value = math.log10(math.e) / denominator
            variance_sum = sum((x - mean) ** 2 for x in magnitudes)
            standard_error = 2.30 * b_value ** 2 * math.sqrt(variance_sum / (n * (n - 1)))
            upper = max(mc, round(max(magnitudes) * 10) / 10)
            steps = int(round((upper - mc) / 0.1)) + 1
            for i in range(steps):
                threshold = round(mc + i * 0.1, 1)
                count = sum(x >= threshold - 1e-9 for x in magnitudes)
                if count:
                    cumulative.append({"magnitude": threshold, "observed": count, "fitted": 10 ** (math.log10(n) - b_value * (threshold - mc))})
    depth_rows = fetch_all("""SELECT width_bucket(depth_km,0,700,70) AS bucket, COUNT(*) AS count
      FROM usgs_events WHERE depth_km >= 0 AND depth_km <= 700
      AND event_time >= make_date(:lo,1,1) AND event_time < make_date(:hi,1,1)
      AND (:hide_defaults = FALSE OR depth_km NOT IN (10,30,33,35))
      GROUP BY 1 ORDER BY 1""", {"lo": bounds[0], "hi": bounds[1] + 1, "hide_defaults": hide_defaults})
    annual = fetch_all("""SELECT CAST(EXTRACT(YEAR FROM event_time) AS INTEGER) AS year,
      CASE WHEN magnitude >= 7 THEN '7.0+'
           WHEN magnitude >= 6 THEN '6.0–6.9'
           WHEN magnitude >= 5 THEN '5.0–5.9'
           WHEN magnitude >= 4.5 THEN '4.5–4.9' ELSE '<4.5' END AS band,
      COUNT(*) AS count FROM usgs_events WHERE event_time >= make_date(:lo,1,1)
      AND event_time < make_date(:hi,1,1) AND magnitude IS NOT NULL
      GROUP BY 1,2 ORDER BY 1,2""", {"lo": bounds[0], "hi": min(bounds[1], 2025) + 1})
    return {"period": period, "mc": mc, "sample_size": n, "b_value": b_value, "standard_error": standard_error,
      "gutenberg_richter": cumulative, "depth_histogram": [{"depth_km": (int(r["bucket"])-1)*10+5, "count": int(r["count"])} for r in depth_rows],
      "annual_magnitude_bands": clean(annual)}
