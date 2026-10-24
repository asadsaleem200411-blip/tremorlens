"""
EMSC Earthquake Ingestion Pipeline (TremorLens ETL).

Fetches earthquake events from the EMSC SeismicPortal FDSN API in monthly time windows,
validates records with Pydantic, upserts valid events into the PostgreSQL
`emsc_events` table, routes invalid records into `rejected_records`,
and tracks execution status and row metrics in `ingest_runs`.
"""

import argparse
import datetime
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

import requests
from dateutil.relativedelta import relativedelta
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator
from sqlalchemy import Engine, text

# Ensure project root is available on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.db import get_engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("etl.emsc")

# EMSC SeismicPortal FDSN Web Service API Endpoint
EMSC_API_URL = "https://www.seismicportal.eu/fdsnws/event/1/query"

# SQL Statements
UPSERT_EMSC_EVENT_SQL = text("""
INSERT INTO emsc_events (
    event_id, event_time, latitude, longitude, depth_km, magnitude,
    mag_type, place, felt, cdi, mmi, sig, auth, source_catalog,
    last_update, raw_payload, ingested_at
) VALUES (
    :event_id, :event_time, :latitude, :longitude, :depth_km, :magnitude,
    :mag_type, :place, :felt, :cdi, :mmi, :sig, :auth, :source_catalog,
    :last_update, CAST(:raw_payload AS JSONB), NOW()
)
ON CONFLICT (event_id) DO UPDATE SET
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
    auth = EXCLUDED.auth,
    source_catalog = EXCLUDED.source_catalog,
    last_update = EXCLUDED.last_update,
    raw_payload = EXCLUDED.raw_payload,
    ingested_at = NOW();
""")

INSERT_REJECTED_RECORD_SQL = text("""
INSERT INTO rejected_records (
    source, raw_payload, error_message, rejected_at
) VALUES (
    :source, CAST(:raw_payload AS JSONB), :error_message, NOW()
);
""")

INSERT_INGEST_RUN_SQL = text("""
INSERT INTO ingest_runs (
    source, started_at, finished_at, rows_fetched, rows_loaded, rows_rejected, status, error_message
) VALUES (
    :source, :started_at, :finished_at, :rows_fetched, :rows_loaded, :rows_rejected, :status, :error_message
);
""")


class EMSCEvent(BaseModel):
    """
    Pydantic model representing a single earthquake event from EMSC SeismicPortal.

    Validates latitude (-90..90), longitude (-180..180), depth (-5..800 km),
    and enforces magnitude is not null.
    """

    model_config = ConfigDict(extra="ignore")

    id: str
    event_time: datetime.datetime
    latitude: float = Field(..., ge=-90.0, le=90.0)
    longitude: float = Field(..., ge=-180.0, le=180.0)
    depth_km: Optional[float] = Field(default=None, ge=-5.0, le=800.0)
    magnitude: float
    mag_type: Optional[str] = None
    place: Optional[str] = None
    felt: Optional[int] = None
    cdi: Optional[float] = None
    mmi: Optional[float] = None
    sig: Optional[int] = None

    # Additional metadata fields mapping to the `emsc_events` database schema
    auth: Optional[str] = None
    source_catalog: Optional[str] = None
    last_update: Optional[datetime.datetime] = None
    raw_payload: Optional[Any] = None

    @model_validator(mode="before")
    @classmethod
    def pre_populate_aliases(cls, data: Any) -> Any:
        if isinstance(data, dict):
            # Normalize dictionary keys for convenient initialization
            if "id" not in data and "unid" in data:
                data["id"] = data["unid"]
            if "event_time" not in data and "time" in data:
                data["event_time"] = data["time"]
            if "latitude" not in data and "lat" in data:
                data["latitude"] = data["lat"]
            if "longitude" not in data and "lon" in data:
                data["longitude"] = data["lon"]
            if "depth_km" not in data and "depth" in data:
                data["depth_km"] = data["depth"]
            if "magnitude" not in data and "mag" in data:
                data["magnitude"] = data["mag"]
            if "mag_type" not in data:
                if "magtype" in data:
                    data["mag_type"] = data["magtype"]
                elif "magType" in data:
                    data["mag_type"] = data["magType"]
            if "place" not in data and "flynn_region" in data:
                data["place"] = data["flynn_region"]
            if "last_update" not in data and "lastupdate" in data:
                data["last_update"] = data["lastupdate"]
        return data

    @field_validator("id")
    @classmethod
    def validate_id(cls, v: str) -> str:
        if not v or not str(v).strip():
            raise ValueError("id must not be empty")
        return str(v).strip()

    @field_validator("event_time", mode="before")
    @classmethod
    def parse_event_time(cls, v: Any) -> datetime.datetime:
        if v is None:
            raise ValueError("event_time must not be null")
        if isinstance(v, (int, float)):
            # Handle epoch milliseconds or seconds
            if v > 1e11:
                return datetime.datetime.fromtimestamp(v / 1000.0, tz=datetime.timezone.utc)
            return datetime.datetime.fromtimestamp(v, tz=datetime.timezone.utc)
        if isinstance(v, str):
            dt = datetime.datetime.fromisoformat(v.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            return dt.astimezone(datetime.timezone.utc)
        if isinstance(v, datetime.datetime):
            if v.tzinfo is None:
                return v.replace(tzinfo=datetime.timezone.utc)
            return v.astimezone(datetime.timezone.utc)
        raise ValueError(f"Invalid event_time format: {v}")

    @field_validator("last_update", mode="before")
    @classmethod
    def parse_last_update(cls, v: Any) -> Optional[datetime.datetime]:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            if v > 1e11:
                return datetime.datetime.fromtimestamp(v / 1000.0, tz=datetime.timezone.utc)
            return datetime.datetime.fromtimestamp(v, tz=datetime.timezone.utc)
        if isinstance(v, str):
            if not v.strip():
                return None
            dt = datetime.datetime.fromisoformat(v.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            return dt.astimezone(datetime.timezone.utc)
        if isinstance(v, datetime.datetime):
            if v.tzinfo is None:
                return v.replace(tzinfo=datetime.timezone.utc)
            return v.astimezone(datetime.timezone.utc)
        return None

    @field_validator("latitude")
    @classmethod
    def validate_latitude(cls, v: float) -> float:
        if v is None or not (-90.0 <= v <= 90.0):
            raise ValueError(f"latitude must be between -90 and 90, got {v}")
        return v

    @field_validator("longitude")
    @classmethod
    def validate_longitude(cls, v: float) -> float:
        if v is None or not (-180.0 <= v <= 180.0):
            raise ValueError(f"longitude must be between -180 and 180, got {v}")
        return v

    @field_validator("depth_km")
    @classmethod
    def validate_depth(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (-5.0 <= v <= 800.0):
            raise ValueError(f"depth_km must be between -5 and 800 km, got {v}")
        return v

    @field_validator("magnitude", mode="before")
    @classmethod
    def validate_magnitude(cls, v: Any) -> Any:
        if v is None:
            raise ValueError("magnitude must not be null")
        return v

    @classmethod
    def from_feature(cls, feature: dict) -> "EMSCEvent":
        """
        Parses an EMSC GeoJSON feature into an EMSCEvent instance.
        """
        props = feature.get("properties") or {}
        geometry = feature.get("geometry") or {}
        coords = geometry.get("coordinates") or []

        # In GeoJSON: coordinates = [longitude, latitude, elevation/negative depth]
        lon = props.get("lon")
        if lon is None and len(coords) > 0:
            lon = coords[0]

        lat = props.get("lat")
        if lat is None and len(coords) > 1:
            lat = coords[1]

        depth = props.get("depth")
        if depth is None and len(coords) > 2:
            # Elevation in GeoJSON is negative depth; earthquake depth is positive in km
            depth = abs(coords[2]) if coords[2] is not None else None

        event_id = feature.get("id") or props.get("unid")
        place = props.get("flynn_region") or props.get("place")
        mag = props.get("mag")
        mag_type = props.get("magtype") or props.get("mag_type") or props.get("magType")

        return cls(
            id=str(event_id) if event_id is not None else "",
            event_time=props.get("time"),
            latitude=lat,
            longitude=lon,
            depth_km=depth,
            magnitude=mag,
            mag_type=mag_type,
            place=place,
            felt=props.get("felt"),
            cdi=props.get("cdi"),
            mmi=props.get("mmi"),
            sig=props.get("sig"),
            auth=props.get("auth"),
            source_catalog=props.get("source_catalog"),
            last_update=props.get("lastupdate") or props.get("last_update"),
            raw_payload=feature,
        )


def generate_monthly_windows(
    start_date: datetime.date, end_date: datetime.date
) -> Generator[Tuple[datetime.datetime, datetime.datetime], None, None]:
    """
    Generates monthly time window ranges from start_date to end_date (inclusive).

    Monthly chunking keeps individual API response event counts well within
    EMSC query response limits.
    """
    start_dt = datetime.datetime.combine(start_date, datetime.time.min, tzinfo=datetime.timezone.utc)
    end_dt = datetime.datetime.combine(end_date, datetime.time.max, tzinfo=datetime.timezone.utc)

    curr = start_dt
    while curr < end_dt:
        next_dt = curr + relativedelta(months=1)
        window_end = min(next_dt, end_dt)
        yield curr, window_end
        curr = window_end


def fetch_emsc_events(
    url: str,
    params: dict,
    max_retries: int = 5,
    backoff_factor: float = 2.0,
    initial_delay: float = 1.0,
    timeout: int = 60,
    session: Optional[requests.Session] = None,
) -> dict:
    """
    Fetches earthquake data from the EMSC SeismicPortal FDSN API with exponential backoff retry.
    Retries on connection errors, timeouts, HTTP 429 (rate-limit), and HTTP 5xx errors.
    """
    s = session or requests.Session()
    delay = initial_delay

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(
                "Requesting EMSC API (attempt %d/%d): %s [starttime=%s, endtime=%s]",
                attempt,
                max_retries,
                url,
                params.get("starttime"),
                params.get("endtime"),
            )
            response = s.get(url, params=params, timeout=timeout)

            # HTTP 204 means no events matched the query in this window
            if response.status_code == 204:
                logger.info("EMSC returned 204 (No Content): 0 events for this window.")
                return {"type": "FeatureCollection", "features": []}

            # Retry on rate limiting (429) or transient server errors (5xx)
            if response.status_code == 429 or response.status_code >= 500:
                response.raise_for_status()

            # Client errors (4xx other than 429) should not be retried
            if 400 <= response.status_code < 500:
                response.raise_for_status()

            return response.json()

        except requests.exceptions.RequestException as e:
            if attempt == max_retries:
                logger.error("EMSC API request failed after %d attempts: %s", max_retries, e)
                raise
            logger.warning(
                "EMSC API request failed (attempt %d/%d): %s. Retrying in %.1f seconds...",
                attempt,
                max_retries,
                e,
                delay,
            )
            time.sleep(delay)
            delay *= backoff_factor

    return {"type": "FeatureCollection", "features": []}


def upsert_valid_events(engine: Engine, events: List[EMSCEvent]) -> int:
    """
    Upserts validated earthquake records into `emsc_events` using
    INSERT ... ON CONFLICT (event_id) DO UPDATE.
    """
    if not events:
        return 0

    records = [
        {
            "event_id": event.id,
            "event_time": event.event_time,
            "latitude": event.latitude,
            "longitude": event.longitude,
            "depth_km": event.depth_km,
            "magnitude": event.magnitude,
            "mag_type": event.mag_type,
            "place": event.place,
            "felt": event.felt,
            "cdi": event.cdi,
            "mmi": event.mmi,
            "sig": event.sig,
            "auth": event.auth,
            "source_catalog": event.source_catalog,
            "last_update": event.last_update,
            "raw_payload": (
                json.dumps(event.raw_payload) if event.raw_payload is not None else None
            ),
        }
        for event in events
    ]

    with engine.begin() as conn:
        conn.execute(UPSERT_EMSC_EVENT_SQL, records)

    return len(records)


def insert_rejected_records(engine: Engine, rejected: List[Dict[str, Any]]) -> int:
    """
    Inserts rejected records into `rejected_records` with raw_payload and error_message.
    """
    if not rejected:
        return 0

    records = [
        {
            "source": item.get("source", "emsc"),
            "raw_payload": (
                json.dumps(item["raw_payload"])
                if isinstance(item["raw_payload"], (dict, list))
                else json.dumps({"raw": str(item["raw_payload"])})
            ),
            "error_message": str(item["error_message"]),
        }
        for item in rejected
    ]

    with engine.begin() as conn:
        conn.execute(INSERT_REJECTED_RECORD_SQL, records)

    return len(records)


def record_ingest_run(
    engine: Engine,
    source: str,
    started_at: datetime.datetime,
    finished_at: datetime.datetime,
    rows_fetched: int,
    rows_loaded: int,
    rows_rejected: int,
    status: str,
    error_message: Optional[str] = None,
) -> None:
    """
    Writes a record into `ingest_runs` with metadata and execution metrics.
    """
    with engine.begin() as conn:
        conn.execute(
            INSERT_INGEST_RUN_SQL,
            {
                "source": source,
                "started_at": started_at,
                "finished_at": finished_at,
                "rows_fetched": rows_fetched,
                "rows_loaded": rows_loaded,
                "rows_rejected": rows_rejected,
                "status": status,
                "error_message": error_message,
            },
        )


def run_pipeline(
    start_date: datetime.date,
    end_date: datetime.date,
    engine: Optional[Engine] = None,
    min_magnitude: float = 4.5,
) -> Dict[str, Any]:
    """
    Main orchestration function for the EMSC earthquake ingestion pipeline.

    - Iterates over monthly time windows between start_date and end_date.
    - Queries the EMSC SeismicPortal FDSN API for events with format=json, minmag=4.5, orderby=time-asc.
    - Validates features using the EMSCEvent Pydantic model.
    - Upserts valid events into emsc_events.
    - Inserts invalid events into rejected_records.
    - Logs metadata and updates ingest_runs upon completion or error.
    """
    if engine is None:
        engine = get_engine()

    started_at = datetime.datetime.now(datetime.timezone.utc)
    total_fetched = 0
    total_loaded = 0
    total_rejected = 0
    status = "success"
    run_error: Optional[str] = None

    logger.info("Initializing EMSC ingestion pipeline from %s to %s", start_date, end_date)
    session = requests.Session()

    try:
        for window_start, window_end in generate_monthly_windows(start_date, end_date):
            starttime_str = window_start.strftime("%Y-%m-%dT%H:%M:%S")
            endtime_str = window_end.strftime("%Y-%m-%dT%H:%M:%S")

            params = {
                "format": "json",
                "minmag": min_magnitude,
                "orderby": "time-asc",
                "starttime": starttime_str,
                "endtime": endtime_str,
            }

            geojson_data = fetch_emsc_events(
                url=EMSC_API_URL,
                params=params,
                session=session,
            )

            features = geojson_data.get("features") or []
            total_fetched += len(features)
            logger.info(
                "Window %s to %s: retrieved %d features from EMSC",
                starttime_str,
                endtime_str,
                len(features),
            )

            valid_events: List[EMSCEvent] = []
            rejected_items: List[Dict[str, Any]] = []

            for feat in features:
                try:
                    event = EMSCEvent.from_feature(feat)
                    valid_events.append(event)
                except (ValidationError, Exception) as val_err:
                    rejected_items.append({
                        "source": "emsc",
                        "raw_payload": feat,
                        "error_message": str(val_err),
                    })

            # Upsert valid events into database
            loaded_count = upsert_valid_events(engine, valid_events)
            total_loaded += loaded_count

            # Store any rejected records
            rejected_count = insert_rejected_records(engine, rejected_items)
            total_rejected += rejected_count

            logger.info(
                "Window %s to %s complete: loaded=%d, rejected=%d (Running totals: fetched=%d, loaded=%d, rejected=%d)",
                starttime_str,
                endtime_str,
                loaded_count,
                rejected_count,
                total_fetched,
                total_loaded,
                total_rejected,
            )

    except Exception as exc:
        status = "failed"
        run_error = str(exc)
        logger.exception("EMSC ingestion pipeline encountered an error: %s", exc)
        raise

    finally:
        finished_at = datetime.datetime.now(datetime.timezone.utc)
        try:
            record_ingest_run(
                engine=engine,
                source="emsc",
                started_at=started_at,
                finished_at=finished_at,
                rows_fetched=total_fetched,
                rows_loaded=total_loaded,
                rows_rejected=total_rejected,
                status=status,
                error_message=run_error,
            )
            logger.info(
                "Recorded ingest_runs row (source=emsc, status=%s, fetched=%d, loaded=%d, rejected=%d, duration=%s)",
                status,
                total_fetched,
                total_loaded,
                total_rejected,
                finished_at - started_at,
            )
        except Exception as db_err:
            logger.error("Failed to write ingestion run summary to ingest_runs: %s", db_err)

    return {
        "status": status,
        "rows_fetched": total_fetched,
        "rows_loaded": total_loaded,
        "rows_rejected": total_rejected,
        "error_message": run_error,
    }


def parse_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    """
    Parses command line arguments --start and --end (YYYY-MM-DD).
    """
    parser = argparse.ArgumentParser(
        description="Fetch and ingest EMSC earthquake events into PostgreSQL."
    )
    parser.add_argument(
        "--start",
        type=str,
        required=True,
        help="Start date in YYYY-MM-DD format (inclusive).",
    )
    parser.add_argument(
        "--end",
        type=str,
        required=True,
        help="End date in YYYY-MM-DD format (inclusive).",
    )

    parsed = parser.parse_args(args)

    try:
        parsed.start_date = datetime.datetime.strptime(parsed.start, "%Y-%m-%d").date()
    except ValueError as e:
        parser.error(f"Invalid --start date format '{parsed.start}'. Expected YYYY-MM-DD: {e}")

    try:
        parsed.end_date = datetime.datetime.strptime(parsed.end, "%Y-%m-%d").date()
    except ValueError as e:
        parser.error(f"Invalid --end date format '{parsed.end}'. Expected YYYY-MM-DD: {e}")

    if parsed.start_date > parsed.end_date:
        parser.error(f"--start date ({parsed.start}) cannot be after --end date ({parsed.end})")

    return parsed


def main() -> None:
    args = parse_args()
    run_pipeline(start_date=args.start_date, end_date=args.end_date)


if __name__ == "__main__":
    main()
