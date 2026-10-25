"""
Unit and integration tests for the TremorLens reconciliation pipeline (etl/reconcile.py).

Tests:
- Haversine distance calculation
- Threshold constants (60s, 100km, 0.5 magnitude)
- 1-to-1 event matching:
  - Exact match
  - Two EMSC events competing for one USGS event (resolved by time diff)
  - Two EMSC events competing for one USGS event (tied time, resolved by distance)
  - Two USGS events competing for one EMSC event
  - Threshold boundaries (time diff = 60s vs 60.1s; dist = 99km vs 101km; mag diff = 0.5 vs 0.51)
- Clean record creation:
  - source='both', USGS values preferred, USGS felt/cdi/mmi/sig kept, matched IDs populated
  - source='usgs_only' and 'emsc_only'
  - depth_is_default boolean column logic (depth_km = 10 exactly)
- Quality report generation:
  - Counts of both, usgs_only, emsc_only
  - Match rate per year
  - Headline stats restricted to 2005 onward with incomplete coverage note
  - Match rate split by EMSC auth = 'NEIC' vs all other authors
  - Matched pairs mean and spread (std) of magnitude, depth, and time diff by decade
  - Share of depth_is_default events per source and per decade
  - CSV file generation
- CLI argument parsing (--start, --end, --report-path)
- End-to-end reconciliation run with in-memory database
"""

import datetime
from pathlib import Path
import sys
import tempfile
from typing import Any, Dict

import pytest
from sqlalchemy import create_engine, text

# Ensure project root is available on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.reconcile import (
    DEFAULT_DEPTH_KM,
    MAX_DISTANCE_KM,
    MAX_MAG_DIFF,
    MAX_TIME_DIFF_SECONDS,
    TIME_WINDOW_SECONDS,
    build_clean_records,
    ensure_schema,
    generate_quality_report,
    haversine_distance,
    is_default_depth,
    match_events,
    parse_args,
    run_reconcile,
    save_quality_report,
)


# =============================================================================
# 1. Constants & Math Tests
# =============================================================================
def test_threshold_constants():
    """Verify that the matching threshold constants are properly defined."""
    assert MAX_TIME_DIFF_SECONDS == 60.0
    assert TIME_WINDOW_SECONDS == 60.0
    assert MAX_DISTANCE_KM == 100.0
    assert MAX_MAG_DIFF == 0.5
    assert DEFAULT_DEPTH_KM == 10.0


def test_haversine_distance():
    """Verify haversine distance calculation."""
    # Same point -> 0.0 km
    assert haversine_distance(0.0, 0.0, 0.0, 0.0) == 0.0
    assert haversine_distance(37.7749, -122.4194, 37.7749, -122.4194) == 0.0

    # 1 degree latitude difference along prime meridian ~ 111.19 km
    d_lat = haversine_distance(0.0, 0.0, 1.0, 0.0)
    assert pytest.approx(d_lat, rel=1e-3) == 111.19

    # 1 degree longitude difference at equator ~ 111.19 km
    d_lon = haversine_distance(0.0, 0.0, 0.0, 1.0)
    assert pytest.approx(d_lon, rel=1e-3) == 111.19

    # Poles
    d_pole = haversine_distance(90.0, 0.0, -90.0, 0.0)
    assert pytest.approx(d_pole, rel=1e-3) == 20015.08


def test_is_default_depth():
    """Verify detection of fixed default depth (10.0 km)."""
    assert is_default_depth(10.0) is True
    assert is_default_depth(10) is True
    assert is_default_depth(10.000001) is True
    assert is_default_depth(10.05) is False
    assert is_default_depth(9.9) is False
    assert is_default_depth(0.0) is False
    assert is_default_depth(-2.0) is False
    assert is_default_depth(100.0) is False
    assert is_default_depth(None) is False


# =============================================================================
# 2. Matching Logic Tests
# =============================================================================
def test_match_single_exact_event():
    """Verify exact identical event matches cleanly."""
    usgs = [{
        "event_id": "us1000",
        "event_time": "2026-05-01T12:00:00Z",
        "latitude": 38.0,
        "longitude": 23.0,
        "depth_km": 15.0,
        "magnitude": 5.2,
        "mag_type": "mw",
        "place": "Greece",
        "felt": 100,
        "cdi": 4.5,
        "mmi": 5.0,
        "sig": 450,
    }]
    emsc = [{
        "event_id": "em2000",
        "event_time": "2026-05-01T12:00:00Z",
        "latitude": 38.0,
        "longitude": 23.0,
        "depth_km": 14.0,
        "magnitude": 5.2,
        "mag_type": "mb",
        "place": "GREECE",
        "felt": 80,
        "cdi": 4.0,
        "mmi": 4.8,
        "sig": 420,
        "auth": "NEIC",
    }]

    matches, unmatched_usgs, unmatched_emsc = match_events(usgs, emsc)
    assert len(matches) == 1
    assert len(unmatched_usgs) == 0
    assert len(unmatched_emsc) == 0

    m = matches[0]
    assert m["usgs"]["event_id"] == "us1000"
    assert m["emsc"]["event_id"] == "em2000"
    assert m["time_diff_seconds"] == 0.0
    assert m["distance_km"] == 0.0
    assert m["magnitude_diff"] == 0.0
    assert m["depth_diff_km"] == 1.0


def test_two_emsc_compete_for_one_usgs_closest_by_time():
    """
    Two EMSC events compete for one USGS event.
    EMSC 1 has smaller time diff (5s vs 20s).
    The algorithm MUST choose EMSC 1 by time.
    """
    usgs = [{
        "event_id": "us_comp_1",
        "event_time": "2026-06-01T10:00:00Z",
        "latitude": 35.0,
        "longitude": 25.0,
        "depth_km": 10.0,
        "magnitude": 6.0,
        "mag_type": "mww",
        "place": "Crete",
    }]

    emsc = [
        {
            "event_id": "em_far_time",
            "event_time": "2026-06-01T10:00:20Z",  # 20s diff
            "latitude": 35.05,
            "longitude": 25.05,
            "depth_km": 10.0,
            "magnitude": 6.1,
            "auth": "EMSC",
        },
        {
            "event_id": "em_close_time",
            "event_time": "2026-06-01T10:00:05Z",  # 5s diff (wins!)
            "latitude": 35.1,
            "longitude": 25.1,
            "depth_km": 12.0,
            "magnitude": 5.9,
            "auth": "NEIC",
        },
    ]

    matches, unmatched_u, unmatched_e = match_events(usgs, emsc)
    assert len(matches) == 1
    assert len(unmatched_u) == 0
    assert len(unmatched_e) == 1

    # Winner is em_close_time
    assert matches[0]["emsc"]["event_id"] == "em_close_time"
    assert unmatched_e[0]["event_id"] == "em_far_time"


def test_two_emsc_compete_for_one_usgs_tied_time_closest_by_distance():
    """
    Two EMSC events compete for one USGS event with identical time diff (both 5s).
    The algorithm MUST choose the closer one by distance.
    """
    usgs = [{
        "event_id": "us_dist_comp",
        "event_time": "2026-06-01T10:00:00Z",
        "latitude": 0.0,
        "longitude": 0.0,
        "depth_km": 10.0,
        "magnitude": 5.0,
    }]

    emsc = [
        {
            "event_id": "em_far_dist",
            "event_time": "2026-06-01T10:00:05Z",  # 5s diff
            "latitude": 0.5,                       # ~55.6 km away
            "longitude": 0.0,
            "depth_km": 10.0,
            "magnitude": 5.0,
            "auth": "EMSC",
        },
        {
            "event_id": "em_close_dist",
            "event_time": "2026-06-01T10:00:05Z",  # 5s diff (tied!)
            "latitude": 0.1,                       # ~11.1 km away (wins!)
            "longitude": 0.0,
            "depth_km": 10.0,
            "magnitude": 5.0,
            "auth": "NEIC",
        },
    ]

    matches, unmatched_u, unmatched_e = match_events(usgs, emsc)
    assert len(matches) == 1
    assert matches[0]["emsc"]["event_id"] == "em_close_dist"
    assert len(unmatched_e) == 1
    assert unmatched_e[0]["event_id"] == "em_far_dist"


def test_two_usgs_compete_for_one_emsc():
    """
    Symmetric test: two USGS events compete for one EMSC event.
    The closer USGS event by time wins.
    """
    usgs = [
        {
            "event_id": "us_far",
            "event_time": "2026-07-01T08:00:30Z",  # 30s diff
            "latitude": 10.0,
            "longitude": 10.0,
            "depth_km": 20.0,
            "magnitude": 4.5,
        },
        {
            "event_id": "us_close",
            "event_time": "2026-07-01T08:00:02Z",  # 2s diff (wins!)
            "latitude": 10.0,
            "longitude": 10.0,
            "depth_km": 20.0,
            "magnitude": 4.5,
        },
    ]

    emsc = [{
        "event_id": "em_target",
        "event_time": "2026-07-01T08:00:00Z",
        "latitude": 10.0,
        "longitude": 10.0,
        "depth_km": 20.0,
        "magnitude": 4.5,
        "auth": "NEIC",
    }]

    matches, unmatched_u, unmatched_e = match_events(usgs, emsc)
    assert len(matches) == 1
    assert matches[0]["usgs"]["event_id"] == "us_close"
    assert len(unmatched_u) == 1
    assert unmatched_u[0]["event_id"] == "us_far"
    assert len(unmatched_e) == 0


def test_threshold_boundaries():
    """Verify boundary conditions for time, distance, and magnitude."""
    # 1. Time boundary: 60s matches, 60.1s does NOT match
    u_base = {
        "event_id": "u1",
        "event_time": "2026-01-01T00:00:00Z",
        "latitude": 0.0,
        "longitude": 0.0,
        "magnitude": 5.0,
    }

    e_exact_60 = [{
        "event_id": "e_60",
        "event_time": "2026-01-01T00:01:00Z",  # Exactly 60s
        "latitude": 0.0,
        "longitude": 0.0,
        "magnitude": 5.0,
    }]
    m_60, _, _ = match_events([u_base], e_exact_60)
    assert len(m_60) == 1

    e_over_60 = [{
        "event_id": "e_61",
        "event_time": "2026-01-01T00:01:00.100Z",  # 60.1s
        "latitude": 0.0,
        "longitude": 0.0,
        "magnitude": 5.0,
    }]
    m_over_60, u_un, e_un = match_events([u_base], e_over_60)
    assert len(m_over_60) == 0
    assert len(u_un) == 1
    assert len(e_un) == 1

    # 2. Distance boundary: 99 km matches, 101 km does NOT match
    # At lat=0, 1 deg lon = ~111.19 km.
    # 0.89 deg ~ 98.96 km (<100 km).
    # 0.92 deg ~ 102.3 km (>100 km).
    e_under_100km = [{
        "event_id": "e_99km",
        "event_time": "2026-01-01T00:00:00Z",
        "latitude": 0.0,
        "longitude": 0.89,
        "magnitude": 5.0,
    }]
    m_dist_ok, _, _ = match_events([u_base], e_under_100km)
    assert len(m_dist_ok) == 1

    e_over_100km = [{
        "event_id": "e_101km",
        "event_time": "2026-01-01T00:00:00Z",
        "latitude": 0.0,
        "longitude": 0.92,
        "magnitude": 5.0,
    }]
    m_dist_fail, _, _ = match_events([u_base], e_over_100km)
    assert len(m_dist_fail) == 0

    # 3. Magnitude boundary: 0.50 matches, 0.51 does NOT match
    e_mag_050 = [{
        "event_id": "e_mag_05",
        "event_time": "2026-01-01T00:00:00Z",
        "latitude": 0.0,
        "longitude": 0.0,
        "magnitude": 5.5,  # diff = 0.5
    }]
    m_mag_ok, _, _ = match_events([u_base], e_mag_050)
    assert len(m_mag_ok) == 1

    e_mag_051 = [{
        "event_id": "e_mag_051",
        "event_time": "2026-01-01T00:00:00Z",
        "latitude": 0.0,
        "longitude": 0.0,
        "magnitude": 5.51,  # diff = 0.51
    }]
    m_mag_fail, _, _ = match_events([u_base], e_mag_051)
    assert len(m_mag_fail) == 0


# =============================================================================
# 3. Clean Record Building Tests
# =============================================================================
def test_build_clean_records_sources_and_preferences():
    """
    Verify clean records:
    - Matched pair: source='both', USGS values preferred, USGS felt/cdi/mmi/sig kept,
      matched_usgs_id and matched_emsc_id filled.
    - Unmatched USGS: source='usgs_only'
    - Unmatched EMSC: source='emsc_only'
    - depth_is_default set properly.
    """
    usgs_event = {
        "event_id": "us100",
        "event_time": "2026-08-01T12:00:00Z",
        "latitude": 34.0,
        "longitude": -118.0,
        "depth_km": 10.0,  # default depth!
        "magnitude": 5.5,
        "mag_type": "mw",
        "place": "Los Angeles, CA",
        "felt": 500,
        "cdi": 6.2,
        "mmi": 6.0,
        "sig": 620,
    }
    emsc_event = {
        "event_id": "em200",
        "event_time": "2026-08-01T12:00:05Z",
        "latitude": 34.01,
        "longitude": -118.02,
        "depth_km": 12.0,
        "magnitude": 5.4,
        "mag_type": "mb",
        "place": "GREATER LOS ANGELES AREA",
        "felt": 50,
        "cdi": 4.0,
        "mmi": 4.5,
        "sig": 300,
        "auth": "NEIC",
    }

    match_item = {
        "usgs": usgs_event,
        "emsc": emsc_event,
        "time_diff_seconds": 5.0,
        "distance_km": 2.1,
        "magnitude_diff": 0.1,
        "depth_diff_km": 2.0,
    }

    unmatched_u = [{
        "event_id": "us_unmatched",
        "event_time": "2026-08-02T00:00:00Z",
        "latitude": 40.0,
        "longitude": -120.0,
        "depth_km": 5.0,
        "magnitude": 4.0,
        "mag_type": "ml",
        "place": "Nevada",
        "felt": None,
        "cdi": None,
        "mmi": None,
        "sig": 100,
    }]

    unmatched_e = [{
        "event_id": "em_unmatched",
        "event_time": "2026-08-03T00:00:00Z",
        "latitude": 36.0,
        "longitude": 28.0,
        "depth_km": 10.0,  # default depth!
        "magnitude": 4.2,
        "mag_type": "mb",
        "place": "DODECANESE ISLANDS, GREECE",
        "felt": 10,
        "cdi": 2.0,
        "mmi": 2.5,
        "sig": 120,
        "auth": "EMSC",
    }]

    records = build_clean_records([match_item], unmatched_u, unmatched_e)
    assert len(records) == 3

    # Check matched row
    r_both = next(r for r in records if r["source"] == "both")
    assert r_both["event_id"] == "us100"
    assert r_both["matched_usgs_id"] == "us100"
    assert r_both["matched_emsc_id"] == "em200"
    assert r_both["magnitude"] == 5.5  # USGS preferred
    assert r_both["place"] == "Los Angeles, CA"  # USGS preferred
    assert r_both["felt"] == 500  # USGS kept
    assert r_both["cdi"] == 6.2   # USGS kept
    assert r_both["mmi"] == 6.0   # USGS kept
    assert r_both["sig"] == 620   # USGS kept
    assert r_both["depth_is_default"] is True  # 10.0 km

    # Check unmatched USGS row
    r_usgs = next(r for r in records if r["source"] == "usgs_only")
    assert r_usgs["event_id"] == "us_unmatched"
    assert r_usgs["matched_usgs_id"] is None
    assert r_usgs["matched_emsc_id"] is None
    assert r_usgs["depth_is_default"] is False  # 5.0 km

    # Check unmatched EMSC row
    r_emsc = next(r for r in records if r["source"] == "emsc_only")
    assert r_emsc["event_id"] == "em_unmatched"
    assert r_emsc["matched_usgs_id"] is None
    assert r_emsc["matched_emsc_id"] is None
    assert r_emsc["depth_is_default"] is True  # 10.0 km


# =============================================================================
# 4. Quality Report Tests
# =============================================================================
def test_generate_quality_report_and_save():
    """
    Verify quality report metrics:
    - Counts of both, usgs_only, emsc_only
    - Annual match rates
    - Headline stats restricted to 2005 onward with incomplete coverage note
    - EMSC auth = 'NEIC' vs all other authors
    - Mean and spread (std) of diffs by decade
    - Share of depth_is_default per source and decade
    - Saved to CSV
    """
    matches = [
        {
            "usgs": {
                "event_id": "u_2004",
                "event_time": "2004-05-01T00:00:00Z",
                "magnitude": 5.0,
                "depth_km": 10.0,
            },
            "emsc": {
                "event_id": "e_2004",
                "event_time": "2004-05-01T00:00:05Z",
                "magnitude": 5.1,
                "depth_km": 10.0,
                "auth": "NEIC",
            },
            "time_diff_seconds": 5.0,
            "distance_km": 10.0,
            "magnitude_diff": 0.1,
            "depth_diff_km": 0.0,
        },
        {
            "usgs": {
                "event_id": "u_2015",
                "event_time": "2015-05-01T00:00:00Z",
                "magnitude": 6.0,
                "depth_km": 15.0,
            },
            "emsc": {
                "event_id": "e_2015",
                "event_time": "2015-05-01T00:00:02Z",
                "magnitude": 6.0,
                "depth_km": 18.0,
                "auth": "NEIC",
            },
            "time_diff_seconds": 2.0,
            "distance_km": 5.0,
            "magnitude_diff": 0.0,
            "depth_diff_km": 3.0,
        },
    ]

    unmatched_u = [{
        "event_id": "u_un_2015",
        "event_time": "2015-06-01T00:00:00Z",
        "magnitude": 5.0,
        "depth_km": 10.0,
    }]

    unmatched_e = [{
        "event_id": "e_un_2015",
        "event_time": "2015-07-01T00:00:00Z",
        "magnitude": 5.0,
        "depth_km": 25.0,
        "auth": "EMSC",
    }]

    raw_emsc = [
        {"event_id": "e_2004", "auth": "NEIC"},
        {"event_id": "e_2015", "auth": "NEIC"},
        {"event_id": "e_un_2015", "auth": "EMSC"},
    ]

    clean_records = build_clean_records(matches, unmatched_u, unmatched_e)

    report_rows = generate_quality_report(
        matches=matches,
        unmatched_usgs=unmatched_u,
        unmatched_emsc=unmatched_e,
        clean_records=clean_records,
        raw_emsc_events=raw_emsc,
    )

    # 1. Check sections presence
    sections = {r["section"] for r in report_rows}
    assert "1. Event Counts by Source" in sections
    assert "2. Match Rate Per Year" in sections
    assert "3. Headline Cross-Source Statistics (2005 Onward)" in sections
    assert "4. Match Rate by EMSC Author (NEIC vs Other)" in sections
    assert "5. Matched Differences by Decade" in sections
    assert "6. Default Depth Share by Source and Decade" in sections

    # 2. Section 1 counts
    sec1_rows = {r["category"]: r["value"] for r in report_rows if r["section"] == "1. Event Counts by Source"}
    assert sec1_rows["both"] == 2
    assert sec1_rows["usgs_only"] == 1
    assert sec1_rows["emsc_only"] == 1
    assert sec1_rows["total_clean"] == 4

    # 3. Section 3 headline note
    sec3_rows = [r for r in report_rows if r["section"] == "3. Headline Cross-Source Statistics (2005 Onward)"]
    assert len(sec3_rows) > 0
    for r in sec3_rows:
        assert "EMSC coverage is incomplete before 2005" in r["notes"]

    # In >=2005: 1 match (u_2015), 1 usgs_only (u_un_2015), 1 emsc_only (e_un_2015)
    matched_2005_row = next(r for r in sec3_rows if r["metric"] == "matched_pairs_count")
    assert matched_2005_row["value"] == 1

    # 4. Section 4 EMSC auth match rate
    sec4_rows = {r["category"]: r for r in report_rows if r["section"] == "4. Match Rate by EMSC Author (NEIC vs Other)"}
    # NEIC had 2 events (e_2004, e_2015), both matched -> 1.0 (100%)
    assert sec4_rows["NEIC"]["value"] == 1.0
    assert "sanity check" in sec4_rows["NEIC"]["notes"]
    # OTHER (EMSC) had 1 event (e_un_2015), unmatched -> 0.0
    assert sec4_rows["OTHER"]["value"] == 0.0

    # 5. Save report to CSV
    with tempfile.TemporaryDirectory() as tmp_dir:
        report_file = Path(tmp_dir) / "reports" / "quality_report.csv"
        saved_path = save_quality_report(report_rows, output_path=report_file)
        assert saved_path.exists()
        content = saved_path.read_text(encoding="utf-8")
        assert "1. Event Counts by Source" in content
        assert "EMSC coverage is incomplete before 2005" in content
        assert "NEIC" in content


# =============================================================================
# 5. CLI Arguments Tests
# =============================================================================
def test_parse_args():
    """Verify command-line argument parsing."""
    args = parse_args(["--start", "2026-01-01", "--end", "2026-02-01", "--dry-run", "--batch-size", "1000"])
    assert args.start == "2026-01-01"
    assert args.end == "2026-02-01"
    assert args.dry_run is True
    assert args.batch_size == 1000
    assert args.report_path == "reports/quality_report.csv"


# =============================================================================
# 6. End-to-End Pipeline Integration Test (In-Memory Database)
# =============================================================================
def test_run_reconcile_sqlite_in_memory():
    """
    Tests run_reconcile end-to-end with an in-memory SQLite database:
    - Tables usgs_events, emsc_events, events_clean created.
    - Sample rows inserted.
    - Reconciliation runs.
    - events_clean rows and quality report verified.
    """
    engine = create_engine("sqlite:///:memory:")

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE usgs_events (
                event_id VARCHAR(255) PRIMARY KEY,
                event_time TIMESTAMP NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                depth_km REAL,
                magnitude REAL,
                mag_type VARCHAR(50),
                place TEXT,
                felt INTEGER,
                cdi REAL,
                mmi REAL,
                sig INTEGER
            );
        """))
        conn.execute(text("""
            CREATE TABLE emsc_events (
                event_id VARCHAR(255) PRIMARY KEY,
                event_time TIMESTAMP NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                depth_km REAL,
                magnitude REAL,
                mag_type VARCHAR(50),
                place TEXT,
                felt INTEGER,
                cdi REAL,
                mmi REAL,
                sig INTEGER,
                auth VARCHAR(50)
            );
        """))
        conn.execute(text("""
            CREATE TABLE events_clean (
                event_id VARCHAR(255) PRIMARY KEY,
                source VARCHAR(50) NOT NULL,
                event_time TIMESTAMP NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                depth_km REAL,
                magnitude REAL,
                mag_type VARCHAR(50),
                place TEXT,
                felt INTEGER,
                cdi REAL,
                mmi REAL,
                sig INTEGER,
                matched_usgs_id VARCHAR(255),
                matched_emsc_id VARCHAR(255),
                ingested_at TIMESTAMP NOT NULL
            );
        """))

        # Insert USGS events
        conn.execute(text("""
            INSERT INTO usgs_events VALUES
            ('us_1', '2026-03-01 12:00:00', 38.0, 23.0, 10.0, 5.5, 'mw', 'Greece', 100, 4.0, 4.5, 400),
            ('us_2', '2026-03-02 14:00:00', 37.0, -122.0, 8.0, 4.2, 'ml', 'California', 50, 3.0, 3.5, 200);
        """))

        # Insert EMSC events:
        # em_1 matches us_1 (diff=10s, lat/lon match, mag diff=0.1)
        # em_compete also near us_1 but time diff=30s (should lose to em_1)
        # em_3 is unique to EMSC
        conn.execute(text("""
            INSERT INTO emsc_events VALUES
            ('em_1', '2026-03-01 12:00:10', 38.01, 23.01, 10.0, 5.6, 'mb', 'GREECE', 80, 3.8, 4.2, 380, 'NEIC'),
            ('em_compete', '2026-03-01 12:00:30', 38.02, 23.02, 10.0, 5.4, 'mb', 'GREECE', 20, 2.0, 2.0, 200, 'EMSC'),
            ('em_3', '2026-03-03 16:00:00', 35.0, 140.0, 30.0, 6.0, 'mw', 'JAPAN', 10, 1.0, 1.0, 100, 'GFZ');
        """))

    with tempfile.TemporaryDirectory() as tmp_dir:
        report_file = Path(tmp_dir) / "reports" / "quality_report.csv"

        res = run_reconcile(
            engine=engine,
            start="2026-03-01T00:00:00Z",
            end="2026-03-05T00:00:00Z",
            report_path=report_file,
            dry_run=False,
            batch_size=100,
        )

        assert res["status"] == "success"
        assert res["matches_count"] == 1
        assert res["usgs_only_count"] == 1
        assert res["emsc_only_count"] == 2  # em_compete and em_3
        assert res["total_clean_records"] == 4
        assert res["rows_upserted"] == 4
        assert report_file.exists()

        # Check database rows
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT event_id, source, depth_is_default, matched_usgs_id, matched_emsc_id FROM events_clean")).fetchall()
            assert len(rows) == 4
            row_dict = {r[0]: r for r in rows}

            # Matched row: us_1 + em_1
            assert row_dict["us_1"][1] == "both"
            assert row_dict["us_1"][2] == 1  # depth_is_default = True (10 km)
            assert row_dict["us_1"][3] == "us_1"
            assert row_dict["us_1"][4] == "em_1"

            # us_2 unmatched
            assert row_dict["us_2"][1] == "usgs_only"
            assert row_dict["us_2"][2] == 0  # 8.0 km -> False
            assert row_dict["us_2"][3] is None

            # em_compete unmatched
            assert row_dict["em_compete"][1] == "emsc_only"
            assert row_dict["em_compete"][2] == 1  # 10.0 km -> True

            # em_3 unmatched
            assert row_dict["em_3"][1] == "emsc_only"
            assert row_dict["em_3"][2] == 0  # 30.0 km -> False


def test_replace_clean_records_date_range():
    """Verify that replace_clean_records cleanly deletes records in range and inserts new ones."""
    from etl.reconcile import replace_clean_records
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE events_clean (
                event_id VARCHAR(255) PRIMARY KEY,
                source VARCHAR(50) NOT NULL,
                event_time TIMESTAMP NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                depth_km REAL,
                magnitude REAL,
                mag_type VARCHAR(50),
                place TEXT,
                felt INTEGER,
                cdi REAL,
                mmi REAL,
                sig INTEGER,
                matched_usgs_id VARCHAR(255),
                matched_emsc_id VARCHAR(255),
                depth_is_default BOOLEAN,
                ingested_at TIMESTAMP NOT NULL
            );
        """))
        # Insert initial records: one in January, one in February
        conn.execute(text("""
            INSERT INTO events_clean (event_id, source, event_time, latitude, longitude, ingested_at) VALUES
            ('old_jan', 'usgs_only', '2026-01-15 10:00:00', 10.0, 10.0, '2026-01-15 10:00:00'),
            ('keep_feb', 'usgs_only', '2026-02-15 10:00:00', 20.0, 20.0, '2026-02-15 10:00:00');
        """))

    new_jan_records = [{
        "event_id": "new_jan",
        "source": "both",
        "event_time": datetime.datetime(2026, 1, 20, 12, 0, 0, tzinfo=datetime.timezone.utc),
        "latitude": 12.0,
        "longitude": 12.0,
        "depth_km": 10.0,
        "magnitude": 5.0,
        "mag_type": "mw",
        "place": "Test",
        "felt": None,
        "cdi": None,
        "mmi": None,
        "sig": None,
        "matched_usgs_id": "new_jan",
        "matched_emsc_id": "em_jan",
        "depth_is_default": True,
        "ingested_at": datetime.datetime.now(datetime.timezone.utc),
    }]

    # Replace January range
    replace_clean_records(
        engine,
        new_jan_records,
        start_time=datetime.datetime(2026, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
        end_time=datetime.datetime(2026, 1, 31, 23, 59, 59, tzinfo=datetime.timezone.utc),
    )

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT event_id FROM events_clean ORDER BY event_id")).fetchall()
        event_ids = [r[0] for r in rows]
        # 'old_jan' should be deleted, 'new_jan' inserted, 'keep_feb' preserved
        assert event_ids == ["keep_feb", "new_jan"]

