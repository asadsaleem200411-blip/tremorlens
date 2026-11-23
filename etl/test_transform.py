"""
Tests for the pandas transform steps of the USGS and EMSC ETL:

- profile_features(): runs BEFORE validation, counts missing ids and bad magnitudes,
  changes nothing.
- dedupe_events():    runs AFTER Pydantic validation, removes duplicate event ids
  (last valid copy wins, order kept).

Also checks that the full pipeline gives exactly the same database rows as
validating and upserting the raw features directly, so the pandas steps do not
change the results.

Runs in memory: no network, no database, no .env needed.
"""

import copy
import datetime
from pathlib import Path
import sys
from unittest.mock import patch

import pytest
from pydantic import ValidationError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl import emsc, usgs


def usgs_feature(event_id, mag=5.0, lat=10.0, lon=20.0, depth=15.0):
    return {
        "id": event_id,
        "properties": {"time": 1672574400000, "mag": mag, "place": "somewhere"},
        "geometry": {"type": "Point", "coordinates": [lon, lat, depth]},
    }


def emsc_feature(event_id, mag=5.0, lat=10.0, lon=20.0, depth=15.0):
    return {
        "id": event_id,
        "properties": {
            "time": "2026-10-24T04:12:05Z", "mag": mag, "lat": lat, "lon": lon,
            "depth": depth, "auth": "EMSC",
        },
        "geometry": {"type": "Point", "coordinates": [lon, lat, -depth]},
    }


CASES = [
    pytest.param(usgs, usgs.USGSEvent, usgs_feature, "fetch_usgs_events", id="usgs"),
    pytest.param(emsc, emsc.EMSCEvent, emsc_feature, "fetch_emsc_events", id="emsc"),
]


# ---------------------------------------------------------------------------
# profile_features (before validation)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("mod,model,make,fetch", CASES)
def test_profile_empty_input(mod, model, make, fetch):
    assert mod.profile_features([]) == {"input_rows": 0, "missing_id": 0, "bad_magnitude": 0}


@pytest.mark.parametrize("mod,model,make,fetch", CASES)
def test_profile_clean_input(mod, model, make, fetch):
    stats = mod.profile_features([make("a"), make("b"), make("c")])
    assert stats == {"input_rows": 3, "missing_id": 0, "bad_magnitude": 0}


@pytest.mark.parametrize("mod,model,make,fetch", CASES)
def test_profile_counts_missing_ids(mod, model, make, fetch):
    stats = mod.profile_features([make(None), make(""), make("   "), make("ok")])
    assert stats["missing_id"] == 3


@pytest.mark.parametrize("mod,model,make,fetch", CASES)
def test_profile_counts_bad_magnitudes(mod, model, make, fetch):
    stats = mod.profile_features([make("a", mag=None), make("b", mag="n/a"), make("c", mag=4.7)])
    assert stats["bad_magnitude"] == 2


@pytest.mark.parametrize("mod,model,make,fetch", CASES)
def test_profile_does_not_modify_input(mod, model, make, fetch):
    feats = [make("  spaced  "), make(None, mag=None)]
    before = copy.deepcopy(feats)
    mod.profile_features(feats)
    assert feats == before


def test_emsc_profile_uses_unid_when_feature_id_missing():
    feats = [{"properties": {"unid": "U1", "mag": 5.0}}, {"properties": {"mag": 5.0}}]
    assert emsc.profile_features(feats)["missing_id"] == 1


# ---------------------------------------------------------------------------
# dedupe_events (after validation)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("mod,model,make,fetch", CASES)
def test_dedupe_empty(mod, model, make, fetch):
    assert mod.dedupe_events([]) == ([], 0)


@pytest.mark.parametrize("mod,model,make,fetch", CASES)
def test_dedupe_no_duplicates_keeps_everything(mod, model, make, fetch):
    events = [model.from_feature(make(i)) for i in ("a", "b", "c")]
    kept, removed = mod.dedupe_events(events)
    assert kept == events and removed == 0


@pytest.mark.parametrize("mod,model,make,fetch", CASES)
def test_dedupe_last_copy_wins_and_order_is_kept(mod, model, make, fetch):
    first = model.from_feature(make("dup", mag=5.0))
    other = model.from_feature(make("other"))
    last = model.from_feature(make("dup", mag=6.5))
    kept, removed = mod.dedupe_events([first, other, last])
    assert kept == [other, last]
    assert removed == 1


# ---------------------------------------------------------------------------
# Equivalence: the pandas steps must not change what ends up in the database
# ---------------------------------------------------------------------------
def direct_result(model, features):
    """What the database would hold if every raw feature were validated and upserted in order."""
    db, rejected = {}, 0
    for feat in features:
        try:
            ev = model.from_feature(feat)
            db[ev.id] = ev.model_dump(exclude={"raw_payload"})  # upsert: last write wins
        except (ValidationError, Exception):
            rejected += 1
    return db, rejected


def pipeline_result(mod, fetch_name, features):
    cap = {"db": {}, "rejected": 0}

    def fake_upsert(engine, events):
        for e in events:
            cap["db"][e.id] = e.model_dump(exclude={"raw_payload"})
        return len(events)

    def fake_rejected(engine, items):
        cap["rejected"] += len(items)
        return len(items)

    payload = {"type": "FeatureCollection", "features": copy.deepcopy(features)}
    with patch.object(mod, fetch_name, return_value=payload), \
         patch.object(mod, "upsert_valid_events", side_effect=fake_upsert), \
         patch.object(mod, "insert_rejected_records", side_effect=fake_rejected), \
         patch.object(mod, "record_ingest_run"):
        result = mod.run_pipeline(datetime.date(2026, 1, 1), datetime.date(2026, 1, 15), engine=object())
    return cap, result


@pytest.mark.parametrize("mod,model,make,fetch", CASES)
def test_pipeline_gives_same_database_rows_as_direct_validation(mod, model, make, fetch):
    features = [
        make("ok1"),
        make("ok2", mag=7.1),
        make("dup", mag=5.0),
        make("dup", mag=5.9),             # duplicate id, last valid copy wins
        make("bad_lat", lat=95.0),        # Pydantic must reject
        make("bad_depth", depth=2000.0),  # Pydantic must reject
        make("null_mag", mag=None),       # Pydantic must reject
        make(None),                       # Pydantic must reject (no id)
    ]
    expected_db, expected_rejected = direct_result(model, features)
    cap, result = pipeline_result(mod, fetch, features)

    assert cap["db"] == expected_db
    assert cap["rejected"] == expected_rejected == 4
    assert result["rows_fetched"] == len(features)
    assert result["rows_rejected"] == 4
    assert result["rows_loaded"] == 3  # ok1, ok2, dup (the two copies of dup count once)


@pytest.mark.parametrize("mod,model,make,fetch", CASES)
def test_valid_first_copy_is_not_lost_when_later_copy_is_invalid(mod, model, make, fetch):
    """Regression test: dedupe must happen after validation, not before."""
    features = [make("same", mag=5.5), make("same", mag=None)]  # 2nd copy is invalid
    expected_db, _ = direct_result(model, features)
    cap, result = pipeline_result(mod, fetch, features)

    assert "same" in cap["db"]
    assert cap["db"]["same"]["magnitude"] == 5.5
    assert cap["db"] == expected_db
    assert result["rows_loaded"] == 1 and result["rows_rejected"] == 1
