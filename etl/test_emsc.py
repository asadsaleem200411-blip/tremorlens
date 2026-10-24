"""
Unit tests for the EMSC earthquake Pydantic event model (EMSCEvent) and utilities.

Tests model validation for valid records and boundary/null validation failures:
- Latitude exceeding 90 (e.g., 95)
- Depth exceeding 800 km (e.g., 2000)
- Null magnitude
- Longitude exceeding 180 (e.g., 200)
- Depth below -5 km (e.g., -10)
- Empty or whitespace id

Also tests parsing real EMSC SeismicPortal GeoJSON features, alias normalization,
and monthly window generation.

Runs completely in-memory without database connections or modifying .env.
"""

import datetime
from pathlib import Path
import sys
from unittest.mock import MagicMock, patch

import pytest
import requests
from pydantic import ValidationError

# Ensure project root is available on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.emsc import EMSCEvent, fetch_emsc_events, generate_monthly_windows


# Base valid dictionary for reuse across tests
VALID_EVENT_DATA = {
    "id": "20261024_0000091",
    "event_time": "2026-10-24T04:12:05.337Z",
    "latitude": -13.9017,
    "longitude": 165.5967,
    "depth_km": 10.0,
    "magnitude": 5.8,
    "mag_type": "mb",
    "place": "VANUATU",
    "felt": 12,
    "cdi": 3.5,
    "mmi": 4.0,
    "sig": 350,
    "auth": "NEIC",
    "source_catalog": "EMSC-RTS",
    "last_update": "2026-10-24T04:25:11.002167Z",
}

# Real-format GeoJSON feature matching EMSC SeismicPortal output
VALID_FEATURE_DATA = {
    "type": "Feature",
    "geometry": {
        "type": "Point",
        "coordinates": [165.5967, -13.9017, -10.0],
    },
    "id": "20261024_0000091",
    "properties": {
        "source_id": "2079739",
        "source_catalog": "EMSC-RTS",
        "lastupdate": "2026-10-24T04:25:11.002167Z",
        "time": "2026-10-24T04:12:05.337Z",
        "flynn_region": "VANUATU",
        "lat": -13.9017,
        "lon": 165.5967,
        "depth": 10.0,
        "evtype": "ke",
        "auth": "NEIC",
        "mag": 5.8,
        "magtype": "mb",
        "unid": "20261024_0000091",
    },
}


def test_valid_record():
    """
    Assert that a valid record passes validation and correctly populates model fields.
    """
    event = EMSCEvent(**VALID_EVENT_DATA)

    assert event.id == "20261024_0000091"
    assert event.latitude == -13.9017
    assert event.longitude == 165.5967
    assert event.depth_km == 10.0
    assert event.magnitude == 5.8
    assert event.mag_type == "mb"
    assert event.place == "VANUATU"
    assert event.felt == 12
    assert event.auth == "NEIC"
    assert event.source_catalog == "EMSC-RTS"
    assert isinstance(event.event_time, datetime.datetime)
    assert event.event_time.tzinfo == datetime.timezone.utc
    assert isinstance(event.last_update, datetime.datetime)
    assert event.last_update.tzinfo == datetime.timezone.utc


def test_invalid_record_latitude_95():
    """
    Assert that a record with latitude 95 raises a ValidationError (allowed range: -90..90).
    """
    invalid_data = dict(VALID_EVENT_DATA)
    invalid_data["latitude"] = 95.0

    with pytest.raises(ValidationError) as exc_info:
        EMSCEvent(**invalid_data)

    errors = exc_info.value.errors()
    assert any("latitude" in str(err["loc"]) for err in errors)


def test_invalid_record_depth_2000():
    """
    Assert that a record with depth_km 2000 raises a ValidationError (allowed range: -5..800 km).
    """
    invalid_data = dict(VALID_EVENT_DATA)
    invalid_data["depth_km"] = 2000.0

    with pytest.raises(ValidationError) as exc_info:
        EMSCEvent(**invalid_data)

    errors = exc_info.value.errors()
    assert any("depth_km" in str(err["loc"]) for err in errors)


def test_invalid_record_magnitude_null():
    """
    Assert that a record with magnitude null (None) raises a ValidationError.
    """
    invalid_data = dict(VALID_EVENT_DATA)
    invalid_data["magnitude"] = None

    with pytest.raises(ValidationError) as exc_info:
        EMSCEvent(**invalid_data)

    errors = exc_info.value.errors()
    assert any("magnitude" in str(err["loc"]) for err in errors)


def test_invalid_record_empty_id():
    """
    Assert that a record with an empty string id raises a ValidationError.
    """
    invalid_data = dict(VALID_EVENT_DATA)
    invalid_data["id"] = "   "

    with pytest.raises(ValidationError) as exc_info:
        EMSCEvent(**invalid_data)

    errors = exc_info.value.errors()
    assert any("id" in str(err["loc"]) for err in errors)


def test_invalid_record_longitude_out_of_bounds():
    """
    Assert that a record with longitude outside -180..180 raises a ValidationError.
    """
    invalid_data = dict(VALID_EVENT_DATA)
    invalid_data["longitude"] = 195.0

    with pytest.raises(ValidationError) as exc_info:
        EMSCEvent(**invalid_data)

    errors = exc_info.value.errors()
    assert any("longitude" in str(err["loc"]) for err in errors)


def test_invalid_record_depth_negative_out_of_bounds():
    """
    Assert that a record with depth_km < -5 km raises a ValidationError.
    """
    invalid_data = dict(VALID_EVENT_DATA)
    invalid_data["depth_km"] = -10.0

    with pytest.raises(ValidationError) as exc_info:
        EMSCEvent(**invalid_data)

    errors = exc_info.value.errors()
    assert any("depth_km" in str(err["loc"]) for err in errors)


@pytest.mark.parametrize(
    "field_override,invalid_value,expected_field_in_error",
    [
        ("latitude", 95.0, "latitude"),
        ("depth_km", 2000.0, "depth_km"),
        ("magnitude", None, "magnitude"),
        ("longitude", -250.0, "longitude"),
        ("id", "", "id"),
    ],
)
def test_invalid_records_parameterized(field_override, invalid_value, expected_field_in_error):
    """
    Parameterized test confirming invalid records raise ValidationError with proper field locations.
    """
    record = dict(VALID_EVENT_DATA)
    record[field_override] = invalid_value

    with pytest.raises(ValidationError) as exc_info:
        EMSCEvent(**record)

    errors = exc_info.value.errors()
    assert any(expected_field_in_error in str(err["loc"]) for err in errors)


def test_valid_record_from_feature():
    """
    Assert that parsing a valid EMSC GeoJSON feature passes validation.
    """
    event = EMSCEvent.from_feature(VALID_FEATURE_DATA)

    assert event.id == "20261024_0000091"
    assert event.latitude == -13.9017
    assert event.longitude == 165.5967
    assert event.depth_km == 10.0
    assert event.magnitude == 5.8
    assert event.mag_type == "mb"
    assert event.place == "VANUATU"
    assert event.auth == "NEIC"
    assert event.source_catalog == "EMSC-RTS"
    assert isinstance(event.event_time, datetime.datetime)
    assert event.raw_payload == VALID_FEATURE_DATA


def test_invalid_feature_latitude_95():
    """
    Assert that a GeoJSON feature with latitude 95 raises a ValidationError.
    """
    feature = {
        "id": "2026_lat95",
        "properties": {"time": "2026-10-24T04:12:05Z", "mag": 5.0, "lat": 95.0, "lon": 10.0},
        "geometry": {"type": "Point", "coordinates": [10.0, 95.0, -10.0]},
    }
    with pytest.raises(ValidationError):
        EMSCEvent.from_feature(feature)


def test_invalid_feature_depth_2000():
    """
    Assert that a GeoJSON feature with depth 2000 km raises a ValidationError.
    """
    feature = {
        "id": "2026_depth2000",
        "properties": {"time": "2026-10-24T04:12:05Z", "mag": 5.0, "depth": 2000.0, "lat": 10.0, "lon": 10.0},
        "geometry": {"type": "Point", "coordinates": [10.0, 10.0, -2000.0]},
    }
    with pytest.raises(ValidationError):
        EMSCEvent.from_feature(feature)


def test_invalid_feature_magnitude_null():
    """
    Assert that a GeoJSON feature with magnitude null raises a ValidationError.
    """
    feature = {
        "id": "2026_magnull",
        "properties": {"time": "2026-10-24T04:12:05Z", "mag": None, "lat": 10.0, "lon": 10.0},
        "geometry": {"type": "Point", "coordinates": [10.0, 10.0, -10.0]},
    }
    with pytest.raises(ValidationError):
        EMSCEvent.from_feature(feature)


def test_raw_property_normalization_aliases():
    """
    Assert that passing raw EMSC property field names directly populates EMSCEvent.
    """
    raw_data = {
        "unid": "20261024_0000091",
        "time": "2026-10-24T04:12:05.337Z",
        "lat": -13.9017,
        "lon": 165.5967,
        "depth": 10.0,
        "mag": 5.8,
        "magtype": "mb",
        "flynn_region": "VANUATU",
        "lastupdate": "2026-10-24T04:25:11.002167Z",
    }
    event = EMSCEvent(**raw_data)
    assert event.id == "20261024_0000091"
    assert event.latitude == -13.9017
    assert event.longitude == 165.5967
    assert event.depth_km == 10.0
    assert event.magnitude == 5.8
    assert event.mag_type == "mb"
    assert event.place == "VANUATU"


def test_generate_monthly_windows():
    """
    Assert that generate_monthly_windows splits date spans into monthly intervals.
    """
    start = datetime.date(2023, 1, 1)
    end = datetime.date(2023, 3, 15)

    windows = list(generate_monthly_windows(start, end))
    assert len(windows) == 3

    w1_start, w1_end = windows[0]
    assert w1_start == datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    assert w1_end == datetime.datetime(2023, 2, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)

    w3_start, w3_end = windows[2]
    assert w3_start == datetime.datetime(2023, 3, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    assert w3_end == datetime.datetime(2023, 3, 15, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc)


def test_fetch_emsc_events_204_no_content():
    """
    Assert that HTTP 204 returns an empty FeatureCollection structure.
    """
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 204
    mock_session.get.return_value = mock_response

    result = fetch_emsc_events("https://fake.url", params={}, session=mock_session)
    assert result == {"type": "FeatureCollection", "features": []}


def test_fetch_emsc_events_retry_on_500():
    """
    Assert that transient HTTP 500 triggers retry and succeeds on subsequent attempt.
    """
    mock_session = MagicMock()
    resp_500 = MagicMock()
    resp_500.status_code = 500
    resp_500.raise_for_status.side_effect = requests.exceptions.HTTPError("Server error 500")

    resp_200 = MagicMock()
    resp_200.status_code = 200
    resp_200.json.return_value = {"type": "FeatureCollection", "features": [{"id": "test_1"}]}

    mock_session.get.side_effect = [resp_500, resp_200]

    with patch("time.sleep", return_value=None):
        result = fetch_emsc_events("https://fake.url", params={}, initial_delay=0.01, session=mock_session)

    assert len(result["features"]) == 1
    assert result["features"][0]["id"] == "test_1"
    assert mock_session.get.call_count == 2
