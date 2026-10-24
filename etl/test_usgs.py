"""
Unit tests for the USGS earthquake Pydantic event model (USGSEvent).

Tests model validation for valid records and boundary/null validation failures:
- Latitude exceeding 90 (e.g., 95)
- Depth exceeding 800 km (e.g., 2000)
- Null magnitude

Runs completely in-memory without database connections or modifying .env.
"""

import datetime
from pathlib import Path
import sys

import pytest
from pydantic import ValidationError

# Ensure project root is available on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.usgs import USGSEvent


# Base valid dictionary for reuse across tests
VALID_EVENT_DATA = {
    "id": "us7000test",
    "event_time": "2023-01-01T12:00:00Z",
    "latitude": 34.0522,
    "longitude": -118.2437,
    "depth_km": 10.5,
    "magnitude": 5.2,
    "mag_type": "mw",
    "place": "10km NE of Los Angeles, CA",
    "felt": 125,
    "cdi": 4.1,
    "mmi": 5.0,
    "alert": "green",
    "status": "reviewed",
    "tsunami": 0,
    "sig": 420,
    "net": "us",
    "code": "7000test",
    "nst": 40,
    "dmin": 0.08,
    "rms": 0.35,
    "gap": 25.0,
}


def test_valid_record():
    """
    Assert that a valid record passes validation and correctly populates model fields.
    """
    event = USGSEvent(**VALID_EVENT_DATA)

    assert event.id == "us7000test"
    assert event.latitude == 34.0522
    assert event.longitude == -118.2437
    assert event.depth_km == 10.5
    assert event.magnitude == 5.2
    assert event.mag_type == "mw"
    assert event.place == "10km NE of Los Angeles, CA"
    assert isinstance(event.event_time, datetime.datetime)
    assert event.event_time.tzinfo == datetime.timezone.utc


def test_invalid_record_latitude_95():
    """
    Assert that a record with latitude 95 raises a ValidationError (allowed range: -90..90).
    """
    invalid_data = dict(VALID_EVENT_DATA)
    invalid_data["latitude"] = 95.0

    with pytest.raises(ValidationError) as exc_info:
        USGSEvent(**invalid_data)

    errors = exc_info.value.errors()
    assert any("latitude" in str(err["loc"]) for err in errors)


def test_invalid_record_depth_2000():
    """
    Assert that a record with depth_km 2000 raises a ValidationError (allowed range: -5..800 km).
    """
    invalid_data = dict(VALID_EVENT_DATA)
    invalid_data["depth_km"] = 2000.0

    with pytest.raises(ValidationError) as exc_info:
        USGSEvent(**invalid_data)

    errors = exc_info.value.errors()
    assert any("depth_km" in str(err["loc"]) for err in errors)


def test_invalid_record_magnitude_null():
    """
    Assert that a record with magnitude null (None) raises a ValidationError.
    """
    invalid_data = dict(VALID_EVENT_DATA)
    invalid_data["magnitude"] = None

    with pytest.raises(ValidationError) as exc_info:
        USGSEvent(**invalid_data)

    errors = exc_info.value.errors()
    assert any("magnitude" in str(err["loc"]) for err in errors)


@pytest.mark.parametrize(
    "field_override,invalid_value,expected_field_in_error",
    [
        ("latitude", 95.0, "latitude"),
        ("depth_km", 2000.0, "depth_km"),
        ("magnitude", None, "magnitude"),
    ],
)
def test_invalid_records_parameterized(field_override, invalid_value, expected_field_in_error):
    """
    Parameterized test confirming all three invalid records raise ValidationError.
    """
    record = dict(VALID_EVENT_DATA)
    record[field_override] = invalid_value

    with pytest.raises(ValidationError) as exc_info:
        USGSEvent(**record)

    errors = exc_info.value.errors()
    assert any(expected_field_in_error in str(err["loc"]) for err in errors)


def test_valid_record_from_feature():
    """
    Assert that parsing a valid USGS GeoJSON feature passes validation.
    """
    feature = {
        "id": "us7000test",
        "properties": {
            "time": 1672574400000,
            "mag": 5.2,
            "magType": "mw",
            "place": "10km NE of Los Angeles, CA",
            "felt": 125,
            "cdi": 4.1,
            "mmi": 5.0,
            "alert": "green",
            "status": "reviewed",
            "tsunami": 0,
            "sig": 420,
            "net": "us",
            "code": "7000test",
            "nst": 40,
            "dmin": 0.08,
            "rms": 0.35,
            "gap": 25.0,
        },
        "geometry": {
            "type": "Point",
            "coordinates": [-118.2437, 34.0522, 10.5],
        },
    }

    event = USGSEvent.from_feature(feature)
    assert event.id == "us7000test"
    assert event.latitude == 34.0522
    assert event.longitude == -118.2437
    assert event.depth_km == 10.5
    assert event.magnitude == 5.2


def test_invalid_feature_latitude_95():
    """
    Assert that a GeoJSON feature with latitude 95 raises a ValidationError.
    """
    feature = {
        "id": "us7000lat95",
        "properties": {"time": 1672574400000, "mag": 5.2},
        "geometry": {"type": "Point", "coordinates": [-118.2437, 95.0, 10.5]},
    }
    with pytest.raises(ValidationError):
        USGSEvent.from_feature(feature)


def test_invalid_feature_depth_2000():
    """
    Assert that a GeoJSON feature with depth 2000 km raises a ValidationError.
    """
    feature = {
        "id": "us7000depth2000",
        "properties": {"time": 1672574400000, "mag": 5.2},
        "geometry": {"type": "Point", "coordinates": [-118.2437, 34.0522, 2000.0]},
    }
    with pytest.raises(ValidationError):
        USGSEvent.from_feature(feature)


def test_invalid_feature_magnitude_null():
    """
    Assert that a GeoJSON feature with magnitude null raises a ValidationError.
    """
    feature = {
        "id": "us7000magnull",
        "properties": {"time": 1672574400000, "mag": None},
        "geometry": {"type": "Point", "coordinates": [-118.2437, 34.0522, 10.5]},
    }
    with pytest.raises(ValidationError):
        USGSEvent.from_feature(feature)
