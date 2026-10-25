import numpy as np
import pandas as pd
import pytest

from etl.threshold_study import build_candidates, greedy_match, haversine_km


def ev(times, lats=None, lons=None, mags=None):
    n = len(times)
    return pd.DataFrame({
        "t": times,
        "lat": lats if lats is not None else [0.0] * n,
        "lon": lons if lons is not None else [0.0] * n,
        "mag": mags if mags is not None else [5.0] * n,
        "mag_type": ["mb"] * n,
        "auth": ["EMSC"] * n,
    })


def test_haversine_one_degree_latitude():
    assert haversine_km(0.0, 0.0, 1.0, 0.0) == pytest.approx(111.2, abs=0.5)
    assert haversine_km(10.0, 20.0, 10.0, 20.0) == pytest.approx(0.0, abs=1e-9)


def test_candidates_are_found_inside_the_window_only():
    u = ev([100.0, 1000.0])
    e = ev([90.0, 150.0, 5000.0])
    c = build_candidates(u, e, window=60)
    assert c["ui"].tolist() == [0, 0]
    assert c["ei"].tolist() == [0, 1]
    assert c["dt"].tolist() == [-10.0, 50.0]


def test_window_edge_is_inclusive():
    c = build_candidates(ev([100.0]), ev([160.0]), window=60)
    assert len(c) == 1


def test_no_candidates_gives_empty_frame_with_columns():
    c = build_candidates(ev([100.0]), ev([9999.0]), window=60)
    assert len(c) == 0
    assert "adt" in c.columns
    mu, me = greedy_match(c, 60, 100, 0.5)
    assert len(mu) == 0 and len(me) == 0


def test_two_emsc_events_compete_for_one_usgs_event():
    c = build_candidates(ev([100.0]), ev([95.0, 101.0]), window=60)
    mu, me = greedy_match(c, 60, 100, 0.5)
    assert mu.tolist() == [0]
    assert me.tolist() == [1]  # the one 1 s away wins, not the one 5 s away


def test_matching_is_one_to_one():
    c = build_candidates(ev([100.0, 102.0]), ev([101.0]), window=60)
    mu, me = greedy_match(c, 60, 100, 0.5)
    assert len(mu) == 1 and len(me) == 1


def test_distance_gate():
    u = ev([100.0], lats=[0.0])
    e = ev([100.0], lats=[2.0])  # about 222 km away
    c = build_candidates(u, e, window=60)
    assert len(greedy_match(c, 60, 100, None)[0]) == 0
    assert len(greedy_match(c, 60, 250, None)[0]) == 1


def test_magnitude_gate():
    u = ev([100.0], mags=[5.0])
    e = ev([100.0], mags=[6.0])
    c = build_candidates(u, e, window=60)
    assert len(greedy_match(c, 60, 100, 0.5)[0]) == 0
    assert len(greedy_match(c, 60, 100, 1.0)[0]) == 1
    assert len(greedy_match(c, 60, 100, None)[0]) == 1