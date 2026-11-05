-- Soft warnings: values that are possible but doubtful. Rebuilt from the other tables every time.
DROP TABLE IF EXISTS quality_issues;

CREATE TABLE quality_issues (
    issue_id    bigserial PRIMARY KEY,
    source      text NOT NULL,
    event_id    text NOT NULL,
    event_time  timestamptz,
    issue       text NOT NULL,
    severity    text NOT NULL DEFAULT 'warning',
    detail      text
);

-- 1. USGS depth is one of the four default-like values
INSERT INTO quality_issues (source, event_id, event_time, issue, detail)
SELECT 'usgs', event_id, event_time, 'default_like_depth', 'depth_km = ' || depth_km::text
FROM usgs_events
WHERE depth_km IN (10, 30, 33, 35);

-- 2. EMSC depth is exactly 10 km
INSERT INTO quality_issues (source, event_id, event_time, issue, detail)
SELECT 'emsc', event_id, event_time, 'default_like_depth', 'depth_km = ' || depth_km::text
FROM emsc_events
WHERE depth_km = 10;

-- 3. Magnitude below the 4.5 limit used for the download
INSERT INTO quality_issues (source, event_id, event_time, issue, detail)
SELECT 'usgs', event_id, event_time, 'magnitude_below_4_5', 'magnitude = ' || magnitude::text
FROM usgs_events
WHERE magnitude < 4.5;

INSERT INTO quality_issues (source, event_id, event_time, issue, detail)
SELECT 'emsc', event_id, event_time, 'magnitude_below_4_5', 'magnitude = ' || magnitude::text
FROM emsc_events
WHERE magnitude < 4.5;

-- 4. Large USGS event (M6 or more, 2005 onward) that EMSC does not have
INSERT INTO quality_issues (source, event_id, event_time, issue, detail)
SELECT 'usgs', event_id, event_time, 'large_event_without_emsc_partner',
       'M' || round(magnitude::numeric, 1)::text || ', ' || coalesce(place, '')
FROM events_clean
WHERE source = 'usgs_only' AND magnitude >= 6.0 AND event_time >= '2005-01-01';

-- 5. EMSC event authored by NEIC (USGS network) with no USGS partner
INSERT INTO quality_issues (source, event_id, event_time, issue, detail)
SELECT 'emsc', c.event_id, c.event_time, 'neic_event_without_usgs_partner',
       'M' || round(c.magnitude::numeric, 1)::text || ', EMSC copy of a USGS-network event'
FROM events_clean c
JOIN emsc_events e ON e.event_id = c.event_id
WHERE c.source = 'emsc_only' AND e.auth = 'NEIC';

-- 6. Matched pair whose depths differ by more than 100 km
INSERT INTO quality_issues (source, event_id, event_time, issue, detail)
SELECT 'both', c.event_id, c.event_time, 'depth_disagreement_over_100km',
       'USGS ' || u.depth_km::text || ' km vs EMSC ' || e.depth_km::text || ' km'
FROM events_clean c
JOIN usgs_events u ON u.event_id = c.matched_usgs_id
JOIN emsc_events e ON e.event_id = c.matched_emsc_id
WHERE c.source = 'both' AND abs(u.depth_km - e.depth_km) > 100;

-- 7. EMSC depth deeper than 700 km (the deepest USGS depth is 700.9 km; 750 km looks like a cap of the location method)
INSERT INTO quality_issues (source, event_id, event_time, issue, detail)
SELECT 'emsc', event_id, event_time, 'depth_over_700km', 'depth_km = ' || depth_km::text
FROM emsc_events
WHERE depth_km > 700;

CREATE INDEX quality_issues_issue_idx ON quality_issues (issue);

SELECT issue, source, count(*) AS n FROM quality_issues GROUP BY 1, 2 ORDER BY 3 DESC;