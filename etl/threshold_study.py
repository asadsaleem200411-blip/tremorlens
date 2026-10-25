"""Read-only study of USGS/EMSC matching thresholds.

Run:  python -m etl.threshold_study [--since 2005-01-01]
It only runs SELECT queries and writes files into reports/.
"""
import argparse
import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger("etl.threshold_study")

REPORTS = Path("reports")
CAND_WINDOW_S = 120.0
TIME_GATES = [30, 60, 120]
DIST_GATES = [50, 100, 150, 250]
MAG_GATES = [0.5, 1.0, 1.5, None]
DEFAULT_GATES = (60, 100, 0.5)
EARTH_R_KM = 6371.0088

CAND_COLUMNS = ["ui", "ei", "dt", "adt", "dist", "dmag", "admag",
                "usgs_mag", "usgs_type", "emsc_type", "emsc_auth"]


def haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dphi = p2 - p1
    dlam = np.radians(lon2) - np.radians(lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dlam / 2) ** 2
    return 2 * EARTH_R_KM * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


def load_events(engine, since):
    from sqlalchemy import text
    q_u = text("SELECT event_id, event_time, latitude AS lat, longitude AS lon, "
               "magnitude AS mag, mag_type FROM usgs_events "
               "WHERE event_time >= CAST(:since AS timestamptz)")
    q_e = text("SELECT event_id, event_time, latitude AS lat, longitude AS lon, "
               "magnitude AS mag, mag_type, auth FROM emsc_events "
               "WHERE event_time >= CAST(:since AS timestamptz)")
    u = pd.read_sql(q_u, engine, params={"since": since})
    e = pd.read_sql(q_e, engine, params={"since": since})
    epoch = pd.Timestamp("1970-01-01", tz="UTC")
    for df in (u, e):
        ts = pd.to_datetime(df["event_time"], utc=True)
        df["t"] = (ts - epoch).dt.total_seconds()
        df["year"] = ts.dt.year
        df["mag_type"] = df["mag_type"].fillna("").astype(str).str.lower()
    e["auth"] = e["auth"].fillna("").astype(str)
    u = u.sort_values("t").reset_index(drop=True)
    e = e.sort_values("t").reset_index(drop=True)
    return u, e


def build_candidates(u, e, window=CAND_WINDOW_S):
    """All (USGS, EMSC) pairs within +-window seconds.

    Two-pointer idea: both tables are sorted by time, so for every USGS event
    the EMSC candidates form one contiguous slice [lo, hi) found by binary
    search. No distance or magnitude limit is applied here.
    """
    tu = u["t"].to_numpy()
    te = e["t"].to_numpy()
    lo = np.searchsorted(te, tu - window, side="left")
    hi = np.searchsorted(te, tu + window, side="right")
    counts = hi - lo
    total = int(counts.sum())
    if total == 0:
        return pd.DataFrame({c: [] for c in CAND_COLUMNS})
    ui = np.repeat(np.arange(len(u)), counts)
    first = np.cumsum(counts) - counts
    ei = np.repeat(lo, counts) + (np.arange(total) - np.repeat(first, counts))
    dt = te[ei] - tu[ui]
    dist = haversine_km(u["lat"].to_numpy()[ui], u["lon"].to_numpy()[ui],
                        e["lat"].to_numpy()[ei], e["lon"].to_numpy()[ei])
    dmag = e["mag"].to_numpy()[ei] - u["mag"].to_numpy()[ui]
    return pd.DataFrame({
        "ui": ui, "ei": ei, "dt": dt, "adt": np.abs(dt), "dist": dist,
        "dmag": dmag, "admag": np.abs(dmag),
        "usgs_mag": u["mag"].to_numpy()[ui],
        "usgs_type": u["mag_type"].to_numpy()[ui],
        "emsc_type": e["mag_type"].to_numpy()[ei],
        "emsc_auth": e["auth"].to_numpy()[ei],
    })


def greedy_match(cand, tmax, dmax, mmax=None):
    """One-to-one matching: closest in time first, then in distance."""
    mask = (cand["adt"] <= tmax) & (cand["dist"] <= dmax)
    if mmax is not None:
        mask = mask & (cand["admag"] <= mmax)
    sub = cand.loc[mask].sort_values(["adt", "dist"], kind="stable")
    used_u, used_e, mu, me = set(), set(), [], []
    for a, b in zip(sub["ui"].tolist(), sub["ei"].tolist()):
        if a in used_u or b in used_e:
            continue
        used_u.add(a)
        used_e.add(b)
        mu.append(a)
        me.append(b)
    return np.array(mu, dtype=int), np.array(me, dtype=int)


def run_gate_grid(u, e, cand):
    neic = (e["auth"] == "NEIC").to_numpy()
    n_neic = int(neic.sum())
    rows = []
    for tg in TIME_GATES:
        for dg in DIST_GATES:
            for mg in MAG_GATES:
                mu, me = greedy_match(cand, tg, dg, mg)
                rows.append({
                    "time_gate_s": tg, "dist_gate_km": dg,
                    "mag_gate": "none" if mg is None else mg,
                    "matched": len(mu),
                    "usgs_match_pct": round(100 * len(mu) / max(len(u), 1), 2),
                    "emsc_match_pct": round(100 * len(me) / max(len(e), 1), 2),
                    "neic_emsc_match_pct": round(100 * int(neic[me].sum()) / max(n_neic, 1), 2),
                })
        log.info("time gate %s s done", tg)
    return pd.DataFrame(rows)


def per_year_rates(u, e, mu, me):
    fu = np.zeros(len(u), dtype=bool)
    fu[mu] = True
    fe = np.zeros(len(e), dtype=bool)
    fe[me] = True
    ur = pd.DataFrame({"year": u["year"], "m": fu}).groupby("year")["m"].agg(
        usgs_n="size", usgs_matched="sum")
    er = pd.DataFrame({"year": e["year"], "m": fe}).groupby("year")["m"].agg(
        emsc_n="size", emsc_matched="sum")
    out = ur.join(er, how="outer").fillna(0).reset_index()
    out["usgs_match_pct"] = (100 * out["usgs_matched"] / out["usgs_n"].clip(lower=1)).round(1)
    out["emsc_match_pct"] = (100 * out["emsc_matched"] / out["emsc_n"].clip(lower=1)).round(1)
    return out


def closest_distance_hist(cand, out_csv, out_png):
    c = cand[cand["adt"] <= 60]
    best = c.groupby("ui")["dist"].min()
    bins = np.logspace(-1, 4, 61)
    counts, edges = np.histogram(best.to_numpy(), bins=bins)
    pd.DataFrame({"dist_from_km": edges[:-1].round(3), "dist_to_km": edges[1:].round(3),
                  "n_usgs_events": counts}).to_csv(out_csv, index=False)
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(8, 4.5))
        ax.hist(best.to_numpy(), bins=bins)
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlabel("distance to nearest EMSC candidate within 60 s (km)")
        ax.set_ylabel("number of USGS events")
        ax.set_title("Where true matches stop and random pairs start")
        fig.tight_layout()
        fig.savefig(out_png, dpi=150)
        plt.close(fig)
    except ImportError:
        log.warning("matplotlib not installed; histogram CSV written but no PNG")


def _describe(label_group, label, s):
    s = s.dropna()
    if len(s) == 0:
        return None
    return {"group": label_group, "label": label, "n": len(s),
            "mean": round(s.mean(), 3), "median": round(s.median(), 3),
            "std": round(s.std(ddof=0), 3),
            "p05": round(s.quantile(0.05), 3), "p95": round(s.quantile(0.95), 3),
            "share_abs_gt_0_5": round(float((s.abs() > 0.5).mean()), 4)}


def magnitude_diff_summary(cand):
    """EMSC minus USGS magnitude, for very close candidates (<=30 s, <=30 km)."""
    c = cand[(cand["adt"] <= 30) & (cand["dist"] <= 30)]
    best = c.sort_values(["ui", "adt"], kind="stable").drop_duplicates("ui")
    rows = [_describe("overall", "all", best["dmag"])]
    band = pd.cut(best["usgs_mag"], [-np.inf, 4.95, 5.95, np.inf],
                  labels=["4.5-4.9", "5.0-5.9", "6.0+"])
    for lab in ["4.5-4.9", "5.0-5.9", "6.0+"]:
        rows.append(_describe("usgs_mag_band", lab, best.loc[band == lab, "dmag"]))
    pair = best["usgs_type"] + " | " + best["emsc_type"]
    for lab, n in pair.value_counts().items():
        if n >= 200:
            rows.append(_describe("mag_type_pair (usgs | emsc)", lab, best.loc[pair == lab, "dmag"]))
    return pd.DataFrame([r for r in rows if r])


def large_event_distance(cand):
    """Do big events need a bigger distance gate? Uses nearest-in-time candidate."""
    c = cand[(cand["adt"] <= 60) & (cand["admag"] <= 1.0)]
    best = c.sort_values(["ui", "adt"], kind="stable").drop_duplicates("ui")
    groups = {"<6.0": best["usgs_mag"] < 6.0,
              "6.0-6.4": (best["usgs_mag"] >= 6.0) & (best["usgs_mag"] < 6.5),
              ">=6.5": best["usgs_mag"] >= 6.5}
    rows = []
    for lab, m in groups.items():
        d = best.loc[m, "dist"]
        if len(d) == 0:
            continue
        rows.append({"usgs_mag_group": lab, "n": len(d),
                     "p50_km": round(d.quantile(0.5), 1), "p75_km": round(d.quantile(0.75), 1),
                     "p90_km": round(d.quantile(0.9), 1), "p95_km": round(d.quantile(0.95), 1),
                     "share_within_100km": round(float((d <= 100).mean()), 4),
                     "share_within_250km": round(float((d <= 250).mean()), 4)})
    return pd.DataFrame(rows)


def main():
    from dotenv import load_dotenv
    from sqlalchemy import create_engine
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2005-01-01")
    args = ap.parse_args()

    load_dotenv()
    engine = create_engine(os.environ["DATABASE_URL"])
    REPORTS.mkdir(exist_ok=True)

    u, e = load_events(engine, args.since)
    log.info("Loaded %d USGS and %d EMSC events since %s", len(u), len(e), args.since)
    cand = build_candidates(u, e)
    log.info("Built %d candidate pairs (+-%s s)", len(cand), CAND_WINDOW_S)

    run_gate_grid(u, e, cand).to_csv(REPORTS / "threshold_study_summary.csv", index=False)
    mu, me = greedy_match(cand, *DEFAULT_GATES)
    per_year_rates(u, e, mu, me).to_csv(REPORTS / "threshold_study_per_year.csv", index=False)
    closest_distance_hist(cand, REPORTS / "closest_distance_hist.csv", REPORTS / "closest_distance_hist.png")
    magnitude_diff_summary(cand).to_csv(REPORTS / "magnitude_diff_summary.csv", index=False)
    large_event_distance(cand).to_csv(REPORTS / "large_event_distance.csv", index=False)
    log.info("Done. Files are in %s/", REPORTS)


if __name__ == "__main__":
    main()