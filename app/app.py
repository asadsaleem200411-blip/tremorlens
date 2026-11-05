"""TremorLens dashboard (Project 1: ETL + EDA).

Run locally from the project root:  streamlit run app/app.py
The database URL comes from .env (local) or from the app secrets (Streamlit Community Cloud).
"""
import os

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import find_dotenv, load_dotenv
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy import create_engine, text

st.set_page_config(page_title="TremorLens", page_icon="🌍", layout="wide")

DEFAULT_DEPTHS = (10, 30, 33, 35)
REGIONS = {
    "World": None,
    "Pakistan and region": {"lat": (23, 38), "lon": (60, 78)},
    "Hindu Kush cluster": {"lat": (35, 37.5), "lon": (69, 72)},
}


# ----------------------------------------------------------------------------------------------
# Database access
# ----------------------------------------------------------------------------------------------
def database_url() -> str:
    url = None
    try:
        url = st.secrets.get("DATABASE_URL")
    except Exception:
        url = None
    if not url:
        load_dotenv(find_dotenv(usecwd=True))
        url = os.environ.get("DATABASE_URL")
    if not url:
        st.error("DATABASE_URL is not set. Put it in .env (local) or in the app secrets (Streamlit Cloud).")
        st.stop()
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    if url.startswith("postgresql://"):
        url = "postgresql+psycopg://" + url[len("postgresql://"):]
    return url


@st.cache_resource
def get_engine():
    return create_engine(database_url(), pool_pre_ping=True)


def run(sql, params=None):
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def year_sql() -> str:
    if get_engine().dialect.name == "postgresql":
        return "CAST(EXTRACT(YEAR FROM event_time) AS INTEGER)"
    return "CAST(strftime('%Y', event_time) AS INTEGER)"


@st.cache_data(ttl=600, show_spinner=False)
def load_counts():
    one = lambda sql: int(run(sql).iloc[0, 0])
    return {
        "usgs": one("SELECT COUNT(*) FROM usgs_events"),
        "emsc": one("SELECT COUNT(*) FROM emsc_events"),
        "clean": one("SELECT COUNT(*) FROM events_clean"),
        "both": one("SELECT COUNT(*) FROM events_clean WHERE source = 'both'"),
        "rejected": one("SELECT COUNT(*) FROM rejected_records"),
        "warnings": one("SELECT COUNT(*) FROM quality_issues"),
        "latest_event": str(run("SELECT MAX(event_time) FROM usgs_events").iloc[0, 0]),
    }


@st.cache_data(ttl=600, show_spinner=False)
def load_runs():
    return run("SELECT id, source, started_at, finished_at, rows_fetched, rows_loaded, rows_rejected, status "
               "FROM ingest_runs ORDER BY id DESC LIMIT 15")


@st.cache_data(ttl=3600, show_spinner=False)
def load_yearly():
    d = run(f"SELECT {year_sql()} AS year, source, COUNT(*) AS n FROM events_clean GROUP BY 1, 2")
    t = d.pivot_table(index="year", columns="source", values="n", aggfunc="sum", fill_value=0)
    for col in ("both", "usgs_only", "emsc_only"):
        if col not in t:
            t[col] = 0
    t["usgs_total"] = t["both"] + t["usgs_only"]
    t["emsc_total"] = t["both"] + t["emsc_only"]
    t["usgs_match_rate"] = t["both"] / t["usgs_total"].clip(lower=1)
    t["emsc_match_rate"] = t["both"] / t["emsc_total"].clip(lower=1)
    return t.reset_index()


@st.cache_data(ttl=3600, show_spinner=False)
def load_default_share():
    d = run(f"SELECT {year_sql()} AS year, "
            f"SUM(CASE WHEN depth_km IN (10, 30, 33, 35) THEN 1 ELSE 0 END) AS defaults, COUNT(*) AS n "
            f"FROM usgs_events GROUP BY 1 ORDER BY 1")
    d["share"] = d["defaults"] / d["n"].clip(lower=1)
    return d


@st.cache_data(ttl=3600, show_spinner=False)
def load_issue_counts():
    return run("SELECT issue, source, COUNT(*) AS n FROM quality_issues GROUP BY issue, source ORDER BY n DESC")


@st.cache_data(ttl=3600, show_spinner=False)
def load_issue_rows(issue):
    return run("SELECT source, event_id, event_time, issue, detail FROM quality_issues "
               "WHERE issue = :issue ORDER BY event_time DESC LIMIT 300", {"issue": issue})


@st.cache_data(ttl=3600, show_spinner=False)
def load_rejected():
    return run("SELECT id, source, error_message, rejected_at FROM rejected_records ORDER BY id DESC LIMIT 50")


@st.cache_data(ttl=3600, show_spinner="Loading USGS events from the database...")
def load_usgs():
    df = run("SELECT event_id, event_time, latitude, longitude, depth_km, magnitude, mag_type, place FROM usgs_events")
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
    df["year"] = df["event_time"].dt.year
    df["depth_default"] = df["depth_km"].isin(DEFAULT_DEPTHS)
    return df


@st.cache_data(ttl=3600, show_spinner="Loading matched events from the database...")
def load_pairs():
    df = run("""SELECT u.magnitude AS mag_usgs, e.magnitude AS mag_emsc,
                       u.depth_km AS depth_usgs, e.depth_km AS depth_emsc
                FROM events_clean c
                JOIN usgs_events u ON u.event_id = c.matched_usgs_id
                JOIN emsc_events e ON e.event_id = c.matched_emsc_id
                WHERE c.source = 'both'""")
    df["dmag"] = df["mag_emsc"] - df["mag_usgs"]
    df["ddepth"] = (df["depth_emsc"] - df["depth_usgs"]).abs()
    df["usgs_default"] = df["depth_usgs"].isin(DEFAULT_DEPTHS)
    return df


# ----------------------------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------------------------
def b_value(m, mc, dm=0.1):
    """Aki maximum likelihood b-value for binned magnitudes, with the Shi and Bolt standard error."""
    m = np.asarray(m, dtype=float)
    n = len(m)
    if n < 50:
        return np.nan, np.nan, n
    mean = m.mean()
    b = np.log10(np.e) / (mean - (mc - dm / 2))
    se = 2.30 * b ** 2 * np.sqrt(((m - mean) ** 2).sum() / (n * (n - 1)))
    return b, se, n


def in_region(df, name):
    box = REGIONS[name]
    if box is None:
        return df
    return df[df["latitude"].between(*box["lat"]) & df["longitude"].between(*box["lon"])]


class EventInput(BaseModel):
    """The same physical limits that the ETL pipeline applies to every incoming record."""
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    depth_km: float = Field(ge=-5, le=800)
    magnitude: float


# ----------------------------------------------------------------------------------------------
# Pages
# ----------------------------------------------------------------------------------------------
def page_overview():
    st.title("TremorLens")
    st.caption("A validated global earthquake dataset built from USGS and EMSC "
               "(magnitude 4.5 or more, 2000 to 2026), loaded into PostgreSQL by a tested ETL pipeline.")
    c = load_counts()
    cols = st.columns(5)
    cols[0].metric("USGS events", f"{c['usgs']:,}")
    cols[1].metric("EMSC events", f"{c['emsc']:,}")
    cols[2].metric("Unified events", f"{c['clean']:,}")
    cols[3].metric("Found in both catalogs", f"{c['both']:,}")
    cols[4].metric("Rejected records", f"{c['rejected']:,}")
    st.write(f"Latest event in the database: **{c['latest_event'][:19]}** (UTC). "
             f"Quality warnings recorded: **{c['warnings']:,}**.")

    y = load_yearly()
    left, right = st.columns(2)
    with left:
        fig = px.bar(y, x="year", y=["both", "usgs_only", "emsc_only"],
                     labels={"value": "events", "variable": "found in"},
                     title="Unified events per year")
        st.plotly_chart(fig)
    with right:
        fig = go.Figure()
        fig.add_scatter(x=y["year"], y=y["usgs_total"], name="USGS", mode="lines+markers")
        fig.add_scatter(x=y["year"], y=y["emsc_total"], name="EMSC", mode="lines+markers")
        fig.update_layout(title="Events per year in each catalog (EMSC coverage grows over time)",
                          xaxis_title="year", yaxis_title="events")
        st.plotly_chart(fig)

    st.subheader("Pipeline health")
    runs = load_runs()
    if len(runs):
        st.dataframe(runs, hide_index=True)
    else:
        st.info("No pipeline runs have been logged yet.")
    st.caption("Every pipeline run writes one row here: how many records were fetched, loaded and rejected, "
               "and whether it finished successfully.")


def page_map():
    st.header("Map of events")
    usgs = load_usgs()
    c1, c2, c3, c4 = st.columns(4)
    years = c1.slider("Years", 2000, 2026, (2005, 2026))
    min_mag = c2.slider("Minimum magnitude", 4.5, 8.0, 6.0, 0.1)
    region = c3.selectbox("Region", list(REGIONS))
    hide_default = c4.checkbox("Hide default-like depths", value=False,
                               help="Depths of exactly 10, 30, 33 or 35 km are very likely default values, not measurements.")

    df = usgs[usgs["year"].between(*years) & (usgs["magnitude"] >= min_mag)]
    df = in_region(df, region)
    if hide_default:
        df = df[~df["depth_default"]]

    known = df.loc[~df["depth_default"] & (df["depth_km"] >= 0), "depth_km"]
    m1, m2, m3 = st.columns(3)
    m1.metric("Events in this selection", f"{len(df):,}")
    m2.metric("Magnitude 6 or more", f"{int((df['magnitude'] >= 6).sum()):,}")
    m3.metric("Median measured depth", f"{known.median():.0f} km" if len(known) else "n/a")

    if df.empty:
        st.warning("No events match this selection.")
        return
    cap = 4000
    plot = df.nlargest(cap, "magnitude").copy()
    if len(df) > cap:
        st.caption(f"Showing the {cap:,} largest of {len(df):,} events.")
    plot["size"] = (plot["magnitude"] - 4.0).clip(lower=0.3) ** 2
    plot["time"] = plot["event_time"].dt.strftime("%Y-%m-%d %H:%M")
    fig = px.scatter_geo(plot, lat="latitude", lon="longitude", size="size", size_max=16,
                         color="depth_km", color_continuous_scale="Viridis_r", range_color=(0, 700),
                         hover_name="place", hover_data={"time": True, "magnitude": True, "depth_km": True,
                                                         "latitude": False, "longitude": False, "size": False},
                         projection="natural earth")
    box = REGIONS[region]
    if box:
        fig.update_geos(lonaxis_range=list(box["lon"]), lataxis_range=list(box["lat"]))
    fig.update_layout(height=560, margin={"l": 0, "r": 0, "t": 10, "b": 0})
    st.plotly_chart(fig)

    top = df.nlargest(10, "magnitude")[["event_time", "magnitude", "depth_km", "place"]].copy()
    top["event_time"] = top["event_time"].dt.strftime("%Y-%m-%d %H:%M")
    st.subheader("Largest events in this selection")
    st.dataframe(top, hide_index=True)


def page_analysis():
    st.header("Exploratory analysis")
    usgs = load_usgs()

    st.subheader("Gutenberg-Richter law")
    periods = {"2005 to 2026 (recommended)": (2005, 2026), "2000 to 2004": (2000, 2004), "All years": (2000, 2026)}
    c1, c2 = st.columns(2)
    period = c1.selectbox("Period", list(periods))
    mc = c2.slider("Completeness magnitude Mc", 4.5, 5.5, 4.8, 0.1)
    lo, hi = periods[period]
    mags = usgs.loc[usgs["year"].between(lo, hi), "magnitude"].dropna()
    mags = mags[mags >= mc - 1e-9]
    b, se, n = b_value(mags, mc)
    if np.isnan(b):
        st.warning("Too few events for this choice.")
    else:
        grid = np.round(np.arange(mc, mags.max() + 0.1, 0.1), 1)
        cum = np.array([(mags >= g - 1e-9).sum() for g in grid])
        a = np.log10(cum[0]) + b * mc
        fig = go.Figure()
        fig.add_scatter(x=grid, y=cum, mode="markers", name="observed N(M or more)")
        fig.add_scatter(x=grid, y=10 ** (a - b * grid), mode="lines", name=f"fit, b = {b:.2f}")
        fig.update_layout(yaxis_type="log", xaxis_title="magnitude", yaxis_title="events with magnitude of at least M",
                          title=f"b = {b:.3f} +/- {se:.3f}  (n = {n:,})")
        st.plotly_chart(fig)
    st.caption("The catalog is not homogeneous: in the early 2000s it was incomplete at the low end, so b is only "
               "stable from 2005 on and for Mc of 4.8 or higher (about 1.11 to 1.18).")

    st.subheader("Depth")
    hide = st.checkbox("Hide default-like depths (10, 30, 33 and 35 km)", value=True)
    d = usgs.loc[usgs["depth_km"] >= 0]
    if hide:
        d = d[~d["depth_default"]]
    fig = px.histogram(d, x="depth_km", nbins=70, log_y=True, labels={"depth_km": "depth (km)"})
    fig.update_layout(yaxis_title="events (log scale)")
    st.plotly_chart(fig)
    st.caption(f"{usgs['depth_default'].mean():.1%} of all events have one of the four default-like depths.")

    st.subheader("Events per year by magnitude band")
    bands = pd.cut(usgs["magnitude"], [0, 5.0, 6.0, 7.0, 20], right=False, labels=["4.5-4.9", "5.0-5.9", "6.0-6.9", "7.0+"])
    yb = usgs.groupby(["year", bands], observed=True).size().reset_index(name="events")
    yb = yb.rename(columns={"magnitude": "band"})
    fig = px.bar(yb[yb["year"] <= 2025], x="year", y="events", color="band", barmode="group",
                 title="2026 is left out (incomplete year)")
    st.plotly_chart(fig)
    st.caption("Large events (6 and above) have a stable yearly count, while small events grew about 60 percent between "
               "the early 2000s and 2012: a sign of a catalog that became more complete.")


def page_quality():
    st.header("Data quality")
    c = load_counts()
    k1, k2 = st.columns(2)
    k1.metric("Rejected records (impossible values)", f"{c['rejected']:,}")
    k2.metric("Quality warnings (doubtful values)", f"{c['warnings']:,}")
    st.write("Validation has two levels. **Hard errors** (impossible values such as a latitude of 95) are rejected "
             "and stored in `rejected_records`. **Soft warnings** (possible but doubtful values) are loaded and "
             "flagged in `quality_issues`.")
    if c["rejected"] == 0:
        st.success("No record from USGS or EMSC failed validation. The validation tester below shows the rules at work.")
    else:
        st.dataframe(load_rejected(), hide_index=True)

    st.subheader("Soft warnings by type")
    counts = load_issue_counts()
    if len(counts):
        fig = px.bar(counts, x="n", y="issue", color="source", orientation="h", log_x=True,
                     labels={"n": "warnings (log scale)", "issue": ""})
        st.plotly_chart(fig)
        issue = st.selectbox("Show events with this warning", counts["issue"].unique())
        rows = load_issue_rows(issue)
        st.caption("Most recent 300 events.")
        st.dataframe(rows, hide_index=True)

    st.subheader("How well do the two catalogs overlap?")
    y = load_yearly()
    fig = go.Figure()
    fig.add_scatter(x=y["year"], y=y["usgs_match_rate"], name="share of USGS events found in EMSC", mode="lines+markers")
    fig.add_scatter(x=y["year"], y=y["emsc_match_rate"], name="share of EMSC events found in USGS", mode="lines+markers")
    fig.update_layout(yaxis_range=[0, 1], xaxis_title="year", yaxis_title="match rate")
    st.plotly_chart(fig)
    st.caption("The USGS-side rate measures how complete EMSC was, not how good the matching is. "
               "The EMSC-side rate is stable (83 to 93 percent) from 2005 on.")

    st.subheader("Share of events with a default-like depth")
    s = load_default_share()
    fig = px.line(s, x="year", y="share", markers=True, labels={"share": "share of events"})
    fig.update_layout(yaxis_range=[0, 1])
    st.plotly_chart(fig)
    st.caption("Depths of exactly 10, 30, 33 or 35 km. Which value is used changes over the years "
               "(33 km until 2003, 35 km from 2005 to about 2013, 10 km increasingly after 2016).")

    st.subheader("Validation tester")
    st.write("Type an event and see whether the pipeline would accept it. Try a latitude of 95, a depth of 2000, "
             "or leave the magnitude empty.")
    f1, f2, f3, f4 = st.columns(4)
    lat = f1.text_input("Latitude", "34.5")
    lon = f2.text_input("Longitude", "73.5")
    depth = f3.text_input("Depth (km)", "26")
    mag = f4.text_input("Magnitude", "7.6")
    try:
        EventInput(latitude=lat, longitude=lon, depth_km=depth, magnitude=mag)
        st.success("Accepted: all values are inside the physical limits.")
    except ValidationError as err:
        st.error("Rejected by validation:")
        for e in err.errors():
            st.write(f"- **{e['loc'][0]}**: {e['msg']}")


def page_compare():
    st.header("USGS against EMSC")
    pairs = load_pairs()
    st.write(f"{len(pairs):,} events were found in both catalogs (matched within 60 seconds, 150 km and 1.0 magnitude units).")

    s = pairs["dmag"]
    k = st.columns(5)
    k[0].metric("Mean difference", f"{s.mean():+.3f}")
    k[1].metric("Median difference", f"{s.median():+.2f}")
    k[2].metric("Standard deviation", f"{s.std():.3f}")
    k[3].metric("Within 0.1", f"{(s.abs() <= 0.1001).mean():.1%}")
    k[4].metric("Within 0.3", f"{(s.abs() <= 0.3001).mean():.1%}")

    left, right = st.columns(2)
    with left:
        fig = px.density_heatmap(pairs, x="mag_usgs", y="mag_emsc", nbinsx=50, nbinsy=50, color_continuous_scale="Viridis",
                                 labels={"mag_usgs": "USGS magnitude", "mag_emsc": "EMSC magnitude"})
        lim = [4.4, float(max(pairs["mag_usgs"].max(), pairs["mag_emsc"].max())) + 0.1]
        fig.add_scatter(x=lim, y=lim, mode="lines", name="EMSC = USGS", line={"color": "red", "dash": "dash"})
        fig.update_layout(title="Magnitude of matched events")
        st.plotly_chart(fig)
    with right:
        fig = px.histogram(pairs, x=pairs["dmag"].clip(-1.25, 1.25), nbins=25, log_y=True,
                           labels={"x": "EMSC minus USGS magnitude"})
        fig.update_layout(title="Magnitude difference (log scale)", xaxis_title="EMSC minus USGS magnitude")
        st.plotly_chart(fig)

    left, right = st.columns(2)
    dp = pairs.dropna(subset=["depth_usgs", "depth_emsc"])
    with left:
        fig = px.density_heatmap(dp.assign(du=dp["depth_usgs"].clip(0, 700), de=dp["depth_emsc"].clip(0, 700)),
                                 x="du", y="de", nbinsx=70, nbinsy=70, color_continuous_scale="Viridis",
                                 labels={"du": "USGS depth (km)", "de": "EMSC depth (km)"})
        fig.add_scatter(x=[0, 700], y=[0, 700], mode="lines", name="EMSC = USGS", line={"color": "red", "dash": "dash"})
        fig.update_layout(title="Depth of matched events")
        st.plotly_chart(fig)
    with right:
        g = dp.groupby("usgs_default")["ddepth"].agg(pairs="size", median="median", mean="mean",
                                                      share_over_100km=lambda x: (x > 100).mean())
        g.index = ["USGS depth measured" if not i else "USGS depth default-like" for i in g.index]
        st.write("Depth disagreement (absolute difference, km)")
        st.dataframe(g.round(3))
        st.caption("Agreement is higher when the USGS depth is default-like, most likely because EMSC repeats "
                   "the USGS (NEIC) depth including its default value. Agreement of two default values does not "
                   "prove that the depth is right.")


def page_about():
    st.header("About this project")
    st.markdown("""
**Pipeline.** USGS and EMSC are read through their public APIs in monthly windows, every record is validated with
Pydantic, cleaned with pandas, and loaded into PostgreSQL (Neon) with upserts so that the pipeline can be re-run
safely. A reconciliation step matches events across the two catalogs and writes the unified table.

**Matching rule.** An EMSC event is matched to a USGS event when the origin times differ by at most 60 seconds,
the epicentres are within 150 km and the magnitudes differ by at most 1.0. Matching is one-to-one. The gates were
chosen from a study of the data, not assumed.

**Things to keep in mind**
- USGS is complete for the whole period; EMSC is sparse before 2005 and keeps improving, so USGS is the base catalog.
- About 46 percent of USGS events carry a default-like depth, and the value used changes over the years.
- Before 2005 the catalog was incomplete at the low end, so the b-value is only stable from 2005 on.
- Some non-ASCII letters in place names appear as question marks in the USGS data itself.
- The project does not predict earthquakes, which is impossible.

**Tables.** `usgs_events`, `emsc_events`, `events_clean`, `rejected_records`, `ingest_runs`, `quality_issues`.
""")


PAGES = {
    "Overview": page_overview,
    "Map": page_map,
    "Analysis": page_analysis,
    "Data quality": page_quality,
    "USGS vs EMSC": page_compare,
    "About": page_about,
}

st.sidebar.title("TremorLens")
choice = st.sidebar.radio("Page", list(PAGES), label_visibility="collapsed")
st.sidebar.caption("Project 1: ETL and EDA of global earthquake catalogs.")
PAGES[choice]()
