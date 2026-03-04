"""
How Clean Is The 626? — Streamlit dashboard
Run from repo root:  streamlit run src/app.py
"""
import json
import os
from datetime import datetime

import duckdb
import pandas as pd
import pydeck as pdk
import streamlit as st

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
MART_HEALTH  = "data/processed/mart_facility_health.parquet"
DIM_ZIP_GEO  = "data/processed/dim_zip_geo.parquet"
MART_NEAR_ME = "data/processed/mart_near_me.parquet"
FCT_INSP     = "data/processed/fct_inspection.parquet"
FCT_VIOL     = "data/processed/fct_violation.parquet"
MARTS_META   = "data/processed/marts_meta.json"

REQUIRED_FILES = {
    MART_HEALTH: "make marts",
    DIM_ZIP_GEO: "make marts",
}

EXAMPLE_ZIPS = ["91754", "91776", "91801", "91101", "91030", "91731", "91732", "91007"]

# ---------------------------------------------------------------------------
# Page config — must be the first Streamlit call
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="How Clean Is The 626?",
    page_icon="🍜",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Startup checks
# ---------------------------------------------------------------------------
def check_required_inputs() -> None:
    missing = [(p, t) for p, t in REQUIRED_FILES.items() if not os.path.exists(p)]
    if missing:
        st.error("**Missing required data files. Run these make targets first:**")
        for path, target in missing:
            st.code(f"{target}   # produces {path}")
        st.stop()


def near_me_available() -> bool:
    return os.path.exists(MART_NEAR_ME)


# ---------------------------------------------------------------------------
# DuckDB — shared in-memory connection
# ---------------------------------------------------------------------------
@st.cache_resource
def get_con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect()


# ---------------------------------------------------------------------------
# Meta — last-updated timestamp + facility distribution
# ---------------------------------------------------------------------------
@st.cache_data
def load_meta() -> dict:
    result = {"last_updated": "N/A", "dist": {}, "ci_stats": {}}
    if os.path.exists(MARTS_META):
        with open(MARTS_META) as f:
            m = json.load(f)
        ts = m.get("generated_at_utc", "")
        if ts:
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                result["last_updated"] = dt.strftime("%b %d, %Y")
            except ValueError:
                result["last_updated"] = ts[:10]
        result["dist"]     = m.get("facility_distribution", {})
        result["ci_stats"] = m.get("cleanliness_index_stats", {})
    return result


# ---------------------------------------------------------------------------
# KPI summary (cached — same for whole session)
# ---------------------------------------------------------------------------
@st.cache_data
def load_kpi_summary() -> dict:
    con = get_con()
    row = con.execute(f"""
        SELECT
            COUNT(*)                                              AS n_facilities,
            ROUND(MEDIAN(cleanliness_index), 2)                   AS median_ci,
            ROUND(100.0 * SUM(low_data_flag::INT) / COUNT(*), 1) AS pct_low_data,
            MAX(latest_activity_date)                             AS ref_date
        FROM read_parquet('{MART_HEALTH}')
    """).fetchone()
    return {
        "n_facilities": row[0],
        "median_ci":    row[1],
        "pct_low_data": row[2],
        "ref_date":     str(row[3]) if row[3] else "N/A",
    }


# ---------------------------------------------------------------------------
# City + ZIP lists for Search filters (cached)
# ---------------------------------------------------------------------------
@st.cache_data
def load_city_zip_lists() -> tuple:
    con = get_con()
    cities = [r[0] for r in con.execute(
        f"SELECT DISTINCT facility_city FROM read_parquet('{MART_HEALTH}') "
        "WHERE facility_city IS NOT NULL ORDER BY 1"
    ).fetchall()]
    zips = [r[0] for r in con.execute(
        f"SELECT DISTINCT facility_zip5 FROM read_parquet('{MART_HEALTH}') "
        "WHERE facility_zip5 IS NOT NULL ORDER BY 1"
    ).fetchall()]
    return cities, zips


# ---------------------------------------------------------------------------
# ZIP validation
# ---------------------------------------------------------------------------
@st.cache_data
def zip_is_known(zip_code: str) -> bool:
    con = get_con()
    result = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{DIM_ZIP_GEO}') WHERE zip = ?",
        [zip_code],
    ).fetchone()
    return result[0] > 0


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------
def _low_data_sql(include_low_data: bool, alias: str) -> str:
    return "" if include_low_data else f"AND {alias}.low_data_flag = false"


_HEALTH_COLS = """
    h.facility_name,
    h.facility_address,
    h.facility_city,
    h.facility_zip5,
    h.latest_grade,
    h.latest_score,
    ROUND(h.cleanliness_index, 2) AS cleanliness_index,
    h.inspections_12mo,
    h.facility_key
"""

_DISPLAY_RENAME = {
    "facility_name":    "Facility",
    "facility_address": "Address",
    "facility_city":    "City",
    "facility_zip5":    "ZIP",
    "latest_grade":     "Grade",
    "latest_score":     "Score",
    "cleanliness_index":"Cleanliness Index",
    "inspections_12mo": "Inspections (12mo)",
    "distance_miles":   "Distance (mi)",
    "near_me_score":    "Near-Me Score",
}


def _rename_for_display(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={k: v for k, v in _DISPLAY_RENAME.items() if k in df.columns})


# ── Best / Worst ──────────────────────────────────────────────────────────────

@st.cache_data
def query_best_worst_health(mode: str, min_insp: int, include_low_data: bool, n: int) -> pd.DataFrame:
    con   = get_con()
    order = "DESC" if mode == "best" else "ASC"
    ld    = _low_data_sql(include_low_data, "h")
    sql = f"""
        SELECT {_HEALTH_COLS}
        FROM read_parquet('{MART_HEALTH}') h
        WHERE h.inspections_12mo >= ?
          AND h.cleanliness_index IS NOT NULL
          {ld}
        ORDER BY h.cleanliness_index {order} NULLS LAST
        LIMIT ?
    """
    return con.execute(sql, [int(min_insp), int(n)]).df()


# ── Search ────────────────────────────────────────────────────────────────────

@st.cache_data
def query_search_health(
    search_text: str,
    city_filter: str,
    zip_filter: str,
    min_insp: int,
    include_low_data: bool,
    n: int,
) -> pd.DataFrame:
    con      = get_con()
    ld       = _low_data_sql(include_low_data, "h")
    like_val = f"%{search_text.strip()}%"
    zip_val  = search_text.strip()

    city_clause = f"AND h.facility_city = '{city_filter}'" if city_filter else ""
    zip_clause  = f"AND h.facility_zip5 = '{zip_filter}'"  if zip_filter  else ""

    sql = f"""
        SELECT {_HEALTH_COLS}
        FROM read_parquet('{MART_HEALTH}') h
        WHERE h.inspections_12mo >= ?
          {ld}
          {city_clause}
          {zip_clause}
          AND (
              h.facility_name    ILIKE ?
           OR h.facility_address ILIKE ?
           OR h.facility_city    ILIKE ?
           OR h.facility_zip5    = ?
          )
        ORDER BY h.cleanliness_index DESC NULLS LAST
        LIMIT ?
    """
    return con.execute(sql, [int(min_insp), like_val, like_val, like_val, zip_val, int(n)]).df()


# ── Map queries ───────────────────────────────────────────────────────────────

@st.cache_data
def query_map_health(
    mode: str, min_insp: int, include_low_data: bool, n: int, search_text: str = ""
) -> pd.DataFrame:
    con   = get_con()
    order = "ASC" if mode == "worst" else "DESC"
    ld    = _low_data_sql(include_low_data, "h")

    if mode in ("best", "worst"):
        sql = f"""
            SELECT
                h.facility_name, h.facility_city, h.facility_zip5,
                ROUND(h.cleanliness_index, 2) AS cleanliness_index,
                h.facility_key,
                g.lat, g.lon
            FROM read_parquet('{MART_HEALTH}') h
            JOIN read_parquet('{DIM_ZIP_GEO}')  g ON h.facility_zip5 = g.zip
            WHERE h.inspections_12mo >= ?
              AND h.cleanliness_index IS NOT NULL
              {ld}
            ORDER BY h.cleanliness_index {order} NULLS LAST
            LIMIT ?
        """
        return con.execute(sql, [int(min_insp), int(n)]).df()

    like_val = f"%{search_text.strip()}%"
    zip_val  = search_text.strip()
    sql = f"""
        SELECT
            h.facility_name, h.facility_city, h.facility_zip5,
            ROUND(h.cleanliness_index, 2) AS cleanliness_index,
            h.facility_key,
            g.lat, g.lon
        FROM read_parquet('{MART_HEALTH}') h
        JOIN read_parquet('{DIM_ZIP_GEO}')  g ON h.facility_zip5 = g.zip
        WHERE h.inspections_12mo >= ?
          {ld}
          AND (
              h.facility_name    ILIKE ?
           OR h.facility_address ILIKE ?
           OR h.facility_city    ILIKE ?
           OR h.facility_zip5    = ?
          )
        ORDER BY h.cleanliness_index DESC NULLS LAST
        LIMIT ?
    """
    return con.execute(sql, [int(min_insp), like_val, like_val, like_val, zip_val, int(n)]).df()


@st.cache_data
def query_map_near_me(
    mode: str, min_insp: int, include_low_data: bool, n: int, search_text: str = ""
) -> pd.DataFrame:
    con   = get_con()
    order = "ASC" if mode == "worst" else "DESC"
    ld    = _low_data_sql(include_low_data, "n")

    if mode in ("best", "worst"):
        sql = f"""
            SELECT
                n.facility_name, n.facility_city, n.facility_zip5,
                ROUND(n.near_me_score,  2) AS near_me_score,
                ROUND(n.distance_miles, 1) AS distance_miles,
                n.facility_key,
                n.zip_lat AS lat, n.zip_lon AS lon
            FROM read_parquet('{MART_NEAR_ME}') n
            JOIN read_parquet('{MART_HEALTH}')  h ON n.facility_key = h.facility_key
            WHERE h.inspections_12mo >= ?
              AND n.near_me_score IS NOT NULL
              {ld}
            ORDER BY n.near_me_score {order} NULLS LAST
            LIMIT ?
        """
        return con.execute(sql, [int(min_insp), int(n)]).df()

    like_val = f"%{search_text.strip()}%"
    zip_val  = search_text.strip()
    sql = f"""
        SELECT
            n.facility_name, n.facility_city, n.facility_zip5,
            ROUND(n.near_me_score,  2) AS near_me_score,
            ROUND(n.distance_miles, 1) AS distance_miles,
            n.facility_key,
            n.zip_lat AS lat, n.zip_lon AS lon
        FROM read_parquet('{MART_NEAR_ME}') n
        JOIN read_parquet('{MART_HEALTH}')  h ON n.facility_key = h.facility_key
        WHERE h.inspections_12mo >= ?
          {ld}
          AND (
              n.facility_name    ILIKE ?
           OR n.facility_address ILIKE ?
           OR n.facility_city    ILIKE ?
           OR n.facility_zip5    = ?
          )
        ORDER BY n.near_me_score DESC NULLS LAST
        LIMIT ?
    """
    return con.execute(sql, [int(min_insp), like_val, like_val, like_val, zip_val, int(n)]).df()


# ── Facility drilldown queries ────────────────────────────────────────────────

@st.cache_data
def query_facility_info(facility_key: str) -> dict:
    con = get_con()
    row = con.execute(
        f"SELECT facility_name, facility_address, facility_city, facility_state, "
        f"facility_zip5, owner_name, latest_activity_date, latest_score, latest_grade, "
        f"cleanliness_index, inspections_12mo, low_data_flag "
        f"FROM read_parquet('{MART_HEALTH}') WHERE facility_key = ?",
        [facility_key],
    ).fetchone()
    if not row:
        return {}
    cols = [
        "facility_name", "facility_address", "facility_city", "facility_state",
        "facility_zip5", "owner_name", "latest_activity_date", "latest_score",
        "latest_grade", "cleanliness_index", "inspections_12mo", "low_data_flag",
    ]
    return dict(zip(cols, row))


@st.cache_data
def query_near_me_info(facility_key: str) -> dict:
    if not near_me_available():
        return {}
    con = get_con()
    row = con.execute(
        f"SELECT near_me_score, distance_miles "
        f"FROM read_parquet('{MART_NEAR_ME}') WHERE facility_key = ?",
        [facility_key],
    ).fetchone()
    if not row:
        return {}
    return {"near_me_score": row[0], "distance_miles": row[1]}


@st.cache_data
def query_recent_inspections(facility_key: str) -> pd.DataFrame:
    con = get_con()
    return con.execute(
        f"SELECT activity_date, score, grade "
        f"FROM read_parquet('{FCT_INSP}') "
        f"WHERE facility_key = ? "
        f"ORDER BY activity_date DESC LIMIT 5",
        [facility_key],
    ).df()


@st.cache_data
def query_top_violations(facility_key: str) -> pd.DataFrame:
    con = get_con()
    return con.execute(
        f"SELECT violation_description, COUNT(*) AS occurrences, SUM(points) AS total_points "
        f"FROM read_parquet('{FCT_VIOL}') "
        f"WHERE facility_key = ? "
        f"GROUP BY violation_description "
        f"ORDER BY total_points DESC NULLS LAST LIMIT 10",
        [facility_key],
    ).df()


# ---------------------------------------------------------------------------
# Facility Details card
# ---------------------------------------------------------------------------
def show_facility_drilldown(facility_key: str) -> None:
    info = query_facility_info(facility_key)
    if not info:
        st.warning("Facility details not found.")
        return

    near_me = query_near_me_info(facility_key)

    st.divider()
    st.subheader(f"Facility Details — {info.get('facility_name', '')}")

    st.markdown(
        f"**{info.get('facility_address', '')}**, "
        f"{info.get('facility_city', '')} {info.get('facility_zip5', '')}  \n"
        f"**Owner:** {info.get('owner_name', '') or '—'}"
        f"&nbsp; · &nbsp;**Last inspected:** {info.get('latest_activity_date', '')}"
    )

    if info.get("low_data_flag"):
        st.warning(
            "Low-data facility — fewer than 2 inspections in the 12-month window. "
            "Score may be less reliable."
        )

    # Primary metrics
    ci     = info.get("cleanliness_index")
    ci_str = f"{ci:.1f} / 100" if ci is not None else "N/A"

    m1, m2, m3, m4 = st.columns(4)
    m1.metric(
        "Cleanliness Index",
        ci_str,
        help=(
            "Composite score (0–100) over the trailing 12 months. "
            "Factors in: inspection scores, score trend, violations per visit, "
            "and severity of violations. Higher = cleaner."
        ),
    )
    m2.metric("Latest Grade", info.get("latest_grade") or "N/A")
    m3.metric("Latest Score", info.get("latest_score") or "N/A")
    m4.metric("Inspections (12mo)", info.get("inspections_12mo") or 0)

    # Near-me metrics (if available)
    if near_me:
        n1, n2, *_ = st.columns(4)
        n1.metric(
            "Near-Me Score",
            f"{near_me.get('near_me_score', 0):.2f}",
            help=(
                "Cleanliness Index discounted by distance: "
                "score ÷ (1 + distance_miles). "
                "Favors both cleanliness and proximity."
            ),
        )
        n2.metric("Distance", f"{near_me.get('distance_miles', 0):.1f} mi")

    st.divider()

    # Inspections + violations side by side
    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("**Last 5 Inspections**")
        df_insp = query_recent_inspections(facility_key)
        if df_insp.empty:
            st.caption("No inspection records found.")
        else:
            st.dataframe(
                df_insp.rename(columns={
                    "activity_date": "Date",
                    "score":         "Score",
                    "grade":         "Grade",
                }),
                use_container_width=True,
                hide_index=True,
            )

    with col_b:
        st.markdown("**Top Violations (by total points)**")
        df_viol = query_top_violations(facility_key)
        if df_viol.empty:
            st.caption("No violation records found.")
        else:
            st.dataframe(
                df_viol.rename(columns={
                    "violation_description": "Violation",
                    "occurrences":           "Count",
                    "total_points":          "Total Pts",
                }),
                use_container_width=True,
                hide_index=True,
            )


# ---------------------------------------------------------------------------
# Map color helper
# ---------------------------------------------------------------------------
def _add_color_column(df: pd.DataFrame, mode: str, metric_col: str) -> pd.DataFrame:
    """Return a copy of df with a '_color' column ([r,g,b,a] per row).

    BEST   → green gradient  (higher score = brighter green)
    WORST  → red gradient    (lower score  = brighter red)
    SEARCH → blue gradient   (gradient by score)
    """
    df = df.copy()
    vals = df[metric_col].fillna(0)
    mn, mx = float(vals.min()), float(vals.max())

    def _norm(v: float) -> float:
        return (v - mn) / (mx - mn) if mx > mn else 0.5

    colors = []
    for v in vals:
        n = _norm(float(v))
        if mode == "BEST":
            colors.append([int(30 + (1 - n) * 60), int(160 + n * 95), int(30 + (1 - n) * 60), 210])
        elif mode == "WORST":
            colors.append([int(160 + (1 - n) * 95), int(30 + n * 100), int(30 + n * 60), 210])
        else:  # SEARCH
            colors.append([20, int(100 + n * 80), int(180 + n * 75), 210])
    df["_color"] = colors
    return df


# ---------------------------------------------------------------------------
# Ranked table with download + drilldown
# ---------------------------------------------------------------------------
def show_ranked_table(df: pd.DataFrame, tab_key: str, n_rows: int = 50) -> None:
    """Ranked, renamed table with CSV download and facility drilldown."""
    if df.empty:
        st.info("No results match the current filters.")
        return

    display_df = df.head(n_rows).copy().reset_index(drop=True)
    display_df.insert(0, "#", range(1, len(display_df) + 1))

    facility_keys  = (
        display_df["facility_key"].tolist()
        if "facility_key" in display_df.columns else []
    )
    facility_names = (
        display_df["facility_name"].tolist()
        if "facility_name" in display_df.columns else []
    )

    disp = _rename_for_display(display_df.drop(columns=["facility_key"], errors="ignore"))
    st.dataframe(disp, use_container_width=True, hide_index=True)

    csv = disp.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download CSV",
        csv,
        file_name=f"{tab_key}.csv",
        mime="text/csv",
        key=f"dl_{tab_key}",
    )

    # Drilldown — rank-prefixed labels prevent issues with duplicate names
    if facility_keys:
        labels  = [f"{i + 1}. {name}" for i, name in enumerate(facility_names)]
        key_map = dict(zip(labels, facility_keys))
        options = ["— select a facility to view details —"] + labels
        chosen  = st.selectbox("View facility details", options, key=f"sel_{tab_key}")
        if chosen != "— select a facility to view details —":
            show_facility_drilldown(key_map[chosen])


# ===========================================================================
# Bootstrap
# ===========================================================================
check_required_inputs()

meta = load_meta()
kpi  = load_kpi_summary()

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("How Clean Is The 626?")
st.markdown(
    "Restaurant health inspection scores for the **626 area code** — San Gabriel Valley.  \n"
    f"Data through **{kpi['ref_date']}**"
    + (f" &nbsp;·&nbsp; Last updated **{meta['last_updated']}**" if meta["last_updated"] != "N/A" else "")
)
st.divider()

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("## Filters")

    with st.expander("Location", expanded=True):
        home_zip = st.text_input(
            "Home ZIP",
            value="91754",
            max_chars=5,
            help="Your home ZIP code. Used as the origin for Near-Me distance scoring.",
        )
        if home_zip and len(home_zip) == 5:
            if not zip_is_known(home_zip):
                st.error(
                    f"ZIP **{home_zip}** not found in the dataset.  \n"
                    "Try one of these 626 ZIPs:  \n"
                    + "  \n".join(f"• `{z}`" for z in EXAMPLE_ZIPS)
                )

        metric_options = ["cleanliness_index"]
        if near_me_available():
            metric_options.append("near_me_score")
        metric = st.selectbox(
            "Metric",
            metric_options,
            format_func=lambda x: {
                "cleanliness_index": "Cleanliness Index",
                "near_me_score":     "Near-Me Score",
            }.get(x, x),
            help=(
                "**Cleanliness Index** — composite 0–100 score based on inspection history.\n\n"
                "**Near-Me Score** — Cleanliness Index weighted by distance from Home ZIP."
            ),
        )

    with st.expander("Data Quality", expanded=False):
        include_low_data = st.checkbox(
            "Include low-data facilities",
            value=False,
            help=(
                "Low-data = fewer than 2 inspections in the 12-month window. "
                "Scores exist but are less reliable."
            ),
        )
        min_inspections = st.number_input(
            "Min inspections (12mo)",
            min_value=0,
            max_value=50,
            value=2,
            step=1,
            help="Only show facilities with at least this many inspections in the past 12 months.",
        )

    with st.expander("Display", expanded=False):
        rows_per_table = st.slider(
            "Rows per table",
            min_value=10,
            max_value=200,
            value=50,
            step=10,
            help="How many rows to show in Best, Worst, and Search tables.",
        )
        map_mode = st.radio(
            "Map shows",
            ["BEST", "WORST", "SEARCH"],
            help="Which facilities to plot on the map.",
        )
        map_n = st.slider(
            "Max map points",
            min_value=10,
            max_value=500,
            value=200,
            step=10,
            help="Maximum number of facilities to render on the map.",
        )

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_overview, tab_best, tab_worst, tab_search, tab_map, tab_about = st.tabs(
    ["Overview", "Best", "Worst", "Search", "Map", "About"]
)

# ── Overview ──────────────────────────────────────────────────────────────────
with tab_overview:
    k1, k2, k3, k4 = st.columns(4)
    k1.metric(
        "Facilities Scored",
        f"{kpi['n_facilities']:,}",
        help="Total facilities with at least one inspection in the dataset.",
    )
    k2.metric(
        "Median Cleanliness Index",
        f"{kpi['median_ci']:.1f}" if kpi["median_ci"] is not None else "N/A",
        help="Median Cleanliness Index across all scored facilities.",
    )
    k3.metric(
        "Low-Data Facilities",
        f"{kpi['pct_low_data']}%",
        help="Facilities with fewer than 2 inspections in the 12-month window.",
    )
    k4.metric(
        "Data Through",
        kpi["ref_date"],
        help="Latest inspection date. Used as the reference point for the 12-month scoring window.",
    )

    st.divider()

    dist = meta.get("dist", {})
    if dist:
        col_chart, col_explain = st.columns(2)

        with col_chart:
            st.markdown("**Facility Distribution by Cleanliness**")
            dist_df = pd.DataFrame(
                {
                    "Tier": ["Excellent (≥90)", "Good (70–89)", "Needs Attention (<70)"],
                    "Facilities": [
                        dist.get("excellent_ge90", 0),
                        dist.get("good_70_89",     0),
                        dist.get("poor_lt70",       0),
                    ],
                }
            ).set_index("Tier")
            st.bar_chart(dist_df)

        with col_explain:
            ci_stats = meta.get("ci_stats", {})
            st.markdown("**About the Cleanliness Index**")
            st.markdown(
                "Each restaurant receives a **0–100 score** computed over a rolling "
                "12-month window. Higher = cleaner.\n\n"
                "**Inputs:**\n"
                "- Raw inspection scores from LA County\n"
                "- Score trend (improving or declining)\n"
                "- Violations per inspection visit\n"
                "- Severity-weighted violation points\n"
                "- Critical / major violation frequency\n\n"
                "Facilities with fewer than 2 inspections in the window are flagged "
                "**low-data** — scores exist but are less reliable."
            )
            if ci_stats:
                st.markdown(
                    f"**Score range:** "
                    f"Min `{ci_stats.get('min', '?')}` · "
                    f"Median `{ci_stats.get('p50', '?')}` · "
                    f"P90 `{ci_stats.get('p90', '?')}` · "
                    f"Max `{ci_stats.get('max', '?')}`"
                )
    else:
        st.caption("Run `make marts` to populate distribution statistics.")

    st.divider()
    st.markdown(
        "Use **Best** and **Worst** to browse top/bottom facilities. "
        "Use **Search** to find a specific restaurant by name, address, city, or ZIP. "
        "Use **Map** for geographic exploration. "
        "Set your **Home ZIP** in the sidebar to enable Near-Me scoring."
    )

# ── Best ──────────────────────────────────────────────────────────────────────
with tab_best:
    st.subheader(f"Top {rows_per_table} Cleanest Facilities")
    st.caption("Ranked by Cleanliness Index, highest first. Adjust filters in the sidebar.")
    df_best = query_best_worst_health("best", min_inspections, include_low_data, rows_per_table)
    show_ranked_table(df_best, "best", n_rows=rows_per_table)

# ── Worst ─────────────────────────────────────────────────────────────────────
with tab_worst:
    st.subheader(f"Bottom {rows_per_table} Facilities")
    st.caption("Ranked by Cleanliness Index, lowest first. Adjust filters in the sidebar.")
    df_worst = query_best_worst_health("worst", min_inspections, include_low_data, rows_per_table)
    show_ranked_table(df_worst, "worst", n_rows=rows_per_table)

# ── Search ────────────────────────────────────────────────────────────────────
with tab_search:
    st.subheader("Search Facilities")

    cities, zips = load_city_zip_lists()

    sc1, sc2 = st.columns(2)
    with sc1:
        city_filter = st.selectbox("Filter by city", ["(any city)"] + cities, key="search_city")
        city_filter = "" if city_filter == "(any city)" else city_filter
    with sc2:
        zip_filter = st.selectbox("Filter by ZIP", ["(any ZIP)"] + zips, key="search_zip")
        zip_filter = "" if zip_filter == "(any ZIP)" else zip_filter

    search_text = st.text_input(
        "Search by name, address, city, or ZIP",
        placeholder="e.g. El Monte Taco, Alhambra, 91801",
        help="Matches on facility name, street address, city, or ZIP code.",
    )

    if search_text.strip() or city_filter or zip_filter:
        df_s = query_search_health(
            search_text, city_filter, zip_filter, min_inspections, include_low_data, 500
        )
        st.caption(f"{len(df_s)} result(s)")
        show_ranked_table(df_s, "search", n_rows=rows_per_table)
    else:
        st.info("Enter a search term or choose a city/ZIP filter above to begin.")

# ── Map ───────────────────────────────────────────────────────────────────────
with tab_map:
    st.subheader("Map View")
    st.caption(
        "Locations are ZIP centroid approximations — the source dataset has no individual "
        "facility coordinates. Use for area-level exploration, not precise navigation."
    )

    if metric == "near_me_score" and not near_me_available():
        st.warning(
            f"**Near-Me Score** selected but `mart_near_me.parquet` is missing.  \n"
            f"Run: `HOME_ZIP={home_zip} make geo_near_me`  \n"
            "Falling back to Cleanliness Index."
        )
        effective_metric = "cleanliness_index"
    else:
        effective_metric = metric

    map_search = ""
    if map_mode == "SEARCH":
        map_search = st.text_input(
            "Search (name / address / city / ZIP)",
            key="map_search_box",
            placeholder="e.g. Alhambra Sushi",
        )

    search_empty = map_mode == "SEARCH" and not map_search.strip()

    if search_empty:
        st.info("Enter a search term above to plot matching facilities.")
    else:
        if effective_metric == "near_me_score" and near_me_available():
            df_map     = query_map_near_me(map_mode.lower(), min_inspections, include_low_data, map_n, map_search)
            metric_col = "near_me_score"
            extra_cols = ["distance_miles"]
        else:
            df_map     = query_map_health(map_mode.lower(), min_inspections, include_low_data, map_n, map_search)
            metric_col = "cleanliness_index"
            extra_cols = []

        if df_map is None or df_map.empty:
            st.info("No facilities match the current filters.")
        else:
            # ── pydeck map ──────────────────────────────────────────────────
            # Build plotting df: drop rows with missing coordinates, add color
            needed = (
                ["facility_name", "facility_city", "facility_zip5",
                 metric_col, "facility_key", "lat", "lon"]
                + extra_cols
            )
            map_plot = (
                df_map[[c for c in needed if c in df_map.columns]]
                .dropna(subset=["lat", "lon"])
                .copy()
            )
            map_plot = _add_color_column(map_plot, map_mode, metric_col)

            # Map center: prefer home ZIP centroid, fallback to data mean
            geo_row = get_con().execute(
                f"SELECT lat, lon FROM read_parquet('{DIM_ZIP_GEO}') WHERE zip = ? LIMIT 1",
                [home_zip],
            ).fetchone()
            if geo_row:
                center_lat, center_lon = float(geo_row[0]), float(geo_row[1])
            else:
                center_lat = float(map_plot["lat"].mean())
                center_lon = float(map_plot["lon"].mean())

            # Tooltip HTML — include distance/near_me_score when available
            if metric_col == "near_me_score":
                tip_html = (
                    "<b>{facility_name}</b><br/>"
                    "{facility_city}, {facility_zip5}<br/>"
                    "Near-me score: <b>{near_me_score}</b><br/>"
                    "Distance: {distance_miles} mi"
                )
            else:
                tip_html = (
                    "<b>{facility_name}</b><br/>"
                    "{facility_city}, {facility_zip5}<br/>"
                    "Cleanliness index: <b>{cleanliness_index}</b>"
                )

            layer = pdk.Layer(
                "ScatterplotLayer",
                data=map_plot,
                get_position=["lon", "lat"],
                get_fill_color="_color",
                get_radius=300,
                pickable=True,
                auto_highlight=True,
                highlight_color=[255, 220, 0, 230],
            )

            deck = pdk.Deck(
                layers=[layer],
                initial_view_state=pdk.ViewState(
                    latitude=center_lat,
                    longitude=center_lon,
                    zoom=11,
                    pitch=0,
                ),
                tooltip={
                    "html": tip_html,
                    "style": {
                        "background": "rgba(0,0,0,0.78)",
                        "color": "white",
                        "fontSize": "13px",
                        "padding": "8px",
                    },
                },
                map_provider="carto",
                map_style=pdk.map_styles.CARTO_LIGHT,
            )

            # Stable key ensures Streamlit re-renders when mode/filters change
            map_key = (
                f"map_{map_mode}_{effective_metric}_{min_inspections}"
                f"_{include_low_data}_{map_n}_{map_search}"
            )
            st.pydeck_chart(deck, key=map_key)

            # Legend
            _legends = {
                "BEST":   "Green — higher cleanliness index = brighter green (top-ranked)",
                "WORST":  "Red — lower cleanliness index = brighter red (bottom-ranked)",
                "SEARCH": "Blue — search results, gradient by cleanliness index",
            }
            st.caption(
                f"**Color:** {_legends[map_mode]}  |  "
                f"Each dot = ZIP centroid (not exact address).  "
                f"Showing {len(map_plot)} facilit{'y' if len(map_plot) == 1 else 'ies'}."
            )

            # ── table + drilldown below the map ────────────────────────────
            st.markdown("**Select a facility below for full details:**")
            table_cols = (
                ["facility_name", "facility_city", "facility_zip5", metric_col, "facility_key"]
                + extra_cols
            )
            show_ranked_table(
                df_map[[c for c in table_cols if c in df_map.columns]],
                "map",
                n_rows=map_n,
            )

# ── About ─────────────────────────────────────────────────────────────────────
with tab_about:
    st.subheader("About This Project")
    st.markdown(
        "**How Clean Is The 626?** tracks restaurant health inspection scores across the "
        "**626 area code** (San Gabriel Valley) using LA County public health data.\n\n"
        "I built it because I eat at these restaurants every week and wanted an honest answer "
        "to a simple question: which ones are actually clean?"
    )

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Cleanliness Index**")
        st.markdown(
            "A composite score (0–100) over a rolling 12-month window "
            "from the most recent inspection date in the dataset.\n\n"
            "| Signal | What It Captures |\n"
            "|--------|------------------|\n"
            "| Inspection scores | Raw health department grades |\n"
            "| Score trend | Improving or declining over time |\n"
            "| Violations per visit | Volume of issues |\n"
            "| Points per visit | Severity-weighted burden |\n"
            "| Severe event rate | Critical / major violation frequency |\n\n"
            "Facilities with fewer than 2 inspections in the window are flagged **low-data**."
        )

        st.markdown("**Near-Me Score**")
        st.markdown(
            "Distance-discounted Cleanliness Index:\n\n"
            "```\nnear_me_score = cleanliness_index / (1 + distance_miles)\n```\n\n"
            "Distances use the Haversine formula between ZIP centroids. "
            "Set your **Home ZIP** in the sidebar to enable this metric."
        )

    with col2:
        st.markdown("**Data Sources**")
        st.markdown(
            "- LA County Environmental Health inspection records (Socrata open data)\n"
            "- Census ZCTA Gazetteer for ZIP centroids\n"
            "- GeoNames fallback for missing centroids\n\n"
            "Raw data is never modified in place."
        )

        st.markdown("**Limitations**")
        st.markdown(
            "- Pasadena, Long Beach, and Vernon are excluded (separate health departments)\n"
            "- Facility locations are ZIP centroid approximations only\n"
            "- Inspection frequency varies across facilities\n"
            "- Low-data facilities are scored but flagged as less reliable"
        )

        st.markdown("**Run the pipeline**")
        st.code(
            "make ingest && make filter_626 && make stage\n"
            "make core && make marts\n"
            "HOME_ZIP=91754 make geo_near_me   # optional\n"
            "streamlit run src/app.py",
            language="bash",
        )
