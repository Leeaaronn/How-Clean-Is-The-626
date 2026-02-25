"""
Phase 5 — Mart layer + cleanliness index.

Run from repo root:
    python -m src.marts

Reference date = max(activity_date) in fct_inspection so the mart is
reproducible regardless of when it is run.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

PROCESSED_DIR = Path("data/processed")

DIM_FACILITY = PROCESSED_DIR / "dim_facility.parquet"
FCT_INSPECTION = PROCESSED_DIR / "fct_inspection.parquet"
FCT_VIOLATION = PROCESSED_DIR / "fct_violation.parquet"

MART_FACILITY_HEALTH_OUT = PROCESSED_DIR / "mart_facility_health.parquet"
MART_ZIP_HEALTH_OUT = PROCESSED_DIR / "mart_zip_health.parquet"
MART_REPEAT_OFFENDERS_OUT = PROCESSED_DIR / "mart_repeat_offenders.parquet"
MARTS_META_OUT = PROCESSED_DIR / "marts_meta.json"


def _require_exists(path: Path) -> None:
    if not path.exists():
        raise RuntimeError(f"Missing required file: {path}")


def main() -> None:
    for p in [DIM_FACILITY, FCT_INSPECTION, FCT_VIOLATION]:
        _require_exists(p)

    con = duckdb.connect(database=":memory:")

    dim_fac = DIM_FACILITY.resolve().as_posix()
    fct_insp = FCT_INSPECTION.resolve().as_posix()
    fct_viol = FCT_VIOLATION.resolve().as_posix()

    insp_rows: int = con.execute(f"SELECT COUNT(*) FROM read_parquet('{fct_insp}')").fetchone()[0]
    viol_rows: int = con.execute(f"SELECT COUNT(*) FROM read_parquet('{fct_viol}')").fetchone()[0]
    fac_rows: int = con.execute(f"SELECT COUNT(*) FROM read_parquet('{dim_fac}')").fetchone()[0]
    print(f"[in] dim_facility:    {fac_rows:,} rows", flush=True)
    print(f"[in] fct_inspection:  {insp_rows:,} rows", flush=True)
    print(f"[in] fct_violation:   {viol_rows:,} rows", flush=True)

    # ---------- mart_facility_health ----------
    # Reference date = max activity_date in dataset (reproducible).
    # Window: 12 months if >=2 inspections, else 24 months.
    # Cleanliness index:
    #   ScoreTrend    = weighted-avg score (0-90d:1.0, 91-180d:0.8, 181-365d:0.6, 366-730d:0.3)
    #   ViolationScore = 100 - 12*(viol/insp) - 8*(pts/insp), clamp 0..100
    #   EventScore    = 100 - 15*(bad_events where score<90), clamp 0..100
    #   CleanlinessIndex = 0.65*ScoreTrend + 0.25*ViolationScore + 0.10*EventScore, clamp 0..100
    con.execute(f"""
        CREATE OR REPLACE TABLE mart_facility_health AS
        WITH
        ref AS (
            SELECT MAX(activity_date) AS ref_date
            FROM read_parquet('{fct_insp}')
        ),
        -- Per-facility inspection counts in 12mo and 24mo to pick window
        window_sel AS (
            SELECT
                i.facility_key,
                r.ref_date,
                COUNT(*) FILTER (
                    WHERE i.activity_date >= r.ref_date - INTERVAL '365 days'
                )                                                           AS insp_12mo,
                COUNT(*) FILTER (
                    WHERE i.activity_date >= r.ref_date - INTERVAL '730 days'
                )                                                           AS insp_24mo,
                CASE
                    WHEN COUNT(*) FILTER (
                        WHERE i.activity_date >= r.ref_date - INTERVAL '365 days'
                    ) >= 2
                    THEN (r.ref_date - INTERVAL '365 days')::DATE
                    ELSE (r.ref_date - INTERVAL '730 days')::DATE
                END                                                         AS window_start
            FROM read_parquet('{fct_insp}') i
            CROSS JOIN ref r
            GROUP BY i.facility_key, r.ref_date
        ),
        -- Inspections within chosen window, annotated with recency weights
        insp_w AS (
            SELECT
                i.serial_number,
                i.facility_key,
                i.activity_date,
                i.score,
                i.grade,
                i.facility_zip5,
                w.ref_date,
                w.insp_12mo,
                w.insp_24mo,
                CASE
                    WHEN i.activity_date >= w.ref_date - INTERVAL '90 days'  THEN 1.0
                    WHEN i.activity_date >= w.ref_date - INTERVAL '180 days' THEN 0.8
                    WHEN i.activity_date >= w.ref_date - INTERVAL '365 days' THEN 0.6
                    ELSE 0.3
                END                                                         AS recency_weight,
                CASE WHEN i.score < 90 THEN 1 ELSE 0 END                   AS is_bad_event
            FROM read_parquet('{fct_insp}') i
            JOIN window_sel w ON i.facility_key = w.facility_key
            WHERE i.activity_date >= w.window_start
        ),
        -- Violations aggregated to facility within same window
        viol_w AS (
            SELECT
                v.facility_key,
                COUNT(*)                    AS violation_count,
                COALESCE(SUM(v.points), 0.0) AS total_points
            FROM read_parquet('{fct_viol}') v
            WHERE v.serial_number IN (SELECT serial_number FROM insp_w)
            GROUP BY v.facility_key
        ),
        -- Per-facility aggregates over window
        fac_metrics AS (
            SELECT
                iw.facility_key,
                any_value(iw.ref_date)                                      AS ref_date,
                any_value(iw.insp_12mo)                                     AS insp_12mo,
                any_value(iw.insp_24mo)                                     AS insp_24mo,
                COUNT(*)                                                    AS insp_in_window,
                SUM(iw.score * iw.recency_weight)
                    / NULLIF(SUM(iw.recency_weight), 0)                     AS score_trend,
                SUM(iw.is_bad_event)                                        AS bad_event_count,
                MAX(iw.activity_date)                                       AS latest_activity_date,
                arg_max(iw.score, iw.activity_date)                         AS latest_score,
                arg_max(iw.grade, iw.activity_date)                         AS latest_grade,
                any_value(iw.facility_zip5)                                 AS facility_zip5
            FROM insp_w iw
            GROUP BY iw.facility_key
        ),
        -- Combine metrics and compute sub-scores
        score_calc AS (
            SELECT
                fm.facility_key,
                fm.ref_date,
                fm.insp_12mo,
                fm.insp_24mo,
                fm.insp_in_window,
                fm.score_trend,
                fm.bad_event_count,
                fm.latest_activity_date,
                fm.latest_score,
                fm.latest_grade,
                fm.facility_zip5,
                COALESCE(vw.violation_count, 0)::DOUBLE                     AS violation_count,
                COALESCE(vw.total_points, 0.0)                              AS total_points,
                COALESCE(vw.violation_count, 0)::DOUBLE / fm.insp_in_window AS violations_per_inspection,
                COALESCE(vw.total_points, 0.0)        / fm.insp_in_window   AS points_per_inspection,
                GREATEST(0.0, LEAST(100.0,
                    100.0
                    - 12.0 * (COALESCE(vw.violation_count, 0)::DOUBLE / fm.insp_in_window)
                    - 8.0  * (COALESCE(vw.total_points,   0.0)        / fm.insp_in_window)
                ))                                                          AS violation_score,
                GREATEST(0.0, LEAST(100.0,
                    100.0 - 15.0 * fm.bad_event_count
                ))                                                          AS event_score,
                fm.insp_24mo < 2                                            AS low_data_flag
            FROM fac_metrics fm
            LEFT JOIN viol_w vw ON fm.facility_key = vw.facility_key
        )
        SELECT
            sc.facility_key,
            d.facility_name,
            d.facility_address,
            d.facility_city,
            d.facility_state,
            sc.facility_zip5,
            d.facility_id,
            d.owner_name,
            sc.latest_activity_date,
            sc.latest_score,
            sc.latest_grade,
            sc.insp_12mo                AS inspections_12mo,
            sc.insp_24mo                AS inspections_24mo,
            sc.insp_in_window           AS inspections_in_window,
            sc.bad_event_count,
            sc.violation_count,
            sc.total_points,
            sc.violations_per_inspection,
            sc.points_per_inspection,
            sc.score_trend,
            sc.violation_score,
            sc.event_score,
            GREATEST(0.0, LEAST(100.0,
                0.65 * sc.score_trend
                + 0.25 * sc.violation_score
                + 0.10 * sc.event_score
            ))                          AS cleanliness_index,
            sc.low_data_flag
        FROM score_calc sc
        JOIN read_parquet('{dim_fac}') d ON sc.facility_key = d.facility_key
    """)

    # ---------- mart_zip_health ----------
    con.execute("""
        CREATE OR REPLACE TABLE mart_zip_health AS
        SELECT
            facility_zip5,
            COUNT(*)                                                            AS facility_count,
            ROUND(AVG(cleanliness_index), 2)                                   AS avg_cleanliness_index,
            ROUND(MIN(cleanliness_index), 2)                                   AS min_cleanliness_index,
            ROUND(MAX(cleanliness_index), 2)                                   AS max_cleanliness_index,
            -- Grade shares from latest inspection per facility
            ROUND(100.0 * COUNT(*) FILTER (WHERE latest_grade = 'A') / COUNT(*), 1) AS grade_a_pct,
            ROUND(100.0 * COUNT(*) FILTER (WHERE latest_grade = 'B') / COUNT(*), 1) AS grade_b_pct,
            ROUND(100.0 * COUNT(*) FILTER (WHERE latest_grade = 'C') / COUNT(*), 1) AS grade_c_pct,
            ROUND(100.0 * COUNT(*) FILTER (WHERE latest_grade IS NULL) / COUNT(*), 1) AS grade_null_pct,
            COUNT(*) FILTER (WHERE cleanliness_index >= 90)                    AS excellent_count,
            COUNT(*) FILTER (WHERE cleanliness_index >= 70
                               AND cleanliness_index < 90)                     AS good_count,
            COUNT(*) FILTER (WHERE cleanliness_index < 70)                     AS poor_count
        FROM mart_facility_health
        GROUP BY facility_zip5
        ORDER BY avg_cleanliness_index DESC
    """)

    # ---------- mart_repeat_offenders ----------
    con.execute("""
        CREATE OR REPLACE TABLE mart_repeat_offenders AS
        SELECT *
        FROM mart_facility_health
        WHERE bad_event_count >= 2 OR cleanliness_index < 70
        ORDER BY cleanliness_index ASC
    """)

    mfh_rows: int = con.execute("SELECT COUNT(*) FROM mart_facility_health").fetchone()[0]
    mzh_rows: int = con.execute("SELECT COUNT(*) FROM mart_zip_health").fetchone()[0]
    mro_rows: int = con.execute("SELECT COUNT(*) FROM mart_repeat_offenders").fetchone()[0]

    # ---------- Validations ----------
    if mfh_rows == 0:
        raise RuntimeError("mart_facility_health is empty")
    if mzh_rows == 0:
        raise RuntimeError("mart_zip_health is empty")
    if mro_rows == 0:
        raise RuntimeError("mart_repeat_offenders is empty")

    fk_null: int = con.execute(
        "SELECT COUNT(*) FROM mart_facility_health WHERE facility_key IS NULL"
    ).fetchone()[0]
    if fk_null > 0:
        raise RuntimeError(f"mart_facility_health: {fk_null} null facility_key rows")

    ci_bad: int = con.execute(
        "SELECT COUNT(*) FROM mart_facility_health "
        "WHERE cleanliness_index IS NULL OR cleanliness_index < 0 OR cleanliness_index > 100"
    ).fetchone()[0]
    if ci_bad > 0:
        raise RuntimeError(f"mart_facility_health: {ci_bad} rows with out-of-range cleanliness_index")

    # Percentile summary
    stats = con.execute("""
        SELECT
            ROUND(MIN(cleanliness_index), 2)                    AS ci_min,
            ROUND(quantile_cont(cleanliness_index, 0.50), 2)    AS ci_p50,
            ROUND(quantile_cont(cleanliness_index, 0.90), 2)    AS ci_p90,
            ROUND(MAX(cleanliness_index), 2)                    AS ci_max
        FROM mart_facility_health
    """).fetchone()
    ci_min, ci_p50, ci_p90, ci_max = stats

    # ---------- Write outputs ----------
    con.execute(
        f"COPY mart_facility_health  TO '{MART_FACILITY_HEALTH_OUT.as_posix()}' (FORMAT PARQUET);"
    )
    con.execute(
        f"COPY mart_zip_health       TO '{MART_ZIP_HEALTH_OUT.as_posix()}'      (FORMAT PARQUET);"
    )
    con.execute(
        f"COPY mart_repeat_offenders TO '{MART_REPEAT_OFFENDERS_OUT.as_posix()}' (FORMAT PARQUET);"
    )

    for p in [MART_FACILITY_HEALTH_OUT, MART_ZIP_HEALTH_OUT, MART_REPEAT_OFFENDERS_OUT]:
        if not p.exists() or p.stat().st_size == 0:
            raise RuntimeError(f"Failed to write output: {p}")

    # Facility health distribution for meta
    dist = con.execute("""
        SELECT
            COUNT(*) FILTER (WHERE cleanliness_index >= 90)             AS excellent,
            COUNT(*) FILTER (WHERE cleanliness_index >= 70 AND cleanliness_index < 90) AS good,
            COUNT(*) FILTER (WHERE cleanliness_index < 70)              AS poor,
            COUNT(*) FILTER (WHERE low_data_flag = true)                AS low_data
        FROM mart_facility_health
    """).fetchone()

    meta: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "inputs": {
            "dim_facility_rows": int(fac_rows),
            "fct_inspection_rows": int(insp_rows),
            "fct_violation_rows": int(viol_rows),
        },
        "outputs": {
            "mart_facility_health_rows": int(mfh_rows),
            "mart_zip_health_rows": int(mzh_rows),
            "mart_repeat_offenders_rows": int(mro_rows),
        },
        "cleanliness_index_stats": {
            "min": float(ci_min),
            "p50": float(ci_p50),
            "p90": float(ci_p90),
            "max": float(ci_max),
        },
        "facility_distribution": {
            "excellent_ge90": int(dist[0]),
            "good_70_89": int(dist[1]),
            "poor_lt70": int(dist[2]),
            "low_data_flag": int(dist[3]),
        },
    }
    MARTS_META_OUT.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print(f"[out] mart_facility_health.parquet:  {mfh_rows:,} rows", flush=True)
    print(f"[out] mart_zip_health.parquet:        {mzh_rows:,} rows", flush=True)
    print(f"[out] mart_repeat_offenders.parquet:  {mro_rows:,} rows", flush=True)
    print(
        f"[ci]  min={ci_min}  p50={ci_p50}  p90={ci_p90}  max={ci_max}",
        flush=True,
    )
    print(
        f"[dist] excellent(>=90)={dist[0]}  good(70-89)={dist[1]}"
        f"  poor(<70)={dist[2]}  low_data={dist[3]}",
        flush=True,
    )
    print(f"[meta] {MARTS_META_OUT} written", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
