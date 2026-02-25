"""
Phase 3 — Staging (stg_*) from 626 parquets.

Run from repo root:
    python -m src.stage
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

PROCESSED_DIR = Path("data/processed")

INSPECTIONS_IN = PROCESSED_DIR / "inspections_626.parquet"
VIOLATIONS_IN = PROCESSED_DIR / "violations_626.parquet"

STG_INSPECTIONS_OUT = PROCESSED_DIR / "stg_inspections.parquet"
STG_VIOLATIONS_OUT = PROCESSED_DIR / "stg_violations.parquet"
STG_META_OUT = PROCESSED_DIR / "stg_meta.json"


def _require_exists(path: Path) -> None:
    if not path.exists():
        raise RuntimeError(f"Missing required file: {path}")


def main() -> None:
    _require_exists(INSPECTIONS_IN)
    _require_exists(VIOLATIONS_IN)

    con = duckdb.connect(database=":memory:")

    insp_path = INSPECTIONS_IN.resolve().as_posix()
    viol_path = VIOLATIONS_IN.resolve().as_posix()

    # Inspect columns to handle "PROGRRAM STATUS" typo safely
    insp_cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{insp_path}')").fetchall()}

    # Input row counts
    insp_in_rows: int = con.execute(f"SELECT COUNT(*) FROM read_parquet('{insp_path}')").fetchone()[0]
    viol_in_rows: int = con.execute(f"SELECT COUNT(*) FROM read_parquet('{viol_path}')").fetchone()[0]

    print(f"[in] inspections_626.parquet: {insp_in_rows:,} rows", flush=True)
    print(f"[in] violations_626.parquet:  {viol_in_rows:,} rows", flush=True)

    # Handle "PROGRRAM STATUS" column-name typo
    if "PROGRRAM STATUS" in insp_cols:
        program_status_expr = "upper(nullif(trim(\"PROGRRAM STATUS\"), ''))"
    elif "PROGRAM STATUS" in insp_cols:
        program_status_expr = "upper(nullif(trim(\"PROGRAM STATUS\"), ''))"
    else:
        program_status_expr = "NULL"

    # ---------- stg_inspections ----------
    con.execute(f"""
        CREATE OR REPLACE TABLE stg_inspections AS
        SELECT
            trim("SERIAL NUMBER")                                          AS serial_number,
            try_strptime(trim("ACTIVITY DATE"), '%m/%d/%Y')::DATE          AS activity_date,
            regexp_extract(trim("FACILITY ZIP"), '([0-9]{{5}})', 1)        AS facility_zip5,
            try_cast(trim("SCORE") AS INTEGER)                             AS score,
            upper(nullif(trim("GRADE"), ''))                               AS grade,
            nullif(trim("FACILITY ID"), '')                                AS facility_id,
            nullif(trim("FACILITY NAME"), '')                              AS facility_name,
            nullif(trim("FACILITY ADDRESS"), '')                           AS facility_address,
            nullif(trim("FACILITY CITY"), '')                              AS facility_city,
            nullif(trim("FACILITY STATE"), '')                             AS facility_state,
            nullif(trim("SERVICE CODE"), '')                               AS service_code,
            nullif(trim("SERVICE DESCRIPTION"), '')                        AS service_description,
            nullif(trim("PROGRAM NAME"), '')                               AS program_name,
            nullif(trim("PROGRAM ELEMENT"), '')                            AS program_element,
            nullif(trim("PE DESCRIPTION"), '')                             AS pe_description,
            {program_status_expr}                                          AS program_status,
            nullif(trim("OWNER ID"), '')                                   AS owner_id,
            nullif(trim("OWNER NAME"), '')                                 AS owner_name,
            nullif(trim("RECORD ID"), '')                                  AS record_id,
            nullif(trim("EMPLOYEE ID"), '')                                AS employee_id
        FROM read_parquet('{insp_path}')
    """)

    # ---------- stg_violations ----------
    con.execute(f"""
        CREATE OR REPLACE TABLE stg_violations AS
        SELECT
            trim("SERIAL NUMBER")                                          AS serial_number,
            upper(nullif(trim("VIOLATION STATUS"), ''))                    AS violation_status,
            nullif(trim("VIOLATION CODE"), '')                             AS violation_code,
            nullif(trim("VIOLATION DESCRIPTION"), '')                      AS violation_description,
            try_cast(trim("POINTS") AS DOUBLE)                             AS points
        FROM read_parquet('{viol_path}')
    """)

    stg_insp_rows: int = con.execute("SELECT COUNT(*) FROM stg_inspections").fetchone()[0]
    stg_viol_rows: int = con.execute("SELECT COUNT(*) FROM stg_violations").fetchone()[0]

    # ---------- Hard validations ----------
    if stg_insp_rows == 0:
        raise RuntimeError("stg_inspections is empty")
    if stg_viol_rows == 0:
        raise RuntimeError("stg_violations is empty")

    insp_sn_null: int = con.execute(
        "SELECT COUNT(*) FROM stg_inspections WHERE serial_number IS NULL OR serial_number = ''"
    ).fetchone()[0]
    if insp_sn_null > 0:
        raise RuntimeError(f"stg_inspections: {insp_sn_null} rows with null/empty serial_number")

    viol_sn_null: int = con.execute(
        "SELECT COUNT(*) FROM stg_violations WHERE serial_number IS NULL OR serial_number = ''"
    ).fetchone()[0]
    if viol_sn_null > 0:
        raise RuntimeError(f"stg_violations: {viol_sn_null} rows with null/empty serial_number")

    insp_date_null: int = con.execute(
        "SELECT COUNT(*) FROM stg_inspections WHERE activity_date IS NULL"
    ).fetchone()[0]
    if insp_date_null > 0:
        raise RuntimeError(f"stg_inspections: {insp_date_null} rows with null activity_date")

    # facility_zip5 must be exactly 5 digits; regexp_extract returns '' on no match
    insp_zip_bad: int = con.execute(
        "SELECT COUNT(*) FROM stg_inspections WHERE facility_zip5 IS NULL OR length(facility_zip5) <> 5"
    ).fetchone()[0]
    if insp_zip_bad > 0:
        raise RuntimeError(f"stg_inspections: {insp_zip_bad} rows where facility_zip5 is not 5 digits")

    # ---------- Informational null counts ----------
    insp_score_null: int = con.execute(
        "SELECT COUNT(*) FROM stg_inspections WHERE score IS NULL"
    ).fetchone()[0]
    insp_grade_null: int = con.execute(
        "SELECT COUNT(*) FROM stg_inspections WHERE grade IS NULL"
    ).fetchone()[0]
    viol_code_null: int = con.execute(
        "SELECT COUNT(*) FROM stg_violations WHERE violation_code IS NULL"
    ).fetchone()[0]
    viol_points_null: int = con.execute(
        "SELECT COUNT(*) FROM stg_violations WHERE points IS NULL"
    ).fetchone()[0]

    # Scores outside [0, 100] — report only, do not fail
    score_out_of_range: int = con.execute(
        "SELECT COUNT(*) FROM stg_inspections WHERE score IS NOT NULL AND (score < 0 OR score > 100)"
    ).fetchone()[0]

    # ---------- Write outputs ----------
    con.execute(f"COPY stg_inspections TO '{STG_INSPECTIONS_OUT.as_posix()}' (FORMAT PARQUET);")
    con.execute(f"COPY stg_violations TO '{STG_VIOLATIONS_OUT.as_posix()}' (FORMAT PARQUET);")

    for p in [STG_INSPECTIONS_OUT, STG_VIOLATIONS_OUT]:
        if not p.exists() or p.stat().st_size == 0:
            raise RuntimeError(f"Failed to write output: {p}")

    meta: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "inputs": {
            "inspections_626_rows": int(insp_in_rows),
            "violations_626_rows": int(viol_in_rows),
        },
        "outputs": {
            "stg_inspections_rows": int(stg_insp_rows),
            "stg_violations_rows": int(stg_viol_rows),
        },
        "null_counts": {
            "stg_inspections": {
                "serial_number": int(insp_sn_null),
                "activity_date": int(insp_date_null),
                "score": int(insp_score_null),
                "grade": int(insp_grade_null),
            },
            "stg_violations": {
                "serial_number": int(viol_sn_null),
                "violation_code": int(viol_code_null),
                "points": int(viol_points_null),
            },
        },
        "warnings": {
            "scores_outside_0_100": int(score_out_of_range),
        },
    }
    STG_META_OUT.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print(f"[out] stg_inspections.parquet: {stg_insp_rows:,} rows", flush=True)
    print(f"[out] stg_violations.parquet:  {stg_viol_rows:,} rows", flush=True)
    print(f"[warn] scores outside [0,100]: {score_out_of_range}", flush=True)
    print(f"[meta] {STG_META_OUT} written", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
