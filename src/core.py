"""
Phase 4 — Core warehouse model (dim/fct) + facility merge report.

Run from repo root:
    python -m src.core
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

PROCESSED_DIR = Path("data/processed")

STG_INSPECTIONS = PROCESSED_DIR / "stg_inspections.parquet"
STG_VIOLATIONS = PROCESSED_DIR / "stg_violations.parquet"

DIM_FACILITY_OUT = PROCESSED_DIR / "dim_facility.parquet"
FCT_INSPECTION_OUT = PROCESSED_DIR / "fct_inspection.parquet"
FCT_VIOLATION_OUT = PROCESSED_DIR / "fct_violation.parquet"
CORE_META_OUT = PROCESSED_DIR / "core_meta.json"

# Shared facility merge key — evaluated in DuckDB SQL context where these columns exist.
FACILITY_KEY_EXPR = (
    "sha256("
    "lower(trim(facility_name)) || '|' || "
    "lower(trim(facility_address)) || '|' || "
    "facility_zip5 || '|' || "
    "lower(trim(facility_city))"
    ")"
)


def _require_exists(path: Path) -> None:
    if not path.exists():
        raise RuntimeError(f"Missing required file: {path}")


def main() -> None:
    _require_exists(STG_INSPECTIONS)
    _require_exists(STG_VIOLATIONS)

    con = duckdb.connect(database=":memory:")

    stg_insp = STG_INSPECTIONS.resolve().as_posix()
    stg_viol = STG_VIOLATIONS.resolve().as_posix()

    insp_rows: int = con.execute(f"SELECT COUNT(*) FROM read_parquet('{stg_insp}')").fetchone()[0]
    viol_rows: int = con.execute(f"SELECT COUNT(*) FROM read_parquet('{stg_viol}')").fetchone()[0]
    print(f"[in] stg_inspections: {insp_rows:,} rows", flush=True)
    print(f"[in] stg_violations:  {viol_rows:,} rows", flush=True)

    # ---------- dim_facility ----------
    # One row per facility_key; representative fields via any_value().
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_facility AS
        WITH keyed AS (
            SELECT
                {FACILITY_KEY_EXPR} AS facility_key,
                *
            FROM read_parquet('{stg_insp}')
        )
        SELECT
            facility_key,
            any_value(facility_name)        AS facility_name,
            any_value(facility_address)     AS facility_address,
            any_value(facility_city)        AS facility_city,
            any_value(facility_state)       AS facility_state,
            any_value(facility_zip5)        AS facility_zip5,
            any_value(facility_id)          AS facility_id,
            any_value(owner_id)             AS owner_id,
            any_value(owner_name)           AS owner_name,
            any_value(service_code)         AS service_code,
            any_value(service_description)  AS service_description,
            any_value(program_name)         AS program_name,
            min(activity_date)              AS first_seen_date,
            max(activity_date)              AS last_seen_date,
            count(*)                        AS inspection_count
        FROM keyed
        GROUP BY facility_key
    """)

    # ---------- fct_inspection ----------
    # One row per serial_number (already unique in stg_inspections).
    con.execute(f"""
        CREATE OR REPLACE TABLE fct_inspection AS
        SELECT
            serial_number,
            {FACILITY_KEY_EXPR} AS facility_key,
            activity_date,
            score,
            grade,
            facility_id,
            facility_name,
            facility_address,
            facility_city,
            facility_state,
            facility_zip5,
            service_code,
            service_description,
            program_name,
            program_element,
            pe_description,
            program_status,
            owner_id,
            owner_name,
            record_id,
            employee_id
        FROM read_parquet('{stg_insp}')
    """)

    # ---------- fct_violation ----------
    # Join to fct_inspection for facility_key; deterministic violation_key via
    # sha256(serial_number || '|' || row_number within serial_number ordered by
    # violation fields).
    con.execute(f"""
        CREATE OR REPLACE TABLE fct_violation AS
        SELECT
            sha256(
                v.serial_number || '|' ||
                cast(
                    row_number() OVER (
                        PARTITION BY v.serial_number
                        ORDER BY
                            coalesce(v.violation_code, ''),
                            coalesce(v.violation_description, ''),
                            coalesce(cast(v.points AS VARCHAR), '')
                    ) AS VARCHAR
                )
            )                       AS violation_key,
            v.serial_number,
            f.facility_key,
            v.violation_status,
            v.violation_code,
            v.violation_description,
            v.points
        FROM read_parquet('{stg_viol}') v
        JOIN fct_inspection f ON v.serial_number = f.serial_number
    """)

    dim_fac_rows: int = con.execute("SELECT COUNT(*) FROM dim_facility").fetchone()[0]
    fct_insp_rows: int = con.execute("SELECT COUNT(*) FROM fct_inspection").fetchone()[0]
    fct_viol_rows: int = con.execute("SELECT COUNT(*) FROM fct_violation").fetchone()[0]

    # ---------- Hard validations ----------
    fac_key_null: int = con.execute(
        "SELECT COUNT(*) FROM dim_facility WHERE facility_key IS NULL"
    ).fetchone()[0]
    if fac_key_null > 0:
        raise RuntimeError(f"dim_facility: {fac_key_null} null facility_key rows")

    fac_key_dupes: int = con.execute(
        "SELECT COUNT(*) - COUNT(DISTINCT facility_key) FROM dim_facility"
    ).fetchone()[0]
    if fac_key_dupes > 0:
        raise RuntimeError(f"dim_facility: {fac_key_dupes} duplicate facility_key rows")

    sn_dupes: int = con.execute(
        "SELECT COUNT(*) - COUNT(DISTINCT serial_number) FROM fct_inspection"
    ).fetchone()[0]
    if sn_dupes > 0:
        raise RuntimeError(f"fct_inspection: {sn_dupes} duplicate serial_number rows")

    # Orphan check: violations whose serial_number has no matching fct_inspection row.
    orphan_count: int = con.execute(f"""
        SELECT COUNT(*)
        FROM read_parquet('{stg_viol}') v
        WHERE v.serial_number NOT IN (SELECT serial_number FROM fct_inspection)
    """).fetchone()[0]
    if orphan_count > 0:
        raise RuntimeError(
            f"fct_violation: {orphan_count} violations have no matching serial_number in fct_inspection"
        )

    # ---------- Facility merge report ----------
    distinct_facility_id: int = con.execute(f"""
        SELECT COUNT(DISTINCT facility_id)
        FROM read_parquet('{stg_insp}')
        WHERE facility_id IS NOT NULL
    """).fetchone()[0]
    distinct_facility_key: int = dim_fac_rows
    merged_facility_ids: int = distinct_facility_id - distinct_facility_key
    merge_rate: float = (
        merged_facility_ids / distinct_facility_id if distinct_facility_id > 0 else 0.0
    )

    top_merged_rows = con.execute(f"""
        WITH keyed AS (
            SELECT
                {FACILITY_KEY_EXPR} AS facility_key,
                facility_id,
                facility_name,
                facility_address,
                facility_zip5,
                facility_city
            FROM read_parquet('{stg_insp}')
            WHERE facility_id IS NOT NULL
        )
        SELECT
            facility_key,
            any_value(facility_name)    AS facility_name,
            any_value(facility_address) AS facility_address,
            any_value(facility_zip5)    AS facility_zip5,
            any_value(facility_city)    AS facility_city,
            COUNT(DISTINCT facility_id) AS facility_id_count
        FROM keyed
        GROUP BY facility_key
        HAVING COUNT(DISTINCT facility_id) > 1
        ORDER BY facility_id_count DESC
        LIMIT 10
    """).fetchall()

    top_merged = [
        {
            "facility_key": r[0],
            "facility_name": r[1],
            "facility_address": r[2],
            "facility_zip5": r[3],
            "facility_city": r[4],
            "facility_id_count": int(r[5]),
        }
        for r in top_merged_rows
    ]

    # ---------- Write outputs ----------
    con.execute(f"COPY dim_facility   TO '{DIM_FACILITY_OUT.as_posix()}'   (FORMAT PARQUET);")
    con.execute(f"COPY fct_inspection TO '{FCT_INSPECTION_OUT.as_posix()}' (FORMAT PARQUET);")
    con.execute(f"COPY fct_violation  TO '{FCT_VIOLATION_OUT.as_posix()}'  (FORMAT PARQUET);")

    for p in [DIM_FACILITY_OUT, FCT_INSPECTION_OUT, FCT_VIOLATION_OUT]:
        if not p.exists() or p.stat().st_size == 0:
            raise RuntimeError(f"Failed to write output: {p}")

    meta: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "inputs": {
            "stg_inspections_rows": int(insp_rows),
            "stg_violations_rows": int(viol_rows),
        },
        "outputs": {
            "dim_facility_rows": int(dim_fac_rows),
            "fct_inspection_rows": int(fct_insp_rows),
            "fct_violation_rows": int(fct_viol_rows),
        },
        "facility_merge_report": {
            "distinct_facility_id": int(distinct_facility_id),
            "distinct_facility_key": int(distinct_facility_key),
            "merged_facility_ids": int(merged_facility_ids),
            "merge_rate": round(merge_rate, 6),
            "top_merged_facilities": top_merged,
        },
    }
    CORE_META_OUT.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print(f"[out] dim_facility.parquet:   {dim_fac_rows:,} rows", flush=True)
    print(f"[out] fct_inspection.parquet: {fct_insp_rows:,} rows", flush=True)
    print(f"[out] fct_violation.parquet:  {fct_viol_rows:,} rows", flush=True)
    print(
        f"[merge] {distinct_facility_id} facility_ids -> {distinct_facility_key} keys "
        f"({merged_facility_ids} merged, rate={merge_rate:.2%})",
        flush=True,
    )
    print(f"[meta] {CORE_META_OUT} written", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
