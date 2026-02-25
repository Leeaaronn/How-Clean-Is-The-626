"""
Phase 2 — Filter LA County food safety data to 626-area ZIP codes.

Run from repo root:
    python -m src.filter_626
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
SEED_ZIPS_PATH = Path("db/seeds/zip_626.csv")

INSPECTIONS_RAW = RAW_DIR / "inspections_raw.csv"
VIOLATIONS_RAW = RAW_DIR / "violations_raw.csv"

INSPECTIONS_OUT = PROCESSED_DIR / "inspections_626.parquet"
VIOLATIONS_OUT = PROCESSED_DIR / "violations_626.parquet"
COVERAGE_OUT = PROCESSED_DIR / "zip_coverage_626.csv"
META_OUT = PROCESSED_DIR / "process_meta.json"


def _require_exists(path: Path) -> None:
    if not path.exists():
        raise RuntimeError(f"Missing required file: {path}")


def _load_seed_zips(path: Path) -> list[str]:
    _require_exists(path)
    zips: list[str] = []
    # simple CSV read to avoid pandas dependency
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0].strip().lower() != "zip":
        raise RuntimeError(f"Seed file must have single header 'zip': {path}")

    for line in lines[1:]:
        val = line.strip()
        if not val:
            continue
        # keep only 5 digits (preserve leading zeros if any)
        digits = "".join(ch for ch in val if ch.isdigit())
        if len(digits) >= 5:
            zips.append(digits[:5])

    zips = sorted(set(zips))
    if len(zips) == 0:
        raise RuntimeError(f"Seed zip list is empty after parsing: {path}")
    return zips


def _count_rows_csv_fast(path: Path) -> int:
    # count data rows (exclude header)
    with path.open("rb") as f:
        n = 0
        for _ in f:
            n += 1
    return max(0, n - 1)


def main() -> None:
    _require_exists(INSPECTIONS_RAW)
    _require_exists(VIOLATIONS_RAW)
    seed_zips = _load_seed_zips(SEED_ZIPS_PATH)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[seed] {len(seed_zips)} ZIP codes loaded from {SEED_ZIPS_PATH}", flush=True)

    raw_insp_rows = _count_rows_csv_fast(INSPECTIONS_RAW)
    raw_viol_rows = _count_rows_csv_fast(VIOLATIONS_RAW)
    print(f"[raw] inspections: {raw_insp_rows:,} rows", flush=True)
    print(f"[raw] violations:  {raw_viol_rows:,} rows", flush=True)

    con = duckdb.connect(database=":memory:")

    # Read as VARCHAR to prevent conversion errors and make Phase 2 “subset + persist” stable.
    # We intentionally defer typing to Phase 3 (staging).
    insp_path = INSPECTIONS_RAW.resolve().as_posix()
    con.execute(
    f"""
    CREATE OR REPLACE VIEW inspections_raw AS
    SELECT *
    FROM read_csv_auto(
        '{insp_path}',
        header=true,
        all_varchar=true,
        ignore_errors=false
    )
    """
)

    viol_path = VIOLATIONS_RAW.resolve().as_posix()
    con.execute(
    f"""
    CREATE OR REPLACE VIEW violations_raw AS
    SELECT *
    FROM read_csv_auto(
        '{viol_path}',
        header=true,
        all_varchar=true,
        ignore_errors=false
    )
    """
)

    # Validate required columns exist (exact header names in your raw file)
    insp_cols = {r[0] for r in con.execute("DESCRIBE inspections_raw").fetchall()}
    viol_cols = {r[0] for r in con.execute("DESCRIBE violations_raw").fetchall()}

    required_insp = {"FACILITY ZIP", "SERIAL NUMBER"}
    required_viol = {"SERIAL NUMBER"}

    missing_insp = required_insp - insp_cols
    missing_viol = required_viol - viol_cols
    if missing_insp:
        raise RuntimeError(f"Inspections missing required columns: {sorted(missing_insp)}")
    if missing_viol:
        raise RuntimeError(f"Violations missing required columns: {sorted(missing_viol)}")

    # Create seed table in DuckDB
    con.execute("CREATE OR REPLACE TABLE seed_zips(zip VARCHAR)")
    con.executemany("INSERT INTO seed_zips VALUES (?)", [(z,) for z in seed_zips])

    # Normalize ZIP: trim, extract first 5 digits (handles ZIP+4 and whitespace)
    con.execute(
        """
        CREATE OR REPLACE TABLE inspections_626 AS
        SELECT
            *,
            regexp_extract(trim("FACILITY ZIP"), '([0-9]{5})', 1) AS facility_zip5
        FROM inspections_raw
        WHERE regexp_extract(trim("FACILITY ZIP"), '([0-9]{5})', 1) IN (SELECT zip FROM seed_zips)
        """
    )

    insp_out_rows = con.execute("SELECT COUNT(*) FROM inspections_626").fetchone()[0]
    if insp_out_rows == 0:
        raise RuntimeError("Filtered inspections_626 is empty — check seed ZIPs or source data.")

    # Filter violations by SERIAL NUMBER join to filtered inspections
    con.execute(
        """
        CREATE OR REPLACE TABLE violations_626 AS
        SELECT v.*
        FROM violations_raw v
        INNER JOIN (
            SELECT DISTINCT "SERIAL NUMBER" AS serial_number
            FROM inspections_626
            WHERE "SERIAL NUMBER" IS NOT NULL AND trim("SERIAL NUMBER") <> ''
        ) s
        ON v."SERIAL NUMBER" = s.serial_number
        """
    )

    viol_out_rows = con.execute("SELECT COUNT(*) FROM violations_626").fetchone()[0]
    if viol_out_rows == 0:
        raise RuntimeError("Filtered violations_626 is empty — join or source data issue.")

    # Coverage report: violations inherit ZIP from inspections_626
    con.execute(
        f"""
        COPY (
            WITH insp AS (
                SELECT facility_zip5 AS zip, COUNT(*)::BIGINT AS inspection_count
                FROM inspections_626
                GROUP BY 1
            ),
            viol AS (
                SELECT i.facility_zip5 AS zip, COUNT(*)::BIGINT AS violation_count
                FROM violations_626 v
                JOIN inspections_626 i
                  ON v."SERIAL NUMBER" = i."SERIAL NUMBER"
                GROUP BY 1
            )
            SELECT
                COALESCE(insp.zip, viol.zip) AS zip,
                COALESCE(insp.inspection_count, 0) AS inspection_count,
                COALESCE(viol.violation_count, 0) AS violation_count
            FROM insp
            FULL OUTER JOIN viol USING (zip)
            ORDER BY zip
        ) TO '{COVERAGE_OUT.as_posix()}' (HEADER, DELIMITER ',');
        """
    )

    # Write Parquet artifacts (deterministic)
    con.execute(f"COPY inspections_626 TO '{INSPECTIONS_OUT.as_posix()}' (FORMAT PARQUET);")
    con.execute(f"COPY violations_626 TO '{VIOLATIONS_OUT.as_posix()}' (FORMAT PARQUET);")

    # Validate outputs exist and non-empty
    for p in [INSPECTIONS_OUT, VIOLATIONS_OUT, COVERAGE_OUT]:
        if (not p.exists()) or p.stat().st_size == 0:
            raise RuntimeError(f"Failed to write output: {p}")

    meta: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "seed_zip_count": len(seed_zips),
        "inputs": {
            "inspections_raw_rows": raw_insp_rows,
            "violations_raw_rows": raw_viol_rows,
        },
        "outputs": {
            "inspections_626_rows": int(insp_out_rows),
            "violations_626_rows": int(viol_out_rows),
            "inspections_626_file": str(INSPECTIONS_OUT),
            "violations_626_file": str(VIOLATIONS_OUT),
            "zip_coverage_file": str(COVERAGE_OUT),
        },
    }
    META_OUT.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print(f"[out] inspections_626.parquet: {insp_out_rows:,} rows", flush=True)
    print(f"[out] violations_626.parquet:  {viol_out_rows:,} rows", flush=True)
    print(f"[out] zip_coverage_626.csv:     written", flush=True)
    print(f"[meta] {META_OUT} written", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)