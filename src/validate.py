"""
Phase 6 — Schema contract + data quality validation runner.

Run from repo root:
    python -m src.validate

Exits non-zero if any check fails.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb

CONTRACTS_DIR = Path("contracts")
PROCESSED_DIR = Path("data/processed")


# ---------------------------------------------------------------------------
# Contract validation
# ---------------------------------------------------------------------------

def _load_contracts() -> list[dict]:
    contracts = []
    for p in sorted(CONTRACTS_DIR.glob("*.json")):
        try:
            contracts.append(json.loads(p.read_text(encoding="utf-8")))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid JSON in contract file: {p}") from exc
    return contracts

def _check_schema(con: duckdb.DuckDBPyConnection, contract: dict) -> list[str]:
    """Return list of failure messages for one contract."""
    failures: list[str] = []
    path = contract["path"]

    if not Path(path).exists():
        return [f"[{contract['table']}] file not found: {path}"]

    actual: dict[str, str] = {
        r[0]: r[1]
        for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
    }

    for col_spec in contract["columns"]:
        name = col_spec["name"]
        expected_type_prefix = col_spec["type"].upper()
        not_null = col_spec.get("not_null", False)

        # Column presence
        if name not in actual:
            failures.append(
                f"[{contract['table']}] missing column: {name}"
            )
            continue

        # Type check: actual type must start with the expected prefix
        # (e.g. "DECIMAL" matches "DECIMAL(38,1)")
        actual_type = actual[name].upper()
        if not actual_type.startswith(expected_type_prefix):
            failures.append(
                f"[{contract['table']}] column '{name}': "
                f"expected type starting with {expected_type_prefix}, got {actual_type}"
            )

        # Not-null check
        if not_null:
            null_count: int = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{path}') "
                f"WHERE \"{name}\" IS NULL"
            ).fetchone()[0]
            if null_count > 0:
                failures.append(
                    f"[{contract['table']}] column '{name}': "
                    f"{null_count:,} unexpected NULL rows (contract: not_null=true)"
                )

    return failures


# ---------------------------------------------------------------------------
# Quality checks
# ---------------------------------------------------------------------------

def _quality_checks(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Run cross-table quality checks. Return list of failure messages."""
    failures: list[str] = []

    paths = {
        "dim_facility":         (PROCESSED_DIR / "dim_facility.parquet").as_posix(),
        "fct_inspection":       (PROCESSED_DIR / "fct_inspection.parquet").as_posix(),
        "fct_violation":        (PROCESSED_DIR / "fct_violation.parquet").as_posix(),
        "mart_facility_health": (PROCESSED_DIR / "mart_facility_health.parquet").as_posix(),
        "dim_zip_geo":          (PROCESSED_DIR / "dim_zip_geo.parquet").as_posix(),
        "mart_near_me":         (PROCESSED_DIR / "mart_near_me.parquet").as_posix(),
    }

    def check(label: str, sql: str, expect_zero: bool = True) -> None:
        result = con.execute(sql).fetchone()[0]
        if expect_zero and result != 0:
            failures.append(f"{label}: got {result:,}, expected 0")
        elif not expect_zero and not result:
            failures.append(f"{label}: returned no result")

    # Row counts > 0
    for name, path in paths.items():
        n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
        if n == 0:
            failures.append(f"[{name}] empty — 0 rows")

    p = paths

    # cleanliness_index within [0, 100]
    check(
        "[mart_facility_health] cleanliness_index out of range",
        f"SELECT COUNT(*) FROM read_parquet('{p['mart_facility_health']}') "
        f"WHERE cleanliness_index < 0 OR cleanliness_index > 100",
    )

    # no null facility_key in mart
    check(
        "[mart_facility_health] null facility_key",
        f"SELECT COUNT(*) FROM read_parquet('{p['mart_facility_health']}') "
        f"WHERE facility_key IS NULL",
    )

    # dim_facility: facility_key unique
    check(
        "[dim_facility] duplicate facility_key",
        f"SELECT COUNT(*) - COUNT(DISTINCT facility_key) "
        f"FROM read_parquet('{p['dim_facility']}')",
    )

    # fct_inspection: serial_number unique
    check(
        "[fct_inspection] duplicate serial_number",
        f"SELECT COUNT(*) - COUNT(DISTINCT serial_number) "
        f"FROM read_parquet('{p['fct_inspection']}')",
    )

    # no orphan violations
    check(
        "[fct_violation] orphan serial_number not in fct_inspection",
        f"SELECT COUNT(*) FROM read_parquet('{p['fct_violation']}') v "
        f"WHERE v.serial_number NOT IN "
        f"(SELECT serial_number FROM read_parquet('{p['fct_inspection']}'))",
    )

    # stg_inspections: serial_number non-empty
    stg_insp = (PROCESSED_DIR / "stg_inspections.parquet").as_posix()
    check(
        "[stg_inspections] null/empty serial_number",
        f"SELECT COUNT(*) FROM read_parquet('{stg_insp}') "
        f"WHERE serial_number IS NULL OR serial_number = ''",
    )

    # stg_inspections: activity_date not null
    check(
        "[stg_inspections] null activity_date",
        f"SELECT COUNT(*) FROM read_parquet('{stg_insp}') "
        f"WHERE activity_date IS NULL",
    )

    # stg_inspections: facility_zip5 is 5 digits
    check(
        "[stg_inspections] invalid facility_zip5",
        f"SELECT COUNT(*) FROM read_parquet('{stg_insp}') "
        f"WHERE facility_zip5 IS NULL OR length(facility_zip5) <> 5",
    )

    # ------------------------------------------------------------------
    # dim_zip_geo
    # ------------------------------------------------------------------

    # lat/lon plausible range for the 626 area (Southern California)
    check(
        "[dim_zip_geo] lat/lon outside Southern California range",
        f"SELECT COUNT(*) FROM read_parquet('{p['dim_zip_geo']}') "
        f"WHERE lat NOT BETWEEN 30 AND 40 OR lon NOT BETWEEN -125 AND -110",
    )

    # ------------------------------------------------------------------
    # mart_near_me
    # ------------------------------------------------------------------

    # distance_miles non-negative
    check(
        "[mart_near_me] distance_miles < 0",
        f"SELECT COUNT(*) FROM read_parquet('{p['mart_near_me']}') "
        f"WHERE distance_miles < 0",
    )

    # near_me_score within [0, 100]
    check(
        "[mart_near_me] near_me_score out of range [0, 100]",
        f"SELECT COUNT(*) FROM read_parquet('{p['mart_near_me']}') "
        f"WHERE near_me_score < 0 OR near_me_score > 100",
    )

    # near_me_score <= cleanliness_index (proximity factor is always <= 1)
    check(
        "[mart_near_me] near_me_score > cleanliness_index",
        f"SELECT COUNT(*) FROM read_parquet('{p['mart_near_me']}') "
        f"WHERE near_me_score > cleanliness_index",
    )

    # row count must equal mart_facility_health (JOIN must not drop facilities)
    near_me_rows = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{p['mart_near_me']}')"
    ).fetchone()[0]
    health_rows = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{p['mart_facility_health']}')"
    ).fetchone()[0]
    if near_me_rows != health_rows:
        failures.append(
            f"[mart_near_me] row count mismatch: "
            f"mart_near_me={near_me_rows:,}, mart_facility_health={health_rows:,}"
        )

    return failures


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    contracts = _load_contracts()
    if not contracts:
        print("ERROR: no contract files found in contracts/", file=sys.stderr)
        sys.exit(1)

    con = duckdb.connect(database=":memory:")
    all_failures: list[str] = []

    print(f"Validating {len(contracts)} contracts ...", flush=True)
    for contract in contracts:
        failures = _check_schema(con, contract)
        if failures:
            for f in failures:
                print(f"  FAIL  {f}", flush=True)
            all_failures.extend(failures)
        else:
            print(f"  OK    [{contract['table']}]", flush=True)

    print()
    print("Running quality checks ...", flush=True)
    q_failures = _quality_checks(con)
    if q_failures:
        for f in q_failures:
            print(f"  FAIL  {f}", flush=True)
        all_failures.extend(q_failures)
    else:
        print("  OK    all quality checks passed", flush=True)

    print()
    if all_failures:
        print(f"RESULT: {len(all_failures)} failure(s).", file=sys.stderr)
        sys.exit(1)
    else:
        print(f"RESULT: all checks passed.")


if __name__ == "__main__":
    main()
