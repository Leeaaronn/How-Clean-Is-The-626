"""
Phase 6 — Data quality tests.

Run from repo root:
    python -m pytest tests/test_quality.py -v
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

PROCESSED = Path("data/processed")

DIM_FACILITY = (PROCESSED / "dim_facility.parquet").as_posix()
FCT_INSPECTION = (PROCESSED / "fct_inspection.parquet").as_posix()
FCT_VIOLATION = (PROCESSED / "fct_violation.parquet").as_posix()
MART_FACILITY_HEALTH = (PROCESSED / "mart_facility_health.parquet").as_posix()
CORE_META = PROCESSED / "core_meta.json"


@pytest.fixture(scope="session")
def con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(database=":memory:")


@pytest.fixture(scope="session")
def core_meta() -> dict:
    return json.loads(CORE_META.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# mart_facility_health
# ---------------------------------------------------------------------------

def test_cleanliness_index_in_range(con):
    bad = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{MART_FACILITY_HEALTH}') "
        f"WHERE cleanliness_index < 0 OR cleanliness_index > 100"
    ).fetchone()[0]
    assert bad == 0, f"{bad} rows have cleanliness_index outside [0, 100]"


def test_no_null_facility_key_in_mart(con):
    null_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{MART_FACILITY_HEALTH}') "
        f"WHERE facility_key IS NULL"
    ).fetchone()[0]
    assert null_count == 0, f"{null_count} null facility_key rows in mart_facility_health"


def test_p90_cleanliness_gt_p50(con):
    p50, p90 = con.execute(
        f"SELECT quantile_cont(cleanliness_index, 0.50), "
        f"       quantile_cont(cleanliness_index, 0.90) "
        f"FROM read_parquet('{MART_FACILITY_HEALTH}')"
    ).fetchone()
    assert p90 > p50, f"p90={p90} not greater than p50={p50}"


def test_cleanliness_100_share_le_5pct(con):
    total, perfect = con.execute(
        f"SELECT COUNT(*), "
        f"       COUNT(*) FILTER (WHERE cleanliness_index = 100) "
        f"FROM read_parquet('{MART_FACILITY_HEALTH}')"
    ).fetchone()
    share = perfect / total if total > 0 else 0.0
    assert share <= 0.05, (
        f"{perfect}/{total} ({share:.1%}) facilities have cleanliness_index=100; "
        f"expected <= 5%"
    )


# ---------------------------------------------------------------------------
# dim_facility
# ---------------------------------------------------------------------------

def test_dim_facility_key_unique(con):
    dupes = con.execute(
        f"SELECT COUNT(*) - COUNT(DISTINCT facility_key) "
        f"FROM read_parquet('{DIM_FACILITY}')"
    ).fetchone()[0]
    assert dupes == 0, f"{dupes} duplicate facility_key rows in dim_facility"


# ---------------------------------------------------------------------------
# fct_inspection
# ---------------------------------------------------------------------------

def test_fct_inspection_serial_number_unique(con):
    dupes = con.execute(
        f"SELECT COUNT(*) - COUNT(DISTINCT serial_number) "
        f"FROM read_parquet('{FCT_INSPECTION}')"
    ).fetchone()[0]
    assert dupes == 0, f"{dupes} duplicate serial_number rows in fct_inspection"


# ---------------------------------------------------------------------------
# fct_violation
# ---------------------------------------------------------------------------

def test_no_orphan_violations(con):
    orphans = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{FCT_VIOLATION}') v "
        f"WHERE v.serial_number NOT IN "
        f"(SELECT serial_number FROM read_parquet('{FCT_INSPECTION}'))"
    ).fetchone()[0]
    assert orphans == 0, f"{orphans} orphan violations (no matching fct_inspection row)"


# ---------------------------------------------------------------------------
# core_meta.json
# ---------------------------------------------------------------------------

def test_merge_rate_lt_5pct(core_meta):
    rate = core_meta["facility_merge_report"]["merge_rate"]
    assert rate < 0.05, (
        f"facility merge_rate={rate:.4f} >= 0.05; "
        f"more IDs are merging than expected"
    )
