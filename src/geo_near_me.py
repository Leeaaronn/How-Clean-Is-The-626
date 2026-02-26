"""
Phase 7B: Near-me geo mart using ZIP centroids.

Inputs:
  data/processed/mart_facility_health.parquet
  data/processed/dim_facility.parquet
  db/seeds/zip_centroids_626.csv

Outputs:
  data/processed/dim_zip_geo.parquet
  data/processed/mart_near_me.parquet
  data/processed/geo_meta.json

Run from repo root:
  python -m src.geo_near_me

HOME_ZIP env var controls the reference location (default: 91754).
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import duckdb

MART_HEALTH = Path("data/processed/mart_facility_health.parquet")
DIM_FACILITY = Path("data/processed/dim_facility.parquet")
ZIP_CENTROIDS = Path("db/seeds/zip_centroids_626.csv")

OUT_ZIP_GEO = Path("data/processed/dim_zip_geo.parquet")
OUT_NEAR_ME = Path("data/processed/mart_near_me.parquet")
OUT_META = Path("data/processed/geo_meta.json")

DEFAULT_HOME_ZIP = "91754"
EARTH_RADIUS_MILES = 3958.8


def main() -> None:
    home_zip = os.environ.get("HOME_ZIP", DEFAULT_HOME_ZIP).strip()

    for p in (MART_HEALTH, DIM_FACILITY, ZIP_CENTROIDS):
        if not p.exists():
            raise RuntimeError(f"Missing input: {p}")

    con = duckdb.connect()

    # ------------------------------------------------------------------ #
    # 1. dim_zip_geo — load CSV, cast to DOUBLE, persist                  #
    # ------------------------------------------------------------------ #
    con.execute(f"""
        CREATE TABLE dim_zip_geo AS
        SELECT
            CAST(zip AS VARCHAR) AS zip,
            CAST(lat AS DOUBLE)  AS lat,
            CAST(lon AS DOUBLE)  AS lon
        FROM read_csv_auto('{ZIP_CENTROIDS.as_posix()}', header = true)
    """)

    null_coords = con.execute(
        "SELECT COUNT(*) FROM dim_zip_geo WHERE lat IS NULL OR lon IS NULL"
    ).fetchone()[0]
    if null_coords > 0:
        raise RuntimeError(f"dim_zip_geo: {null_coords} rows with null lat/lon")

    zip_geo_rows = con.execute("SELECT COUNT(*) FROM dim_zip_geo").fetchone()[0]
    print(f"dim_zip_geo:   {zip_geo_rows} rows")

    OUT_ZIP_GEO.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY dim_zip_geo TO '{OUT_ZIP_GEO.as_posix()}' (FORMAT PARQUET)")
    print(f"[ok] wrote {OUT_ZIP_GEO} ({zip_geo_rows} rows)")

    # Confirm dim_facility is readable (row count only — columns already in mart)
    df_rows = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{DIM_FACILITY.as_posix()}')"
    ).fetchone()[0]
    print(f"dim_facility:  {df_rows} rows")

    # ------------------------------------------------------------------ #
    # 2. Resolve home ZIP coordinates                                      #
    # ------------------------------------------------------------------ #
    home_row = con.execute(
        "SELECT lat, lon FROM dim_zip_geo WHERE zip = ?", [home_zip]
    ).fetchone()
    if home_row is None:
        raise RuntimeError(f"HOME_ZIP '{home_zip}' not found in zip centroids")
    home_lat, home_lon = home_row

    # ------------------------------------------------------------------ #
    # 3. mart_near_me — haversine distance + near_me_score               #
    # ------------------------------------------------------------------ #
    health_rows = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{MART_HEALTH.as_posix()}')"
    ).fetchone()[0]
    # CTE computes distance once; outer SELECT derives near_me_score.
    # Haversine in SQL (result in miles):
    #   d = R * 2 * asin(sqrt(
    #         sin²(Δlat/2) + cos(lat1)*cos(lat2)*sin²(Δlon/2)
    #       ))
    con.execute(f"""
        CREATE TABLE mart_near_me AS
        WITH base AS (
            SELECT
                h.facility_key,
                h.facility_name,
                h.facility_address,
                h.facility_city,
                h.facility_zip5,
                h.cleanliness_index,
                h.low_data_flag,
                g.lat                                                        AS zip_lat,
                g.lon                                                        AS zip_lon,
                '{home_zip}'                                                 AS home_zip,
                {EARTH_RADIUS_MILES} * 2.0 * asin(sqrt(
                    power(sin(radians((g.lat - {home_lat!r}) / 2.0)), 2)
                    + cos(radians({home_lat!r})) * cos(radians(g.lat))
                    * power(sin(radians((g.lon - {home_lon!r}) / 2.0)), 2)
                ))                                                           AS distance_miles
            FROM read_parquet('{MART_HEALTH.as_posix()}') h
            JOIN dim_zip_geo g ON g.zip = h.facility_zip5
        )
        SELECT
            *,
            cleanliness_index * (1.0 / (1.0 + distance_miles)) AS near_me_score
        FROM base
    """)

    # ------------------------------------------------------------------ #
    # 4. Validations (hard fail)                                          #
    # ------------------------------------------------------------------ #
    null_key = con.execute(
        "SELECT COUNT(*) FROM mart_near_me WHERE facility_key IS NULL"
    ).fetchone()[0]
    if null_key > 0:
        raise RuntimeError(f"mart_near_me: {null_key} rows with null facility_key")

    null_dist = con.execute(
        "SELECT COUNT(*) FROM mart_near_me WHERE distance_miles IS NULL"
    ).fetchone()[0]
    if null_dist > 0:
        raise RuntimeError(f"mart_near_me: {null_dist} rows with null distance_miles")

    neg_dist = con.execute(
        "SELECT COUNT(*) FROM mart_near_me WHERE distance_miles < 0"
    ).fetchone()[0]
    if neg_dist > 0:
        raise RuntimeError(f"mart_near_me: {neg_dist} rows with distance_miles < 0")

    near_me_rows = con.execute("SELECT COUNT(*) FROM mart_near_me").fetchone()[0]

    if near_me_rows != health_rows:
        dropped = health_rows - near_me_rows
        print(
            f"DIAGNOSTIC: mart_facility_health={health_rows} rows, "
            f"mart_near_me={near_me_rows} rows, dropped={dropped}",
            file=sys.stderr,
        )
        top_missing = con.execute(f"""
            SELECT h.facility_zip5, COUNT(*) AS facility_count
            FROM read_parquet('{MART_HEALTH.as_posix()}') h
            WHERE h.facility_zip5 NOT IN (SELECT zip FROM dim_zip_geo)
            GROUP BY h.facility_zip5
            ORDER BY facility_count DESC
            LIMIT 10
        """).fetchall()
        print("DIAGNOSTIC: top facility_zip5 values missing from dim_zip_geo:", file=sys.stderr)
        for zip5, cnt in top_missing:
            print(f"  {zip5}  ({cnt} facilities)", file=sys.stderr)
        raise RuntimeError(
            f"ZIP join dropped {dropped} facilities "
            f"({health_rows} in mart_facility_health, {near_me_rows} in mart_near_me); "
            "add missing ZIPs to zip_centroids_626.csv"
        )

    print(f"mart_near_me:  {near_me_rows} rows")

    con.execute(f"COPY mart_near_me TO '{OUT_NEAR_ME.as_posix()}' (FORMAT PARQUET)")
    print(f"[ok] wrote {OUT_NEAR_ME} ({near_me_rows} rows)")

    # ------------------------------------------------------------------ #
    # 5. Top 25 printout                                                  #
    # ------------------------------------------------------------------ #
    top25 = con.execute("""
        SELECT
            facility_name,
            facility_city,
            facility_zip5,
            ROUND(distance_miles,    2) AS distance_miles,
            ROUND(cleanliness_index, 4) AS cleanliness_index,
            ROUND(near_me_score,     4) AS near_me_score
        FROM mart_near_me
        WHERE cleanliness_index IS NOT NULL
        ORDER BY near_me_score DESC
        LIMIT 25
    """).fetchall()

    header = (
        f"\nTop 25 near me (HOME_ZIP={home_zip})\n"
        f"{'facility_name':<45} {'city':<15} {'zip':<7}"
        f" {'dist_mi':>7} {'clean':>7} {'score':>8}"
    )
    print(header)
    print("-" * 92)
    for name, city, zip5, dist, clean, score in top25:
        print(
            f"{(name  or '')[:45]:<45}"
            f" {(city or '')[:15]:<15}"
            f" {(zip5 or ''):<7}"
            f" {dist:>7.2f}"
            f" {clean:>7.4f}"
            f" {score:>8.4f}"
        )

    # ------------------------------------------------------------------ #
    # 6. geo_meta.json                                                    #
    # ------------------------------------------------------------------ #
    meta = {
        "home_zip": home_zip,
        "home_lat": home_lat,
        "home_lon": home_lon,
        "dim_zip_geo_rows": zip_geo_rows,
        "mart_near_me_rows": near_me_rows,
    }
    OUT_META.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"[ok] wrote {OUT_META}")

    con.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
