"""
One-time utility: build db/seeds/zip_centroids_626.csv from Census ZCTA Gazetteer,
with a GeoNames fallback for postal ZIPs absent from the ZCTA dataset.

Run from repo root:
  python -m src.make_zip_centroids

Sources:
  1. tmp/2022_Gaz_zcta_national.zip  — Census ZCTA gazetteer (primary)
  2. tmp/US.zip                       — GeoNames US postal codes (fallback)

GeoNames format: tab-delimited, no header row, columns are:
  country_code, postal_code, place_name, admin_name1, admin_code1,
  admin_name2, admin_code2, admin_name3, admin_code3, latitude, longitude, accuracy

For ZIPs that appear multiple times in GeoNames we use the FIRST occurrence
(deterministic; avoids floating-point averaging across unrelated place names).
"""

from __future__ import annotations

import csv
import sys
import zipfile
from pathlib import Path


SEED_ZIPS = Path("db/seeds/zip_626.csv")
GAZ_ZIP = Path("tmp/2022_Gaz_zcta_national.zip")
GEONAMES_ZIP = Path("tmp/US.zip")
OUT = Path("db/seeds/zip_centroids_626.csv")
MISSING_OUT = Path("db/seeds/zip_centroids_missing_626.csv")
OVERRIDES = Path("db/seeds/zip_centroids_overrides_626.csv")

# GeoNames column indices (0-based, tab-delimited, no header)
GN_POSTAL_CODE = 1
GN_LATITUDE = 9
GN_LONGITUDE = 10


def load_seed_zips(path: Path) -> set[str]:
    if not path.exists():
        raise RuntimeError(f"Missing seed zips file: {path}")
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0].strip().lower() != "zip":
        raise RuntimeError(f"Expected header 'zip' in {path}")
    zips: set[str] = set()
    for line in lines[1:]:
        digits = "".join(ch for ch in line.strip() if ch.isdigit())
        if len(digits) == 5:
            zips.add(digits)
    if not zips:
        raise RuntimeError(f"No ZIPs parsed from {path}")
    return zips


def load_zcta_centroids(wanted: set[str]) -> dict[str, tuple[str, str]]:
    """Return {zip: (lat, lon)} for ZIPs found in the Census ZCTA gazetteer."""
    if not GAZ_ZIP.exists():
        raise RuntimeError(f"Missing gazetteer zip (download first): {GAZ_ZIP}")

    found: dict[str, tuple[str, str]] = {}
    with zipfile.ZipFile(GAZ_ZIP) as z:
        txt_names = [
            n for n in z.namelist()
            if n.lower().endswith(".txt") and "zcta" in n.lower()
        ]
        if not txt_names:
            raise RuntimeError(f"Could not find ZCTA .txt inside {GAZ_ZIP}")

        with z.open(txt_names[0]) as f:
            lines = (line.decode("utf-8") for line in f)

            # Read and normalize header — Census files have trailing spaces on some fields
            header = next(lines).rstrip("\n")
            fields = [c.strip() for c in header.split("\t")]

            required = {"GEOID", "INTPTLAT", "INTPTLONG"}
            if not required.issubset(set(fields)):
                raise RuntimeError(f"Unexpected columns in gazetteer file: {fields}")

            reader = csv.DictReader(lines, delimiter="\t", fieldnames=fields)
            for r in reader:
                zcta = (r.get("GEOID") or "").strip()
                if zcta in wanted:
                    lat = (r.get("INTPTLAT") or "").strip()
                    lon = (r.get("INTPTLONG") or "").strip()
                    found[zcta] = (lat, lon)

    return found


def load_geonames_centroids(wanted: set[str]) -> dict[str, tuple[str, str]]:
    """Return {zip: (lat, lon)} for ZIPs found in GeoNames US postal code file.

    Uses the FIRST occurrence of each ZIP (deterministic).
    """
    if not GEONAMES_ZIP.exists():
        raise RuntimeError(f"Missing GeoNames zip (download first): {GEONAMES_ZIP}")

    found: dict[str, tuple[str, str]] = {}
    with zipfile.ZipFile(GEONAMES_ZIP) as z:
        txt_names = [n for n in z.namelist() if n.upper() == "US.TXT"]
        if not txt_names:
            raise RuntimeError(f"Could not find US.txt inside {GEONAMES_ZIP}")

        with z.open(txt_names[0]) as f:
            for raw in f:
                parts = raw.decode("utf-8").rstrip("\n").split("\t")
                if len(parts) <= GN_LONGITUDE:
                    continue
                postal = parts[GN_POSTAL_CODE].strip()
                if postal not in wanted or postal in found:
                    continue  # not needed, or already recorded (keep first occurrence)
                lat = parts[GN_LATITUDE].strip()
                lon = parts[GN_LONGITUDE].strip()
                if lat and lon:
                    found[postal] = (lat, lon)

    return found
def apply_overrides(
    centroids: dict[str, tuple[str, str]],
    still_missing: set[str],
) -> tuple[dict[str, tuple[str, str]], set[str], int]:
    """Apply deterministic overrides for remaining missing ZIPs.

    Overrides file schema:
      zip,method,neighbors,source_note

    Supported methods:
      - mean_neighbors: centroid = mean(lat/lon) of neighbor ZIPs already in centroids
    """
    if not OVERRIDES.exists() or not still_missing:
        return centroids, still_missing, 0

    found_from_overrides = 0
    rows = OVERRIDES.read_text(encoding="utf-8").splitlines()
    if not rows:
        return centroids, still_missing, 0

    reader = csv.DictReader(rows)
    required_cols = {"zip", "method", "neighbors"}
    if not required_cols.issubset(set(reader.fieldnames or [])):
        raise RuntimeError(f"Overrides file has unexpected header: {reader.fieldnames}")

    for r in reader:
        z = (r.get("zip") or "").strip()
        if z not in still_missing:
            continue

        method = (r.get("method") or "").strip()
        neighbors_raw = (r.get("neighbors") or "").strip().strip('"')
        neighbors = [n.strip() for n in neighbors_raw.split("|") if n.strip()]

        if method != "mean_neighbors":
            raise RuntimeError(f"Unsupported override method for {z}: {method}")

        if not neighbors:
            raise RuntimeError(f"Override {z} has no neighbors")

        lats: list[float] = []
        lons: list[float] = []
        for n in neighbors:
            if n not in centroids:
                raise RuntimeError(f"Override {z} neighbor missing centroid: {n}")
            lat_s, lon_s = centroids[n]
            lats.append(float(lat_s))
            lons.append(float(lon_s))

        lat = sum(lats) / len(lats)
        lon = sum(lons) / len(lons)

        # write with fixed precision for stability
        centroids[z] = (f"{lat:.6f}", f"{lon:.6f}")
        still_missing.remove(z)
        found_from_overrides += 1

    return centroids, still_missing, found_from_overrides

def main() -> None:
    seed = load_seed_zips(SEED_ZIPS)

    # 1) primary: Census ZCTA
    zcta_centroids = load_zcta_centroids(seed)
    found_from_zcta = set(zcta_centroids)

    # 2) fallback: GeoNames for ZIPs not in ZCTA
    still_needed = seed - found_from_zcta
    geonames_centroids = load_geonames_centroids(still_needed)
    found_from_geonames = set(geonames_centroids)

    # 3) combine
    centroids: dict[str, tuple[str, str]] = dict(zcta_centroids)
    centroids.update(geonames_centroids)

    # 4) overrides for anything still missing
    still_missing_set = set(seed) - set(centroids)
    centroids, still_missing_set, found_from_overrides = apply_overrides(centroids, still_missing_set)

    still_missing = sorted(still_missing_set)

    print(
        f"seed_zips={len(seed)}  "
        f"found_from_zcta={len(found_from_zcta)}  "
        f"found_from_geonames={len(found_from_geonames)}  "
        f"found_from_overrides={found_from_overrides}  "
        f"still_missing={len(still_missing)}"
    )

    # 5) write outputs
    OUT.parent.mkdir(parents=True, exist_ok=True)

    all_rows = sorted(
        [{"zip": z, "lat": lat, "lon": lon} for z, (lat, lon) in centroids.items()],
        key=lambda x: x["zip"],
    )

    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["zip", "lat", "lon"])
        w.writeheader()
        w.writerows(all_rows)
    print(f"[ok] wrote {OUT} ({len(all_rows)} rows)")

    with MISSING_OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["zip"])
        w.writeheader()
        w.writerows({"zip": z} for z in still_missing)
    print(f"[ok] wrote {MISSING_OUT} ({len(still_missing)} rows)")

    # 6) exit status
    if still_missing:
        print(
            f"ERROR: {len(still_missing)} ZIPs not found in any source/override: {still_missing}",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
