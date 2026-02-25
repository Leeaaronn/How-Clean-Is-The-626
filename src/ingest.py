"""
Phase 1 — Ingest LA County Food Safety data (ArcGIS Online CSV items).

Run from repo root:
    python -m src.ingest
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

RAW_DIR = Path("data/raw")
TIMEOUT_SECS = 120

# ArcGIS Online "item data" endpoint streams the file for public CSV items. :contentReference[oacute:1]{index=1}
ARCGIS_ITEM_DATA_BASE = "https://lacounty.maps.arcgis.com/sharing/rest/content/items"

DATASETS: list[dict[str, str]] = [
    {
        "name": "inspections",
        "item_id": "19b6607ac82c4512b10811870975dbdc",
        "filename": "inspections_raw.csv",
    },
    {
        "name": "violations",
        "item_id": "5eaea9f89b7549ee841da7617d3a9cba",
        "filename": "violations_raw.csv",
    },
]


def _download_file(url: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "626-food-safety/1.0",
                "Accept": "text/csv,*/*",
            },
        )
        with urllib.request.urlopen(req, timeout=TIMEOUT_SECS) as resp:
            if getattr(resp, "status", 200) != 200:
                raise RuntimeError(f"HTTP {resp.status} downloading {url}")

            # Stream to disk to avoid holding ~50MB in memory
            with out_path.open("wb") as f:
                while True:
                    chunk = resp.read(1024 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)

    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
        snippet = body[:300].replace("\n", " ")
        raise RuntimeError(f"HTTP error {exc.code} downloading {url}: {exc.reason}. Body: {snippet}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"URL error downloading {url}: {exc.reason}") from exc

    if (not out_path.exists()) or out_path.stat().st_size == 0:
        raise RuntimeError(f"Failed to write non-empty file: {out_path}")


def _count_csv_rows(path: Path) -> int:
    # Count data rows (exclude header). Works even if file is large.
    with path.open("rb") as f:
        lines = 0
        for _ in f:
            lines += 1
    return max(0, lines - 1)


def ingest_dataset(ds: dict[str, str]) -> dict[str, Any]:
    name = ds["name"]
    item_id = ds["item_id"]
    filename = ds["filename"]

    url = f"{ARCGIS_ITEM_DATA_BASE}/{item_id}/data"
    out_path = RAW_DIR / filename

    print(f"[{name}] downloading item_id={item_id} ...", flush=True)
    _download_file(url, out_path)

    row_count = _count_csv_rows(out_path)
    if row_count == 0:
        raise RuntimeError(f"[{name}] zero data rows after download — aborting")

    print(f"[{name}] wrote {out_path} ({row_count:,} rows)", flush=True)

    return {
        "name": name,
        "item_id": item_id,
        "source_url": url,
        "output_filename": filename,
        "row_count": row_count,
        "bytes": out_path.stat().st_size,
    }


def main() -> None:
    entries: list[dict[str, Any]] = []
    for ds in DATASETS:
        entries.append(ingest_dataset(ds))

    meta = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "datasets": entries,
    }

    meta_path = RAW_DIR / "ingest_meta.json"
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print(f"[meta] written to {meta_path}", flush=True)
    for e in entries:
        print(f"  {e['name']}: {e['row_count']:,} rows ({e['bytes']:,} bytes)", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)