"""
process_local.py — convert already-downloaded bulk JSONL(.gz) files to Parquet
WITHOUT re-downloading. Use it to salvage a run whose downloads succeeded but
whose Parquet writes failed (e.g. the pre-gzip-fix run).

Needs only pyarrow + transform.py — no API creds, no network.
Set JSONL_DIR to the run's _jsonl folder, then:  python process_local.py
"""

from __future__ import annotations

import json
import gzip
import pathlib

import pyarrow as pa
import pyarrow.parquet as pq

import transform as T

# --- point this at the _jsonl folder that already holds the downloads -------
JSONL_DIR = pathlib.Path(
    r"\\bigdata.wu.ac.at\vkiefner\panjiva\data\raw\pull_20260529T091651Z\_jsonl"
)
# Parquet goes here (defaults to that same run folder):
OUT_ROOT = JSONL_DIR.parent
# ---------------------------------------------------------------------------


def _open_text(path):
    with open(path, "rb") as fh:
        magic = fh.read(2)
    if magic == b"\x1f\x8b":
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, "r", encoding="utf-8", errors="replace")


def convert(src, source, ship_path, item_path, batch=50_000):
    ship_path.parent.mkdir(parents=True, exist_ok=True)
    item_path.parent.mkdir(parents=True, exist_ok=True)
    sw = pq.ParquetWriter(ship_path, T.SHIPMENT_SCHEMA, compression="zstd")
    iw = pq.ParquetWriter(item_path, T.ITEM_SCHEMA, compression="zstd")
    ship, items, n_s, n_i = [], [], 0, 0

    def flush():
        nonlocal ship, items, n_s, n_i
        if ship:
            sw.write_table(pa.Table.from_pylist(ship, schema=T.SHIPMENT_SCHEMA))
            n_s += len(ship); ship = []
        if items:
            iw.write_table(pa.Table.from_pylist(items, schema=T.ITEM_SCHEMA))
            n_i += len(items); items = []

    try:
        with _open_text(src) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                ship.append(T.flatten_shipment(rec, source))
                items.extend(T.flatten_items(rec, source))
                if len(ship) >= batch:
                    flush()
        flush()
    finally:
        sw.close(); iw.close()
    return n_s, n_i


def main():
    files = sorted(JSONL_DIR.glob("*.jsonl")) + sorted(JSONL_DIR.glob("*.jsonl.gz"))
    print(f"Found {len(files)} downloaded files in {JSONL_DIR}")
    total_s = total_i = 0
    for i, fp in enumerate(files, 1):
        stem = fp.name
        for ext in (".jsonl.gz", ".jsonl"):
            if stem.endswith(ext):
                stem = stem[: -len(ext)]; break
        source, start = stem.rsplit("_", 1)   # 'us-imports', '2024-01-01'
        ship = OUT_ROOT / "shipments" / source / f"{source}_{start}.parquet"
        item = OUT_ROOT / "items" / source / f"{source}_{start}.parquet"
        try:
            n_s, n_i = convert(fp, source, ship, item)
            total_s += n_s; total_i += n_i
            print(f"[{i}/{len(files)}] {source} {start}: {n_s:,} shipments / {n_i:,} items")
        except Exception as e:  # noqa: BLE001
            print(f"[{i}/{len(files)}] {fp.name} FAILED: {e}")
    print(f"\nTOTAL: {total_s:,} shipments / {total_i:,} items -> {OUT_ROOT}")


if __name__ == "__main__":
    main()
