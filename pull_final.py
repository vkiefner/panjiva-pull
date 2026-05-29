"""
pull.py — extraction + cleaning pipeline (verbose logging build).

Per (source, week): submit bulk job -> poll status -> download JSONL ->
flatten via transform.py -> write shipments + items Parquet.

This build prints each step (submit / each poll / download bytes / write) so
you can see progress on a slow connection instead of a silent bar. Resumable
via checkpoint.json. Keep SOURCES + dates within your trial entitlement.
"""

from __future__ import annotations

import json
import gzip
import time
import pathlib
from datetime import datetime, timezone, timedelta
from dateutil.parser import parse as parse_date

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from client import PanjivaClient
import transform as T

# --------------------------------------------------------------------------
# Pull authorized US import/export. ~6 min per calendar-week (both sources),
# so 12h ~= 2.3 years. Resumable: re-run to continue into the SAME folder.
RUN_NAME = "us_2022_2024"          # output folder under <DATA_DIR>/raw/
SOURCES = ["us-imports", "us-exports"]
DATE_FROM = "2022-01-01"
DATE_TO = "2024-03-31"
CHUNK_DAYS = 7
POLL_SECONDS = 30
POLL_TIMEOUT = 6 * 3600
CAP_WARN_AT = 950_000
# --------------------------------------------------------------------------


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def day_chunks(date_from, date_to, days):
    cur = parse_date(date_from).date()
    end = parse_date(date_to).date()
    while cur <= end:
        stop = min(cur + timedelta(days=days - 1), end)
        yield cur.isoformat(), stop.isoformat()
        cur = stop + timedelta(days=1)


class Checkpoint:
    def __init__(self, path):
        self.path = path
        self.done = set(json.loads(path.read_text())["done"]) if path.exists() else set()

    def mark(self, key):
        self.done.add(key)
        self.path.write_text(json.dumps({"done": sorted(self.done)}, indent=2))

    def has(self, key):
        return key in self.done


def submit_bulk(c, source, start, stop):
    body = {"data_source": source, "output_format": "jsonl",
            "filters": {"record_date": {"gte": start, "lte": stop}}}
    if c.s.notify_email:
        body["notify_email"] = c.s.notify_email
    resp = c.post_bulk(c.s.async_bulk_path, body)
    job_id = resp.get("job_id") or resp.get("id")
    if not job_id:
        raise RuntimeError(f"No job_id in response: {resp}")
    log(f"    submitted job {job_id}")
    return job_id


def wait_for_job(c, job_id):
    waited = 0
    status_path = f"{c.s.async_status_path}/{job_id}"
    while waited < POLL_TIMEOUT:
        status = c.get(status_path)
        state = (status.get("status") or "").lower()
        url = status.get("download_url")
        log(f"    poll +{waited:>4}s  status={state or '?'}")
        if url:                       # ready whenever a URL appears, any status name
            return url
        if state in ("failed", "error", "cancelled", "canceled"):
            raise RuntimeError(f"job failed: {status}")
        time.sleep(POLL_SECONDS)
        waited += POLL_SECONDS
    raise TimeoutError(f"job {job_id} not ready after {POLL_TIMEOUT}s")


def download_jsonl(c, url, dest):
    log("    downloading...")
    with c.session.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0)) or None
        bar = tqdm(total=total, unit="B", unit_scale=True, unit_divisor=1024,
                   desc="      bytes", leave=False)
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))
        bar.close()
    mb = dest.stat().st_size / 1024 / 1024
    log(f"    downloaded {mb:,.1f} MB")


def _open_text(path):
    """Open as text whether the file is gzip-compressed or plain.

    Panjiva serves the bulk download as gzipped JSONL, so we sniff the gzip
    magic bytes (1f 8b) and decompress on the fly. errors='replace' keeps one
    stray byte from killing an entire week's chunk during an unattended run.
    """
    with open(path, "rb") as fh:
        magic = fh.read(2)
    if magic == b"\x1f\x8b":
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, "r", encoding="utf-8", errors="replace")


def jsonl_to_tables(src, source, ship_path, item_path, batch=50_000):
    ship_path.parent.mkdir(parents=True, exist_ok=True)
    item_path.parent.mkdir(parents=True, exist_ok=True)
    sw = pq.ParquetWriter(ship_path, T.SHIPMENT_SCHEMA, compression="zstd")
    iw = pq.ParquetWriter(item_path, T.ITEM_SCHEMA, compression="zstd")
    ship_rows, item_rows, n_ship, n_item = [], [], 0, 0

    def flush():
        nonlocal ship_rows, item_rows, n_ship, n_item
        if ship_rows:
            sw.write_table(pa.Table.from_pylist(ship_rows, schema=T.SHIPMENT_SCHEMA))
            n_ship += len(ship_rows); ship_rows = []
        if item_rows:
            iw.write_table(pa.Table.from_pylist(item_rows, schema=T.ITEM_SCHEMA))
            n_item += len(item_rows); item_rows = []

    try:
        with _open_text(src) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                ship_rows.append(T.flatten_shipment(rec, source))
                item_rows.extend(T.flatten_items(rec, source))
                if len(ship_rows) >= batch:
                    flush()
        flush()
    finally:
        sw.close(); iw.close()
    return n_ship, n_item


def main():
    c = PanjivaClient()
    root = pathlib.Path(c.s.data_dir) / "raw" / RUN_NAME
    tmp = root / "_jsonl"; tmp.mkdir(parents=True, exist_ok=True)
    ckpt = Checkpoint(root / "checkpoint.json")

    chunks = [(s, a, b) for s in SOURCES for (a, b) in day_chunks(DATE_FROM, DATE_TO, CHUNK_DAYS)]
    log(f"Run '{RUN_NAME}': {len(chunks)} (source, {CHUNK_DAYS}d) chunks -> {root}")
    log(f"  ({len(ckpt.done)} already done and will be skipped)")

    for i, (source, start, stop) in enumerate(chunks, 1):
        key = f"{source}:{start}"
        if ckpt.has(key):
            log(f"[{i}/{len(chunks)}] {key} already done, skipping")
            continue
        log(f"[{i}/{len(chunks)}] {key}  ({start}..{stop})")
        try:
            job_id = submit_bulk(c, source, start, stop)
            url = wait_for_job(c, job_id)
            jsonl_path = tmp / f"{source}_{start}.jsonl"
            download_jsonl(c, url, jsonl_path)
            ship_path = root / "shipments" / source / f"{source}_{start}.parquet"
            item_path = root / "items" / source / f"{source}_{start}.parquet"
            log("    writing parquet...")
            n_ship, n_item = jsonl_to_tables(jsonl_path, source, ship_path, item_path)
            jsonl_path.unlink(missing_ok=True)
            ckpt.mark(key)
            warn = "  <-- NEAR BULK CAP, shrink CHUNK_DAYS" if n_ship >= CAP_WARN_AT else ""
            log(f"    DONE: {n_ship:,} shipments / {n_item:,} items{warn}")
        except Exception as e:  # noqa: BLE001
            log(f"    !! FAILED ({e}); will retry on next run")

    log(f"Finished. Parquet under {root}. Re-run to resume any failed chunks.")
    log(f"Count later: duckdb -c \"SELECT count(*) FROM "
        f"read_parquet('{root}/shipments/**/*.parquet')\"")


if __name__ == "__main__":
    main()
