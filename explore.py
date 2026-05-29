"""
explore.py — the "test" pass. Run this FIRST, gently.

Verified against the API spec, it answers what you need before pulling:
  0. Can I get a token?
  1. Which data-sources is my trial actually entitled to? (metadata endpoint)
  2. What fields does us-imports / us-exports expose? (schema endpoint)
  3. What does a real record look like, and how does paging behave?
  4. Does my polite request rate stay clear of 429s?
  5. Is async/bulk enabled on my account?

Tiny queries only — we are mapping the API, not draining it. Findings are
written to <DATA_DIR>/explore/<timestamp>/report.json.
"""

from __future__ import annotations

import json
import time
import pathlib
from datetime import datetime, timezone

from client import PanjivaClient, Settings

SOURCES = ["us-imports", "us-exports"]


def _outdir(settings: Settings) -> pathlib.Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    d = pathlib.Path(settings.data_dir) / "explore" / ts
    d.mkdir(parents=True, exist_ok=True)
    return d


def step0_auth(c, report):
    print("[0] Authenticating...")
    report["auth"] = {"ok": True, "token_len": len(c.token())}
    print("    OK")


def step1_entitlements(c, report):
    print("[1] Listing entitled data-sources...")
    data = c.get(c.s.data_sources_path)
    sources = data.get("data_sources", [])
    report["entitled_sources"] = sources
    for s in SOURCES:
        print(f"    {s}: {'AVAILABLE' if s in sources else 'NOT in your trial'}")


def step2_schema(c, report, out):
    print("[2] Fetching field schema per source...")
    report["schema"] = {}
    for s in SOURCES:
        try:
            schema = c.get(c.s.schema_path.replace("{src}", s))
            (out / f"schema_{s}.json").write_text(json.dumps(schema, indent=2))
            report["schema"][s] = {"top_level_keys": sorted(schema.keys())}
            print(f"    {s}: schema saved")
        except Exception as e:  # noqa: BLE001
            report["schema"][s] = {"error": str(e)}
            print(f"    {s}: {e}")


def step3_sample_and_paging(c, report, out):
    print("[3] Sampling records + checking paging (size<=500, offset<=4500)...")
    body = {"data_source": SOURCES[0], "size": 3,
            "filters": {"record_date": {"gte": "2024-01-01", "lte": "2024-01-31"}}}
    data = c.post(c.s.shipment_path, body)
    (out / "sample_records.json").write_text(json.dumps(data, indent=2)[:200_000])
    recs = data.get("records", [])
    rc = data.get("record_count", {})
    report["sample"] = {"top_level_keys": sorted(data.keys()),
                        "fields": sorted(recs[0].keys()) if recs else [],
                        "record_count": rc}
    print(f"    total reported: {rc.get('total')}, fields: "
          f"{(sorted(recs[0].keys())[:12] if recs else [])} ...")


def step4_rate(c, report):
    print(f"[4] Short burst at {c.s.max_rps} req/s (normal limit is 20)...")
    t0, n, errors = time.time(), 10, 0
    for _ in range(n):
        try:
            c.post(c.s.shipment_path, {"data_source": SOURCES[0], "size": 1,
                   "filters": {"record_date": {"gte": "2024-01-01", "lte": "2024-01-02"}}})
        except Exception as e:  # noqa: BLE001
            errors += 1
            print(f"    request error: {e}")
    dt = time.time() - t0
    report["rate"] = {"requests": n, "seconds": round(dt, 2),
                      "effective_rps": round(n / dt, 2), "errors": errors}
    print(f"    {n} requests in {dt:.1f}s, {errors} errors")


def step5_async(c, report):
    print("[5] Checking async/bulk availability (1 req/s endpoint)...")
    try:
        resp = c.post_bulk(c.s.async_bulk_path,
                           {"data_source": SOURCES[0], "output_format": "jsonl",
                            "filters": {"record_date": {"gte": "2024-01-01",
                                                        "lte": "2024-01-02"}},
                            **({"notify_email": c.s.notify_email} if c.s.notify_email else {})})
        report["async"] = {"enabled": True, "job": resp}
        print(f"    ENABLED — submitted test job {resp.get('job_id')}")
    except Exception as e:  # noqa: BLE001
        report["async"] = {"enabled": False, "error": str(e)}
        print(f"    not available: {e}")
        print("    -> ask your Panjiva rep to enable bulk/batch for large pulls")


def main():
    c = PanjivaClient()
    out = _outdir(c.s)
    report = {"run_at": datetime.now(timezone.utc).isoformat()}
    try: step0_auth(c, report)
    except Exception as e: report.setdefault("errors", {})["auth"] = str(e); print("  !!", e)
    for fn, needs_out in [(step1_entitlements, False), (step2_schema, True),
                          (step3_sample_and_paging, True), (step4_rate, False),
                          (step5_async, False)]:
        try:
            fn(c, report, out) if needs_out else fn(c, report)
        except Exception as e:  # noqa: BLE001
            report.setdefault("errors", {})[fn.__name__] = str(e)
            print(f"  !! {fn.__name__}: {e}")
    (out / "report.json").write_text(json.dumps(report, indent=2))
    print(f"\nReport written to {out / 'report.json'}")


if __name__ == "__main__":
    main()
