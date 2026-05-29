"""
transform.py — turn nested Panjiva shipment records into a flat, WRDS-style
star schema:

  * shipments : one row per record_id (wide; all scalar + single-nested fields)
  * items     : one row per line item, keyed by record_id (the one-to-many part)

Both tables use EXPLICIT pyarrow schemas so every Parquet file produced across
every chunk has identical columns/types. That means you can later read the
whole pull as one dataset (DuckDB / pyarrow) with no schema-drift surprises.

Records differ a little between us-imports and the three us-exports shapes
(aes/dis/paper) — e.g. exports carry transport_flight_code but no
place_of_receipt/manifest/notify_party. Everything here uses .get(), so a
missing field just lands as null in the unified schema.
"""

from __future__ import annotations

import json
from datetime import datetime

import pyarrow as pa


# --- small, defensive casters ---------------------------------------------
def _s(x):
    if x is None:
        return None
    s = str(x).strip()
    return s or None


def _f(x):
    try:
        return float(str(x).replace(",", "")) if x not in (None, "") else None
    except (ValueError, TypeError):
        return None


def _i(x):
    try:
        return int(x) if x not in (None, "") else None
    except (ValueError, TypeError):
        return None


def _b(x):
    return bool(x) if isinstance(x, bool) else None


def _date(x):
    if not x:
        return None
    try:
        return datetime.strptime(str(x)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _latlon(arr):
    if isinstance(arr, (list, tuple)) and len(arr) == 2:
        return _f(arr[0]), _f(arr[1])
    return None, None


def _first_gics(gics):
    if isinstance(gics, list) and gics:
        g = gics[0] or {}
        return _s(g.get("code")), _s(g.get("description"))
    return None, None


def _company(prefix: str, obj: dict | None) -> dict:
    """Flatten a consignee/shipper object (incl. address, ultimate_parent,
    corporate_info) into prefixed columns."""
    obj = obj or {}
    addr = obj.get("address") or {}
    a_lat, a_lon = _latlon(addr.get("lat_long"))
    up = obj.get("ultimate_parent") or {}
    up_addr = up.get("address") or {}
    ci = up.get("corporate_info") or {}
    p = prefix
    return {
        f"{p}_pid": _i(obj.get("pid")),
        f"{p}_ccn_id": _i(obj.get("ccn_id")),
        f"{p}_capiq_id": _i(obj.get("capiq_id")),
        f"{p}_duns_number": _s(obj.get("duns_number")),
        f"{p}_name": _s(obj.get("name")),
        f"{p}_trade_roles": _s(obj.get("trade_roles")),
        f"{p}_addr_full": _s(addr.get("full_address")),
        f"{p}_addr_street": _s(addr.get("street")),
        f"{p}_addr_city": _s(addr.get("city")),
        f"{p}_addr_region": _s(addr.get("region")),
        f"{p}_addr_postal_code": _s(addr.get("postal_code")),
        f"{p}_addr_country": _s(addr.get("country")),
        f"{p}_addr_lat": a_lat,
        f"{p}_addr_lon": a_lon,
        f"{p}_up_name": _s(up.get("name")),
        f"{p}_up_website": _s(up.get("website")),
        f"{p}_up_addr_full": _s(up_addr.get("full_address")),
        f"{p}_up_addr_city": _s(up_addr.get("city")),
        f"{p}_up_addr_region": _s(up_addr.get("region")),
        f"{p}_up_addr_country": _s(up_addr.get("country")),
        f"{p}_up_capiq_id": _i(ci.get("capiq_id")),
        f"{p}_up_revenue": _s(ci.get("revenue")),
        f"{p}_up_employees": _s(ci.get("employees")),
        f"{p}_up_incorporation_date": _i(ci.get("incorporation_date")),
        f"{p}_up_industry": _s(ci.get("industry")),
        f"{p}_up_market_cap": _s(ci.get("market_cap")),
    }


# --- shipment (header) row -------------------------------------------------
def flatten_shipment(rec: dict, data_source: str) -> dict:
    pol = rec.get("port_of_lading") or {}
    pou = rec.get("port_of_unlading") or {}
    pol_lat, pol_lon = _latlon(pol.get("lat_long"))
    pou_lat, pou_lon = _latlon(pou.get("lat_long"))
    tr = rec.get("transporter") or {}
    vessel = rec.get("vessel") or {}
    npy = rec.get("notify_party") or {}
    sdate = _date(rec.get("shipment_date"))
    gics_code, gics_desc = _first_gics(rec.get("gics_code"))

    row = {
        "record_id": _s(rec.get("record_id")),
        "data_source": data_source,
        "data_source_country": _s(rec.get("data_source_country")),
        "trade_direction": _s(rec.get("trade_direction")),
        "document_number": _s(rec.get("document_number")),
        "shipment_date": sdate,
        "shipment_year": sdate.year if sdate else None,
        "shipment_month": sdate.month if sdate else None,
        "transport_method": _s(rec.get("transport_method")),
        "transport_flight_code": _s(rec.get("transport_flight_code")),
        "containerized": _b(rec.get("containerized")),
        "shipment_origin": _s(rec.get("shipment_origin")),
        "shipment_destination": _s(rec.get("shipment_destination")),
        "weight_kg": _f(rec.get("weight_kg")),
        "value_usd": _f(rec.get("value_usd")),
        "volume_teu": _f(rec.get("volume_teu")),
        "gics_code": gics_code,
        "gics_description": gics_desc,
        "place_of_receipt": _s(rec.get("place_of_receipt")),
        "frob": _s(rec.get("frob")),
        "manifest_number": _s(rec.get("manifest_number")),
        "master_bill_of_lading": _s(rec.get("master_bill_of_lading")),
        "bill_type": _s(rec.get("bill_type")),
        "inbond_code": _s(rec.get("inbond_code")),
        "number_of_containers": _i(rec.get("number_of_containers")),
        "weight_original_format": _s(rec.get("weight_original_format")),
        "measurement": _s(rec.get("measurement")),
        "quantity": _s(rec.get("quantity")),
        "vessel_name": _s(vessel.get("name")),
        "vessel_voyage_number": _s(vessel.get("voyage_number")),
        "vessel_imo": _s(vessel.get("imo")),
        "pol_name": _s(pol.get("name")),
        "pol_un_locode": _s(pol.get("un_locode")),
        "pol_country": _s(pol.get("country")),
        "pol_lat": pol_lat,
        "pol_lon": pol_lon,
        "pou_name": _s(pou.get("name")),
        "pou_un_locode": _s(pou.get("un_locode")),
        "pou_country": _s(pou.get("country")),
        "pou_lat": pou_lat,
        "pou_lon": pou_lon,
        "transporter_scac": _s(tr.get("scac")),
        "transporter_name": _s(tr.get("name")),
        "notify_party_name": _s(npy.get("name")),
        "notify_party_address": _s(npy.get("address")),
        "notify_party_scacs": (json.dumps(rec["notify_party_scacs"], ensure_ascii=False)
                               if rec.get("notify_party_scacs") else None),
    }
    row.update(_company("consignee", rec.get("consignee")))
    row.update(_company("shipper", rec.get("shipper")))
    return row


# --- item (child) rows -----------------------------------------------------
def flatten_items(rec: dict, data_source: str) -> list[dict]:
    record_id = _s(rec.get("record_id"))
    sdate = _date(rec.get("shipment_date"))
    out = []
    for idx, it in enumerate(rec.get("items") or []):
        it = it or {}
        hs = it.get("hs_code") or []
        hs = hs if isinstance(hs, list) else [hs]
        cq = it.get("container_quantities")
        out.append({
            "record_id": record_id,
            "data_source": data_source,
            "shipment_date": sdate,
            "item_index": idx,
            "description": _s(it.get("description")),
            "description_full": _s(it.get("description_full")),
            "hs_code": ";".join(str(h) for h in hs) if hs else None,
            "hs_code_primary": _s(hs[0]) if hs else None,
            "container_marks": _s(it.get("container_marks")),
            "container_number": _s(it.get("container_number")),
            "container_type_of_service": _s(it.get("container_type_of_service")),
            "container_type": _s(it.get("container_type")),
            "container_quantities": (json.dumps(cq, ensure_ascii=False) if cq else None),
            "quantity": _s(it.get("quantity")),
            "lcl": _b(it.get("lcl")),
            "dangerous_goods": _b(it.get("dangerous_goods")),
        })
    return out


# --- explicit schemas (keep in sync with the dicts above) ------------------
def _company_fields(p: str) -> list:
    return [
        pa.field(f"{p}_pid", pa.int64()), pa.field(f"{p}_ccn_id", pa.int64()),
        pa.field(f"{p}_capiq_id", pa.int64()), pa.field(f"{p}_duns_number", pa.string()),
        pa.field(f"{p}_name", pa.string()), pa.field(f"{p}_trade_roles", pa.string()),
        pa.field(f"{p}_addr_full", pa.string()), pa.field(f"{p}_addr_street", pa.string()),
        pa.field(f"{p}_addr_city", pa.string()), pa.field(f"{p}_addr_region", pa.string()),
        pa.field(f"{p}_addr_postal_code", pa.string()), pa.field(f"{p}_addr_country", pa.string()),
        pa.field(f"{p}_addr_lat", pa.float64()), pa.field(f"{p}_addr_lon", pa.float64()),
        pa.field(f"{p}_up_name", pa.string()), pa.field(f"{p}_up_website", pa.string()),
        pa.field(f"{p}_up_addr_full", pa.string()), pa.field(f"{p}_up_addr_city", pa.string()),
        pa.field(f"{p}_up_addr_region", pa.string()), pa.field(f"{p}_up_addr_country", pa.string()),
        pa.field(f"{p}_up_capiq_id", pa.int64()), pa.field(f"{p}_up_revenue", pa.string()),
        pa.field(f"{p}_up_employees", pa.string()), pa.field(f"{p}_up_incorporation_date", pa.int32()),
        pa.field(f"{p}_up_industry", pa.string()), pa.field(f"{p}_up_market_cap", pa.string()),
    ]


SHIPMENT_SCHEMA = pa.schema([
    pa.field("record_id", pa.string()), pa.field("data_source", pa.string()),
    pa.field("data_source_country", pa.string()), pa.field("trade_direction", pa.string()),
    pa.field("document_number", pa.string()), pa.field("shipment_date", pa.date32()),
    pa.field("shipment_year", pa.int32()), pa.field("shipment_month", pa.int32()),
    pa.field("transport_method", pa.string()), pa.field("transport_flight_code", pa.string()),
    pa.field("containerized", pa.bool_()), pa.field("shipment_origin", pa.string()),
    pa.field("shipment_destination", pa.string()), pa.field("weight_kg", pa.float64()),
    pa.field("value_usd", pa.float64()), pa.field("volume_teu", pa.float64()),
    pa.field("gics_code", pa.string()), pa.field("gics_description", pa.string()),
    pa.field("place_of_receipt", pa.string()), pa.field("frob", pa.string()),
    pa.field("manifest_number", pa.string()), pa.field("master_bill_of_lading", pa.string()),
    pa.field("bill_type", pa.string()), pa.field("inbond_code", pa.string()),
    pa.field("number_of_containers", pa.int32()), pa.field("weight_original_format", pa.string()),
    pa.field("measurement", pa.string()), pa.field("quantity", pa.string()),
    pa.field("vessel_name", pa.string()), pa.field("vessel_voyage_number", pa.string()),
    pa.field("vessel_imo", pa.string()),
    pa.field("pol_name", pa.string()), pa.field("pol_un_locode", pa.string()),
    pa.field("pol_country", pa.string()), pa.field("pol_lat", pa.float64()),
    pa.field("pol_lon", pa.float64()),
    pa.field("pou_name", pa.string()), pa.field("pou_un_locode", pa.string()),
    pa.field("pou_country", pa.string()), pa.field("pou_lat", pa.float64()),
    pa.field("pou_lon", pa.float64()),
    pa.field("transporter_scac", pa.string()), pa.field("transporter_name", pa.string()),
    pa.field("notify_party_name", pa.string()), pa.field("notify_party_address", pa.string()),
    pa.field("notify_party_scacs", pa.string()),
] + _company_fields("consignee") + _company_fields("shipper"))


ITEM_SCHEMA = pa.schema([
    pa.field("record_id", pa.string()), pa.field("data_source", pa.string()),
    pa.field("shipment_date", pa.date32()), pa.field("item_index", pa.int32()),
    pa.field("description", pa.string()), pa.field("description_full", pa.string()),
    pa.field("hs_code", pa.string()), pa.field("hs_code_primary", pa.string()),
    pa.field("container_marks", pa.string()), pa.field("container_number", pa.string()),
    pa.field("container_type_of_service", pa.string()), pa.field("container_type", pa.string()),
    pa.field("container_quantities", pa.string()), pa.field("quantity", pa.string()),
    pa.field("lcl", pa.bool_()), pa.field("dangerous_goods", pa.bool_()),
])
