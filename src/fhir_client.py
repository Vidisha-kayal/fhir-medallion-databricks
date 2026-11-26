"""fhir_client.py: fetch FHIR bundles (pagination + retries) and land them
untouched in the Raw layer, with a manifest of every API call."""
import json
import os
import shutil
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

import requests

from src import config_loader as cl

FHIR_NS = {"f": "http://hl7.org/fhir"}
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


# ---------- search parameters ----------
def day_window(load_date: str):
    """'2026-11-26' -> ('2026-11-26T00:00:00Z', '2026-11-27T00:00:00Z')"""
    d = datetime.strptime(load_date, "%Y-%m-%d")
    fmt = "%Y-%m-%dT00:00:00Z"
    return d.strftime(fmt), (d + timedelta(days=1)).strftime(fmt)


def build_search_params(cfg: dict, since: str, until: str) -> list:
    """List of tuples because _lastUpdated appears twice (ge ... and lt ...)."""
    api = cfg["api"]
    inc = api["incremental_param"]
    return [
        (inc, f"ge{since}"),
        (inc, f"lt{until}"),
        ("_count", api["page_size"]),
        ("_sort", inc),
    ]


# ---------- HTTP with retries ----------
def _get_with_retry(session, url, params, headers, timeout, retries, backoff):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, params=params, headers=headers, timeout=timeout)
            if r.status_code in RETRYABLE_STATUS:
                raise requests.HTTPError(f"retryable status {r.status_code}", response=r)
            r.raise_for_status()
            return r
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
            last_err = e
            resp = getattr(e, "response", None)
            if resp is not None and resp.status_code not in RETRYABLE_STATUS:
                raise  # real client error (400, 404...): fail fast
            if attempt < retries:
                time.sleep(backoff * 2 ** (attempt - 1))
    raise last_err


# ---------- parsing a bundle (only what we need for paging) ----------
def parse_bundle(content: bytes, fmt: str):
    """Return (entry_count, next_url). The raw bytes are NOT modified."""
    if fmt == "json":
        bundle = json.loads(content)
        if bundle.get("resourceType") != "Bundle":
            raise ValueError(f"Expected a Bundle, got: {str(bundle)[:300]}")
        next_url = next((l.get("url") for l in bundle.get("link", [])
                         if l.get("relation") == "next"), None)
        return len(bundle.get("entry", [])), next_url
    if fmt == "xml":
        root = ET.fromstring(content)
        if not root.tag.endswith("Bundle"):
            raise ValueError(f"Expected a Bundle, got root tag {root.tag}")
        next_url = None
        for link in root.findall("f:link", FHIR_NS):
            rel = link.find("f:relation", FHIR_NS)
            if rel is not None and rel.get("value") == "next":
                next_url = link.find("f:url", FHIR_NS).get("value")
        return len(root.findall("f:entry", FHIR_NS)), next_url
    raise ValueError(f"Unsupported format: {fmt}")


# ---------- pagination ----------
def fetch_pages(cfg, resource_name, params, fmt=None, max_pages=None):
    """Generator: yields one dict per page and follows Bundle.link[next]."""
    api = cfg["api"]
    fmt = fmt or api["format"]
    headers = {"Accept": api["accept_headers"][fmt]}
    url = f"{api['base_url']}/{resource_name}"
    page = 0
    with requests.Session() as session:
        while url and (max_pages is None or page < max_pages):
            r = _get_with_retry(
                session, url,
                params if page == 0 else None,   # next-links already carry their params
                headers, api["timeout_seconds"], api["max_retries"], api["backoff_seconds"],
            )
            page += 1
            entry_count, next_url = parse_bundle(r.content, fmt)
            yield {
                "page_number": page,
                "api_url": r.url,                # exact URL called -> api_url_or_params
                "status_code": r.status_code,
                "content": r.content,            # untouched bytes
                "entry_count": entry_count,
                "extracted_at": cl.utc_now(),
                "next_url": next_url,
            }
            url = next_url
            if url:
                time.sleep(api["request_delay_seconds"])


# ---------- fetch + save to Raw ----------
def ingest_resource_to_raw(cfg, resource_name, load_date, max_pages=None,
                           fmt=None, clean_existing=True) -> dict:
    """Fetch one resource for one day and save every page untouched to the Raw volume.
    Never raises on API errors: returns a summary with status SUCCESS/FAILED."""
    fmt = fmt or cfg["api"]["format"]
    max_pages = max_pages if max_pages is not None else cfg["api"]["max_pages_per_run"]
    since, until = day_window(load_date)
    params = build_search_params(cfg, since, until)

    out_dir = cl.raw_dir(cfg, resource_name, load_date, fmt)
    if clean_existing and os.path.isdir(out_dir):
        shutil.rmtree(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    batch_id = cl.new_batch_id()
    started_at = cl.utc_now()
    manifest, total_records, last_next = [], 0, None
    status, error = "SUCCESS", None

    try:
        for p in fetch_pages(cfg, resource_name, params, fmt, max_pages):
            path = cl.raw_file_path(cfg, resource_name, load_date, p["page_number"], fmt)
            with open(path, "wb") as f:
                f.write(p["content"])
            saved_at = cl.utc_now()
            total_records += p["entry_count"]
            last_next = p["next_url"]
            manifest.append({
                "batch_id": batch_id,
                "resource": resource_name,
                "load_date": load_date,
                "page_number": p["page_number"],
                "file_name": os.path.basename(path),
                "file_path": path,
                "api_url": p["api_url"],
                "status_code": p["status_code"],
                "entry_count": p["entry_count"],
                "bytes": len(p["content"]),
                "extracted_at": p["extracted_at"].isoformat(),
                "saved_at": saved_at.isoformat(),
            })
    except Exception as e:  # keep partial progress, report failure to the caller
        status, error = "FAILED", f"{type(e).__name__}: {e}"
    finally:
        with open(os.path.join(out_dir, "_manifest.jsonl"), "w") as mf:
            for row in manifest:
                mf.write(json.dumps(row) + "\n")

    return {
        "batch_id": batch_id,
        "resource": resource_name,
        "load_date": load_date,
        "window_since": since,
        "window_until": until,
        "format": fmt,
        "pages_fetched": len(manifest),
        "records_fetched": total_records,
        "truncated": bool(last_next) and len(manifest) >= max_pages,
        "status": status,
        "error": error,
        "started_at": started_at.isoformat(),
        "ended_at": cl.utc_now().isoformat(),
        "raw_dir": out_dir,
    }