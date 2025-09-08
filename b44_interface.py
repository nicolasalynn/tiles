#!/usr/bin/env python3
"""
Base44 -> PBS poller (minimal S3 integration)

Adds:
- job_id = entity['id']
- Upload results JSON/CSV to s3://<S3_BUCKET>/<S3_PREFIX>/outputs_<job_id>/
- Return the HTTPS (or presigned) URLs to Base44
- Status transitions: queued -> running -> completed/failed
"""

import os
import sys
import json
import time
import re
import csv
import hashlib
import random
from pathlib import Path
from typing import Iterable, List, Dict, Tuple

import requests
import boto3
from botocore.exceptions import ClientError

from main_handler import tiles_pipeline

# ======== CONFIG ========
APP_ID   = os.getenv("B44_APP_ID",   "68832b4596c03b454b3e49e6")
API_KEY  = os.getenv("B44_API_KEY",  "REPLACE_WITH_YOUR_API_KEY")
ENTITY   = os.getenv("B44_TILES_ENTITY",   "MutationAnalysis")
API_BASE = os.getenv("B44_API_BASE", "https://app.base44.com/api")

# S3 config
S3_BUCKET           = os.getenv("TILES_S3_BUCKET", "tiles-runs")
S3_PREFIX           = os.getenv("TILES_S3_PREFIX", "processed-runs").strip("/")
S3_PUBLIC           = (os.getenv("S3_PUBLIC", "1") != "0")  # set "0" if bucket is private
S3_PRESIGN_SECONDS  = int(os.getenv("S3_PRESIGN_SECONDS", "0"))  # e.g., 604800 for 7 days (used if not public)

# Where to save inputs + local outputs
DEFAULT_ROOT  = Path.home() / "mutagenesis_inputs"
DOWNLOAD_DIR  = Path(os.getenv("B44_INPUT_DIR", str(DEFAULT_ROOT)))
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

# After a successful download, update status -> 'queued'
UPDATE_TO_QUEUED = (os.getenv("B44_UPDATE_TO_QUEUED", "1") != "0")

# State file to avoid re-downloading / reprocessing the same record
STATE_FILE = Path.home() / ".base44_pull_state.json"
# ========================

HEADERS = {"api_key": API_KEY, "Content-Type": "application/json"}
ENTITIES_URL = f"{API_BASE}/apps/{APP_ID}/entities/{ENTITY}"

# ---------- HTTP helpers ----------
def backoff_sleep(attempt: int):
    time.sleep((2 ** attempt) + random.random())

def http_get(url, *, params=None, stream=False, timeout=30, retries=3):
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, stream=stream, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception:
            if attempt == retries:
                raise
            backoff_sleep(attempt)

# ---------- state & misc ----------
def load_state() -> Dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"downloaded_ids": {}}

def save_state(state: Dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))

def sanitize_filename(name: str) -> str:
    name = name.strip().replace("\x00", "")
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name)[:200] or "file.dat"

def pick_filename(entity: Dict) -> str:
    fname = (entity.get("filename") or "").strip()
    if not fname:
        url = entity.get("file_url", "")
        h = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]
        fname = f"upload_{h}.dat"
    return sanitize_filename(fname)

def get_processing(limit: int = 1000) -> List[Dict]:
    """Plain GET (no params), filter client-side for status == 'processing' (skip samples)."""
    r = http_get(ENTITIES_URL, timeout=30)
    data = r.json()
    items = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
    proc = [e for e in items
            if str(e.get("status", "")).lower() == "processing" and not e.get("is_sample")]
    return proc[:limit]

def download(url: str, dst: Path) -> None:
    tmp = dst.with_suffix(dst.suffix + ".part")
    with http_get(url, stream=True, timeout=300) as r:
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    tmp.replace(dst)  # atomic move

def update_entity(entity_id: str, payload: Dict) -> Dict:
    url = f"{ENTITIES_URL}/{entity_id}"
    r = requests.put(url, headers=HEADERS, json=payload, timeout=60)
    r.raise_for_status()
    return r.json()

def print_table(rows: Iterable[Dict]) -> None:
    rows = list(rows)
    if not rows:
        print("No entities with status=processing."); return
    print(f"\n{'ID':<26} | {'Filename':<40} | {'Status':<10} | File URL")
    print("-" * 120)
    for e in rows:
        print(f"{e.get('id',''):<26} | {str(e.get('filename',''))[:40]:40} | "
              f"{e.get('status',''):<10} | {e.get('file_url','')}")

# ---------- S3 helpers ----------
def _bucket_region(s3, bucket: str) -> str:
    try:
        resp = s3.get_bucket_location(Bucket=bucket)
        loc  = resp.get("LocationConstraint")
        return "us-east-1" if loc in (None, "") else loc
    except ClientError:
        # Fallback to session region if we can't query
        return boto3.session.Session().region_name or "us-east-1"

def s3_upload(local_path: Path, bucket: str, key: str,
              *, public: bool, presign_seconds: int) -> str:
    s3 = boto3.client("s3")

    def _region():
        try:
            resp = s3.get_bucket_location(Bucket=bucket)
            loc  = resp.get("LocationConstraint")
            return "us-east-1" if loc in (None, "") else loc
        except ClientError:
            return boto3.session.Session().region_name or "us-east-1"

    # Try upload WITHOUT ACL first (compatible with "Bucket owner enforced")
    try:
        s3.upload_file(str(local_path), bucket, key)
    except ClientError as e:
        # Some environments still need ACLs (rare). If the first attempt failed
        # and the error is NOT "ACLs disabled", try once with ACL.
        code = e.response.get("Error", {}).get("Code", "")
        if code in {"AccessControlListNotSupported", "InvalidRequest"}:
            # ACLs are disabled -> re-raise (we already tried the correct path)
            raise
        # Try with ACL if caller asked for public and bucket supports ACLs
        if public:
            s3.upload_file(str(local_path), bucket, key, ExtraArgs={"ACL": "public-read"})
        else:
            raise

    region = _region()

    # Build default virtual-hosted URL
    url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"

    # If the bucket is private and caller wants a presigned URL, generate it
    if not public and presign_seconds > 0:
        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=presign_seconds,
        )
    return url

# ---------- your dummy pipeline ----------
def run_dummy_pipeline(input_path: Path) -> Tuple[bool, List[str], List[Dict], Dict]:
    """
    Minimal placeholder for your real processing.
    Returns:
      success, errors, rows (for CSV), meta (for JSON)
    """
    started = time.time()
    errors: List[str] = []
    rows: List[Dict] = []

    try:
        text = input_path.read_text(encoding="utf-8", errors="replace")
        lines = text.splitlines()
        for i, line in enumerate(lines, start=1):
            rows.append({
                "line_number": i,
                "length": len(line),
                "has_comma": "," in line,
                "preview": line[:40],
            })
        success = True
    except Exception as e:
        success = False
        errors.append(f"processing error: {e!s}")

    finished = time.time()
    meta = {
        "run_successful": True,
        "error_messages": "",
        "total_mutations": 573,
        "splice_disrupting_mutations": 10,
        "tss_disrupting_mutations": 4, 
        "potential_epitopes": 127,
        "high_pathogenicity_count": 42,
        "processing_time": 120
        }

    return success, errors, rows, meta


def write_run_artifacts(job_id: str, out_dir: Path,
                        *, success: bool, errors: List[str], rows: List[Dict]) -> Tuple[Path, Path]:
    """
    Write minimal JSON + CSV artifacts. Return (json_path, csv_path).
    JSON: {'run_successful': <bool>, 'error_messages': [..]}
    CSV:  header from first row keys, then values
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / f"results_{job_id}.json"
    csv_path  = out_dir / f"results_{job_id}.csv"

    # JSON (basic; we overwrite with rich meta below)
    meta = {"run_successful": bool(success), "error_messages": errors}
    json_path.write_text(json.dumps(meta, indent=2))

    # CSV with proper quoting
    if rows:
        hdr = list(rows[0].keys())
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=hdr)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in hdr})
    else:
        csv_path.write_text("message\nno data rows\n")

    return json_path, csv_path

# ---------- main ----------
def main() -> None:
    if not API_KEY or "REPLACE_WITH_YOUR_API_KEY" in API_KEY:
        print("ERROR: Set your API key (env B44_API_KEY or edit the script).", file=sys.stderr)
        sys.exit(2)

    state = load_state()
    downloaded_ids = state.get("downloaded_ids", {})

    entities = get_processing(limit=100)
    print(f"Found {len(entities)} processing candidate(s).")
    print_table(entities)

    new_downloads = 0
    for e in entities:
        job_id = str(e.get("id") or "").strip()   # <-- your job_id
        furl   = e.get("file_url")
        if not job_id or not furl:
            continue
        if job_id in downloaded_ids:
            continue

        fname = pick_filename(e)
        dst   = DOWNLOAD_DIR / fname

        print(f"\n-> Downloading {fname}  (job_id={job_id})")
        try:
            # 1) download
            download(furl, dst)
            print(f"   Saved to: {dst}")

            # 2) mark queued (optional)
            if UPDATE_TO_QUEUED:
                try:
                    update_entity(job_id, {"status": "queued"})
                    print("   Status updated to: queued")
                except Exception as ex:
                    print(f"   NOTE: could not update status to queued: {ex}")

            # 3) mark running (recommended)
            try:
                update_entity(job_id, {"status": "running"})
                print("   Status updated to: running")
            except Exception as ex:
                print(f"   NOTE: could not update status to running: {ex}")

            # 4) process
            success, errors, rows, meta = tiles_pipeline(dst, job_id=job_id)

            # 5) write artifacts locally (overwrite JSON with rich meta)
            out_dir = DOWNLOAD_DIR / f"outputs_{job_id}"
            json_path, csv_path = write_run_artifacts(
                job_id, out_dir,
                success=meta["run_successful"],
                errors=meta["error_messages"],
                rows=rows,
            )
            Path(json_path).write_text(json.dumps(meta, indent=2))
            print(f"   Wrote artifacts: {json_path.name}, {csv_path.name}")

            # 6) upload to S3 under outputs_<job_id>/ and build URLs
            base_key = f"{S3_PREFIX}/outputs_{job_id}".strip("/")
            json_key = f"{base_key}/results_{job_id}.json"
            csv_key  = f"{base_key}/results_{job_id}.csv"

            json_url = s3_upload(json_path, S3_BUCKET, json_key,
                                 public=S3_PUBLIC, presign_seconds=S3_PRESIGN_SECONDS)
            csv_url  = s3_upload(csv_path,  S3_BUCKET, csv_key,
                                 public=S3_PUBLIC, presign_seconds=S3_PRESIGN_SECONDS)

            print(f"   Uploaded:\n      JSON -> {json_url}\n      CSV  -> {csv_url}")

            # 7) update entity in Base44 with result URLs
            payload = {
                "status": "completed" if success else "failed",
                "results_json_url": json_url,
                "results_csv_url": csv_url,
            }
            if errors:
                payload["error_message"] = "; ".join(errors)[:1000]

            update_entity(job_id, payload)
            print(f"   Entity updated: {payload['status']}")

            # 8) remember we've handled this id
            new_downloads += 1
            downloaded_ids[job_id] = {"filename": fname, "saved_to": str(dst), "ts": int(time.time())}
            save_state({"downloaded_ids": downloaded_ids})

        except Exception as ex:
            print(f"   ERROR processing job_id={job_id}: {ex}")
            try:
                update_entity(job_id, {"status": "failed", "error_message": str(ex)[:1000]})
                print("   Marked entity as failed.")
            except Exception as ex2:
                print(f"   NOTE: could not mark failed: {ex2}")

    if new_downloads == 0:
        print("\nNo new files to download.")
    else:
        print(f"\nDownloaded, processed, and uploaded {new_downloads} file(s). Local root: {DOWNLOAD_DIR}")

if __name__ == "__main__":
    main()