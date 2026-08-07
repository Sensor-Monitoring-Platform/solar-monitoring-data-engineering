"""
hf_storage.py

Thin S3-compatible client + sync helpers for Hugging Face Storage Buckets.
Mirrors the client construction already used in
generators/simulate_sensors.py, so the Bronze pipeline talks to the same
bucket the same way the generator does.

Responsibilities:
1. Build a boto3 client pointed at HF's S3-compatible endpoint.
2. List / read raw objects for the reader.
3. Download an existing Delta table (data + _delta_log/) to local disk
   before a run, so appends continue the same table history.
4. Upload a local Delta table back to the bucket after a run.
"""

from pathlib import Path
from typing import Iterator, List

import boto3
from botocore.config import Config as BotoConfig

try:
    from .config import HF_ACCESS_KEY_ID, HF_NAMESPACE, HF_S3_ENDPOINT, HF_SECRET_ACCESS_KEY
except ImportError:  # pragma: no cover - direct script execution
    from config import HF_ACCESS_KEY_ID, HF_NAMESPACE, HF_S3_ENDPOINT, HF_SECRET_ACCESS_KEY


def create_s3_client():
    """Create a boto3 client pointed at Hugging Face's S3-compatible endpoint."""
    if not (HF_NAMESPACE and HF_ACCESS_KEY_ID and HF_SECRET_ACCESS_KEY):
        raise RuntimeError(
            "Missing HF_NAMESPACE / HF_ACCESS_KEY_ID / HF_SECRET_ACCESS_KEY. "
            "Set these env vars, or set USE_HF_STORAGE=false to run against local disk only."
        )

    boto_config = BotoConfig(
        s3={"addressing_style": "path"},
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    )

    client = boto3.client(
        "s3",
        endpoint_url=HF_S3_ENDPOINT,
        aws_access_key_id=HF_ACCESS_KEY_ID,
        aws_secret_access_key=HF_SECRET_ACCESS_KEY,
        region_name="us-east-1",
        config=boto_config,
    )
    return client


def list_objects(s3_client, bucket: str, prefix: str) -> Iterator[str]:
    """Yield every object key under a prefix."""
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]


def list_raw_batches(s3_client, bucket: str, prefix: str) -> List[str]:
    """Return all raw NDJSON/JSON object keys under the raw prefix."""
    return [
        key
        for key in list_objects(s3_client, bucket, prefix)
        if key.lower().endswith((".json", ".ndjson", ".jsonl"))
    ]


def download_prefix(s3_client, bucket: str, prefix: str, local_dir: str) -> int:
    """
    Download every object under `prefix` into `local_dir`, preserving the
    relative path structure below the prefix (including _delta_log/).

    Used to restore the existing Bronze/quarantine Delta table before an
    append run, since GitHub Actions runners start from a clean filesystem
    every time. Returns the number of files downloaded (0 if the prefix
    doesn't exist yet, e.g. on the very first run).
    """
    local_root = Path(local_dir)
    local_root.mkdir(parents=True, exist_ok=True)
    count = 0
    for key in list_objects(s3_client, bucket, prefix):
        relative = key[len(prefix):].lstrip("/")
        if not relative:
            continue
        dest = local_root / relative
        dest.parent.mkdir(parents=True, exist_ok=True)
        s3_client.download_file(bucket, key, str(dest))
        count += 1
    return count


def upload_directory(s3_client, bucket: str, local_dir: str, prefix: str) -> int:
    """
    Upload every file under `local_dir` to `bucket` under `prefix`,
    preserving relative structure (including _delta_log/).

    Note: this re-uploads the whole local table on every run rather than
    diffing against what's already in the bucket. That's simple and
    correct, at the cost of re-uploading unchanged Parquet files each
    time. Fine at portfolio scale; for a larger table, switch to
    uploading only files newer than the last download, or mount the
    bucket with an S3A/Hadoop connector instead of this download/upload
    wrapper.
    """
    local_root = Path(local_dir)
    if not local_root.exists():
        return 0
    count = 0
    for path in local_root.rglob("*"):
        if path.is_file():
            key = f"{prefix.rstrip('/')}/{path.relative_to(local_root).as_posix()}"
            s3_client.upload_file(str(path), bucket, key)
            count += 1
    return count