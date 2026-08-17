"""
hf_storage.py

Thin S3-compatible client + sync helpers for Hugging Face Storage Buckets.
Same pattern as pipelines/bronze/hf_storage.py.

Silver has no row-level watermark -- it always rebuilds fully from the
complete Bronze table when it runs (rolling windows/streak counts/
forward-fill all need full history to be correct). What IS tracked here
is a run-level checkpoint: Bronze's own watermark, read (never written)
to detect "has anything changed since Silver last ran" so an unchanged
Bronze table doesn't trigger a needless full rebuild every schedule tick.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterator, Optional

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

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

    return boto3.client(
        "s3",
        endpoint_url=HF_S3_ENDPOINT,
        aws_access_key_id=HF_ACCESS_KEY_ID,
        aws_secret_access_key=HF_SECRET_ACCESS_KEY,
        region_name="us-east-1",
        config=boto_config,
    )


def list_objects(s3_client, bucket: str, prefix: str) -> Iterator[str]:
    """Yield every object key under a prefix."""
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]


def _is_checksum_sidecar(name: str) -> bool:
    """
    Hadoop's local filesystem writer creates a hidden .crc checksum
    sidecar next to every file it writes. Not real Delta table content --
    see pipelines/bronze/hf_storage.py for the full explanation of why
    this matters (ChecksumException from a stale sidecar synced across
    separate runs).
    """
    return name.startswith(".") and name.endswith(".crc")


def download_prefix(s3_client, bucket: str, prefix: str, local_dir: str) -> int:
    """
    Download every object under `prefix` into `local_dir`, preserving the
    relative path structure below the prefix (including _delta_log/).
    Returns the number of files downloaded (0 if the prefix doesn't exist).
    """
    local_root = Path(local_dir)
    local_root.mkdir(parents=True, exist_ok=True)
    count = 0
    for key in list_objects(s3_client, bucket, prefix):
        relative = key[len(prefix):].lstrip("/")
        if not relative or _is_checksum_sidecar(Path(relative).name):
            continue
        dest = local_root / relative
        dest.parent.mkdir(parents=True, exist_ok=True)
        s3_client.download_file(bucket, key, str(dest))
        count += 1
    return count


def upload_directory(
    s3_client, bucket: str, local_dir: str, prefix: str, modified_after: Optional[float] = None
) -> int:
    """
    Upload every file under `local_dir` to `bucket` under `prefix`,
    preserving relative structure (including _delta_log/).

    If `modified_after` is given, only files with an mtime strictly newer
    than it are uploaded -- see pipelines/bronze/hf_storage.py for the
    full explanation. Silver does a full mode("overwrite") each run, so
    most files genuinely are new, but old _delta_log commit/checkpoint
    files from many runs back (Delta doesn't delete superseded files
    until a VACUUM) would otherwise still get needlessly re-uploaded
    every time.
    """
    local_root = Path(local_dir)
    if not local_root.exists():
        return 0
    count = 0
    for path in local_root.rglob("*"):
        if not path.is_file() or _is_checksum_sidecar(path.name):
            continue
        if modified_after is not None and path.stat().st_mtime <= modified_after:
            continue
        key = f"{prefix.rstrip('/')}/{path.relative_to(local_root).as_posix()}"
        s3_client.upload_file(str(path), bucket, key)
        count += 1
    return count


# ---------------------------------------------------------------------------
# Checkpoint (run-level, not row-level -- see module docstring)
# ---------------------------------------------------------------------------

def read_checkpoint(s3_client, bucket: str, checkpoint_key: str) -> Optional[Dict]:
    """
    Read a checkpoint JSON from the bucket. Returns None if it doesn't
    exist yet (e.g. Bronze hasn't run, or this is Silver's first run),
    rather than raising.
    """
    try:
        body = s3_client.get_object(Bucket=bucket, Key=checkpoint_key)["Body"].read()
        return json.loads(body.decode("utf-8"))
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code in ("NoSuchKey", "404"):
            return None
        raise


def write_checkpoint(
    s3_client, bucket: str, checkpoint_key: str, watermark: datetime, processing_job_id: str, files_processed: int
) -> None:
    """Persist Silver's checkpoint -- the Bronze watermark that Silver has now fully processed."""
    payload = {
        "last_processed_utc": watermark.astimezone(timezone.utc).isoformat(),
        "processing_job_id": processing_job_id,
        "files_processed": files_processed,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    s3_client.put_object(
        Bucket=bucket,
        Key=checkpoint_key,
        Body=json.dumps(payload, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def parse_checkpoint_watermark(checkpoint: Optional[Dict]) -> Optional[datetime]:
    """Extract the watermark datetime from a checkpoint dict, or None if there isn't one."""
    if not checkpoint or not checkpoint.get("last_processed_utc"):
        return None
    return datetime.fromisoformat(checkpoint["last_processed_utc"])