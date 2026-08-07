import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from pyspark.sql import DataFrame, SparkSession

try:
    from .schemas import BRONZE_OUTPUT_SCHEMA, QUARANTINE_SCHEMA
except ImportError:  # pragma: no cover - direct script execution
    from schemas import BRONZE_OUTPUT_SCHEMA, QUARANTINE_SCHEMA


# ---------------------------------------------------------------------------
# Source providers — each yields (source_file_name, lines) tuples.
# The validation/parsing loop below doesn't care which one supplied a line.
# ---------------------------------------------------------------------------

def _resolve_local_files(input_path: str) -> List[Path]:
    path = Path(input_path)
    if path.is_file():
        return [path]
    if not path.exists():
        return []
    return sorted(
        [
            file_path
            for file_path in path.rglob("*")
            if file_path.is_file() and file_path.suffix.lower() in {".json", ".jsonl", ".ndjson"}
        ]
    )


def _iter_local_sources(input_path: str) -> Iterable[Tuple[str, Iterable[str]]]:
    """Yield (source_name, lines) for every local NDJSON file under input_path."""
    for file_path in _resolve_local_files(input_path):
        with open(file_path, "r", encoding="utf-8") as handle:
            yield str(file_path), handle.readlines()


def _iter_s3_sources(s3_client, bucket: str, prefix: str) -> Iterable[Tuple[str, Iterable[str]]]:
    """Yield (source_name, lines) for every raw object in the HF bucket."""
    try:
        from .hf_storage import list_raw_batches
    except ImportError:  # pragma: no cover - direct script execution
        from hf_storage import list_raw_batches

    for key in list_raw_batches(s3_client, bucket, prefix):
        body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
        yield f"s3://{bucket}/{key}", body.decode("utf-8").splitlines()


# ---------------------------------------------------------------------------
# Parsing / validation (unchanged from the original implementation)
# ---------------------------------------------------------------------------

def _parse_timestamp(value) -> datetime | None:
    if not value:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    return None


def _build_record_hash(record: Dict) -> str:
    payload = "|".join(
        [
            str(record.get("device_id", "")),
            str(record.get("timestamp", "")),
            str(record.get("reading_value", "")),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _validate_record(record: Dict, device_lookup: Dict, sensor_lookup: Dict, tolerance_minutes: int, now: datetime) -> List[str]:
    errors: List[str] = []

    if not record.get("device_id"):
        errors.append("device_id is missing")
    else:
        device_metadata = device_lookup.get(record["device_id"])
        if not device_metadata:
            errors.append("device_id is not present in metadata")
        elif device_metadata.get("device_type") != record.get("device_type"):
            errors.append("device_type does not match device metadata")

    if not record.get("device_type"):
        errors.append("device_type is missing")
    elif record["device_type"] not in sensor_lookup:
        errors.append("device_type is not configured")

    if not record.get("plant_id"):
        errors.append("plant_id is missing")

    if not record.get("machine_id"):
        errors.append("machine_id is missing")

    reading_value = record.get("reading_value")
    if reading_value is None:
        errors.append("reading_value is missing")
    else:
        try:
            float(reading_value)
        except (TypeError, ValueError):
            errors.append("reading_value is not numeric")

    reading_unit = record.get("reading_unit")
    if not reading_unit:
        errors.append("reading_unit is missing")
    else:
        expected_unit = None
        sensor_metadata = sensor_lookup.get(record.get("device_type"))
        if sensor_metadata:
            expected_unit = sensor_metadata.get("unit")
        if expected_unit and reading_unit != expected_unit:
            errors.append(f"reading_unit {reading_unit} does not match expected unit {expected_unit}")

    parsed_timestamp = _parse_timestamp(record.get("timestamp"))
    if parsed_timestamp is None:
        errors.append("timestamp is not valid")
    else:
        future_limit = now + timedelta(minutes=tolerance_minutes)
        past_limit = now - timedelta(minutes=tolerance_minutes)
        if parsed_timestamp > future_limit:
            errors.append("timestamp is in the future")
        if parsed_timestamp < past_limit:
            errors.append("timestamp is too old")

    return errors


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def read_raw_data(
    spark: SparkSession,
    device_lookup: Dict,
    sensor_lookup: Dict,
    processing_job_id: str,
    tolerance_minutes: int = 5,
    input_path: str = None,
    use_hf_storage: bool = False,
    s3_client=None,
    hf_bucket: str = None,
    hf_prefix: str = "raw/",
) -> Tuple[DataFrame, DataFrame]:
    """
    Read raw NDJSON records either from the HF bucket (use_hf_storage=True)
    or from local disk (use_hf_storage=False, reading input_path). Every
    record goes through the same validation/hashing/quarantine logic
    regardless of source.
    """
    if use_hf_storage:
        if s3_client is None or hf_bucket is None:
            raise ValueError("s3_client and hf_bucket are required when use_hf_storage=True")
        sources = _iter_s3_sources(s3_client, hf_bucket, hf_prefix)
    else:
        if not input_path:
            raise ValueError("input_path is required when use_hf_storage=False")
        sources = _iter_local_sources(input_path)

    valid_rows: List[Dict] = []
    quarantine_rows: List[Dict] = []
    seen_hashes = set()
    now = datetime.now(timezone.utc)
    files_seen = 0

    for source_file_name, lines in sources:
        files_seen += 1
        for line_number, line in enumerate(lines, 1):
            payload = line.strip()
            if not payload:
                continue

            try:
                parsed = json.loads(payload)
            except json.JSONDecodeError as exc:
                quarantine_rows.append(
                    {
                        "source_file_name": source_file_name,
                        "raw_payload": payload,
                        "error_reason": f"Malformed JSON at line {line_number}: {exc}",
                        "processing_timestamp": now,
                        "processing_job_id": processing_job_id,
                        "is_corrupt": True,
                    }
                )
                continue

            if not isinstance(parsed, dict):
                quarantine_rows.append(
                    {
                        "source_file_name": source_file_name,
                        "raw_payload": payload,
                        "error_reason": "Record is not a JSON object",
                        "processing_timestamp": now,
                        "processing_job_id": processing_job_id,
                        "is_corrupt": True,
                    }
                )
                continue

            reading = parsed.get("reading", {}) or {}
            if not isinstance(reading, dict):
                reading = {}

            row = {
                "device_id": parsed.get("device_id"),
                "device_type": parsed.get("device_type"),
                "plant_id": parsed.get("plant_id"),
                "machine_id": parsed.get("machine_id"),
                "timestamp": parsed.get("timestamp"),
                "reading_value": reading.get("value"),
                "reading_unit": reading.get("unit"),
                "firmware_version": parsed.get("firmware_version"),
                "quality_flag": parsed.get("quality_flag", "OK"),
                "ingestion_timestamp": parsed.get("ingestion_timestamp"),
                "source_file_name": source_file_name,
            }

            errors = _validate_record(row, device_lookup, sensor_lookup, tolerance_minutes, now)
            if errors:
                quarantine_rows.append(
                    {
                        "source_file_name": source_file_name,
                        "raw_payload": payload,
                        "error_reason": "; ".join(errors),
                        "processing_timestamp": now,
                        "processing_job_id": processing_job_id,
                        "is_corrupt": True,
                    }
                )
                continue

            record_hash = _build_record_hash(row)
            if record_hash in seen_hashes:
                quarantine_rows.append(
                    {
                        "source_file_name": source_file_name,
                        "raw_payload": payload,
                        "error_reason": "Duplicate record",
                        "processing_timestamp": now,
                        "processing_job_id": processing_job_id,
                        "is_corrupt": True,
                    }
                )
                continue

            seen_hashes.add(record_hash)
            parsed_timestamp = _parse_timestamp(row["timestamp"])
            ingestion_timestamp = _parse_timestamp(row["ingestion_timestamp"])
            valid_rows.append(
                {
                    "device_id": row["device_id"],
                    "device_type": row["device_type"],
                    "plant_id": row["plant_id"],
                    "machine_id": row["machine_id"],
                    "timestamp": parsed_timestamp,
                    "reading_value": float(row["reading_value"]),
                    "reading_unit": row["reading_unit"],
                    "firmware_version": row["firmware_version"],
                    "quality_flag": row["quality_flag"],
                    "ingestion_timestamp": ingestion_timestamp or now,
                    "source_file_name": row["source_file_name"],
                    "processing_timestamp": now,
                    "processing_job_id": processing_job_id,
                    "bronze_version": "1.0",
                    "record_hash": record_hash,
                    "is_corrupt": False,
                    "error_reason": None,
                }
            )

    if files_seen == 0:
        return (
            spark.createDataFrame([], schema=BRONZE_OUTPUT_SCHEMA),
            spark.createDataFrame([], schema=QUARANTINE_SCHEMA),
        )

    bronze_df = spark.createDataFrame(valid_rows, schema=BRONZE_OUTPUT_SCHEMA)
    quarantine_df = spark.createDataFrame(quarantine_rows, schema=QUARANTINE_SCHEMA)
    return bronze_df, quarantine_df