"""
simulate_sensors.py — IoT Sensor Data Generator
=================================================
Generates realistic manufacturing sensor readings and uploads
them as raw JSON files to Hugging Face Storage Buckets (S3-compatible).

Bronze layer: raw, untransformed data — exactly what a device would send.

Usage:
    # Local run (needs env vars set)
    python simulate_sensors.py

    # GitHub Actions (env vars come from secrets)
    python simulate_sensors.py --duration-minutes 5 --batch-size 500

Environment Variables Required:
    HF_ACCESS_KEY_ID      — Hugging Face S3 Access Key (starts with HFAK...)
    HF_SECRET_ACCESS_KEY  — Hugging Face S3 Secret Key
    HF_NAMESPACE          — Your HF username or org name
    HF_BUCKET_NAME        — Name of your Storage Bucket (e.g. "iot-bronze")
"""

import argparse
import json
import logging
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

import boto3
import yaml
from botocore.config import Config as BotoConfig

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("sensor_gen")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_DIR = SCRIPT_DIR / "config"

# Anomaly injection probabilities (per reading)
ANOMALY_PROB = 0.03          # 3 % chance a reading is anomalous
SPIKE_PROB   = 0.50          # of anomalies, 50 % are spikes
DRIFT_PROB   = 0.30          # 30 % are gradual drift
NULL_PROB    = 0.10          # 10 % are null/missing readings
FLATLINE_PROB = 0.10         # 10 % are flatline (stuck sensor)

# Late-arriving data
LATE_ARRIVAL_PROB = 0.02     # 2 % chance a reading arrives "late"
LATE_ARRIVAL_MAX_SECONDS = 300

# Device offline simulation
DEVICE_OFFLINE_PROB = 0.005  # 0.5 % chance a device goes offline per batch


# ---------------------------------------------------------------------------
# Config loaders
# ---------------------------------------------------------------------------
def load_yaml(path: Path) -> dict:
    """Load a YAML config file."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def flatten_devices(devices_cfg: dict) -> list[dict]:
    """
    Walk the plant → line → machine → sensor hierarchy
    and return a flat list of sensor dicts, each carrying its
    parent IDs for context.
    """
    sensors = []
    for plant in devices_cfg["plants"]:
        for line in plant["production_lines"]:
            for machine in line["machines"]:
                for sensor in machine["sensors"]:
                    sensors.append({
                        "plant_id": plant["plant_id"],
                        "plant_name": plant["plant_name"],
                        "line_id": line["line_id"],
                        "machine_id": machine["machine_id"],
                        "device_id": sensor["device_id"],
                        "device_type": sensor["device_type"],
                        "firmware_version": sensor["firmware_version"],
                    })
    return sensors


# ---------------------------------------------------------------------------
# Reading generation
# ---------------------------------------------------------------------------
class SensorSimulator:
    """
    Generates realistic sensor readings with:
      - Normal Gaussian noise around a baseline
      - Occasional anomalies (spikes, drift, null, flatline)
      - Late-arriving timestamps
      - Device offline simulation
    """

    def __init__(self, sensor_config: dict):
        self.sensor_config = sensor_config
        self._drift_accumulators: dict[str, float] = {}
        self._flatline_values: dict[str, float] = {}
        self._offline_devices: set[str] = set()

    def _get_type_config(self, device_type: str) -> dict | None:
        return self.sensor_config.get("sensor_types", {}).get(device_type)

    def generate_reading(self, sensor: dict, timestamp: datetime) -> dict | None:
        """
        Generate one raw sensor reading dict.
        Returns None if the device is currently offline.
        """
        device_id = sensor["device_id"]
        device_type = sensor["device_type"]
        type_cfg = self._get_type_config(device_type)

        # --- Device offline? ---
        if device_id in self._offline_devices:
            # 30 % chance it comes back online each call
            if random.random() < 0.30:
                self._offline_devices.discard(device_id)
                log.debug("Device %s back online", device_id)
            else:
                return None  # still offline, no reading

        # New offline event?
        if random.random() < DEVICE_OFFLINE_PROB:
            self._offline_devices.add(device_id)
            log.info("Device %s went OFFLINE", device_id)
            return None

        # --- access_control is event-driven, not numeric ---
        if device_type == "access_control":
            return self._access_event(sensor, timestamp)

        if type_cfg is None or type_cfg.get("normal_range") is None:
            return None

        # --- Numeric reading ---
        normal = type_cfg["normal_range"]
        baseline = (normal["min"] + normal["max"]) / 2
        noise_std = type_cfg.get("noise_std_dev", 1.0)

        # Normal value
        value = random.gauss(baseline, noise_std)

        # Anomaly injection
        quality_flag = "OK"
        if random.random() < ANOMALY_PROB:
            value, quality_flag = self._inject_anomaly(
                device_id, device_type, type_cfg, value
            )

        # Clamp to physically plausible (sensor can't read negative pressure etc.)
        if value is not None:
            value = round(value, 4)

        # Late-arriving timestamp
        reading_ts = timestamp
        if random.random() < LATE_ARRIVAL_PROB:
            delay = random.randint(30, LATE_ARRIVAL_MAX_SECONDS)
            reading_ts = timestamp - timedelta(seconds=delay)
            quality_flag = "LATE"

        ingestion_ts = datetime.now(timezone.utc)

        return {
            "device_id": device_id,
            "device_type": device_type,
            "plant_id": sensor["plant_id"],
            "machine_id": sensor["machine_id"],
            "timestamp": reading_ts.isoformat(),
            "reading": {
                "value": value,
                "unit": type_cfg["unit"],
            },
            "firmware_version": sensor["firmware_version"],
            "quality_flag": quality_flag,
            "ingestion_timestamp": ingestion_ts.isoformat(),
        }

    def _inject_anomaly(
        self, device_id: str, device_type: str, type_cfg: dict, value: float
    ) -> tuple[float | None, str]:
        """Pick an anomaly type and return (modified_value, quality_flag)."""
        roll = random.random()

        if roll < SPIKE_PROB:
            # Spike: jump far outside warning range
            crit = type_cfg.get("critical_threshold", {})
            direction = random.choice([-1, 1])
            spike_magnitude = abs(crit.get("high", 100) - crit.get("low", 0)) * 0.3
            return value + direction * spike_magnitude, "SPIKE"

        elif roll < SPIKE_PROB + DRIFT_PROB:
            # Drift: accumulate a small offset over time
            drift_rate = type_cfg.get("drift_rate_per_hour", 0.05)
            current_drift = self._drift_accumulators.get(device_id, 0.0)
            current_drift += drift_rate * random.uniform(0.5, 2.0)
            self._drift_accumulators[device_id] = current_drift
            return value + current_drift, "DRIFT"

        elif roll < SPIKE_PROB + DRIFT_PROB + NULL_PROB:
            # Null reading — sensor glitch
            return None, "NULL_READING"

        else:
            # Flatline: sensor stuck on one value
            if device_id not in self._flatline_values:
                self._flatline_values[device_id] = value
            return self._flatline_values[device_id], "FLATLINE"

    def _access_event(self, sensor: dict, timestamp: datetime) -> dict:
        """Generate a badge-scan event (no numeric value)."""
        badge_ids = [f"EMP-{i:04d}" for i in range(1, 51)]
        ingestion_ts = datetime.now(timezone.utc)

        return {
            "device_id": sensor["device_id"],
            "device_type": "access_control",
            "plant_id": sensor["plant_id"],
            "machine_id": sensor["machine_id"],
            "timestamp": timestamp.isoformat(),
            "reading": {
                "value": None,
                "unit": None,
                "badge_id": random.choice(badge_ids),
                "access_granted": random.random() > 0.05,
            },
            "firmware_version": sensor["firmware_version"],
            "quality_flag": "OK",
            "ingestion_timestamp": ingestion_ts.isoformat(),
        }


# ---------------------------------------------------------------------------
# S3 (Hugging Face Buckets) uploader
# ---------------------------------------------------------------------------
def create_s3_client():
    """
    Create a boto3 S3 client pointed at Hugging Face Storage Buckets.
    Requires env vars: HF_ACCESS_KEY_ID, HF_SECRET_ACCESS_KEY, HF_NAMESPACE
    """
    namespace = os.environ["HF_NAMESPACE"]
    endpoint_url = f"https://s3.hf.co/{namespace}"

    boto_config = BotoConfig(
        s3={"addressing_style": "path"},
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    )

    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=os.environ["HF_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["HF_SECRET_ACCESS_KEY"],
        region_name="us-east-1",
        config=boto_config,
    )
    log.info("S3 client created → endpoint: %s", endpoint_url)
    return client


def ensure_bucket_exists(s3_client, bucket_name: str):
    """Create the bucket if it doesn't already exist."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        log.info("Bucket '%s' already exists", bucket_name)
    except s3_client.exceptions.ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            log.info("Creating bucket '%s'...", bucket_name)
            s3_client.create_bucket(Bucket=bucket_name)
            log.info("Bucket '%s' created successfully", bucket_name)
        else:
            raise


def upload_batch(s3_client, bucket_name: str, readings: list[dict], batch_id: str):
    """
    Upload a batch of readings as a single NDJSON file.
    Path convention: bronze/year=YYYY/month=MM/day=DD/hour=HH/{batch_id}.json

    This partitioning scheme lets downstream tools (DuckDB, Spark)
    do partition-pruning on date columns without a catalog.
    """
    if not readings:
        log.warning("Empty batch %s — skipping upload", batch_id)
        return

    now = datetime.now(timezone.utc)
    partition_path = (
        f"raw/"
        f"year={now.strftime('%Y')}/"
        f"month={now.strftime('%m')}/"
        f"day={now.strftime('%d')}/"
        f"hour={now.strftime('%H')}"
    )
    object_key = f"{partition_path}/{batch_id}.json"

    # Stamp each record with its source file name
    for r in readings:
        r["source_file_name"] = object_key

    # NDJSON: one JSON object per line
    body = "\n".join(json.dumps(r, default=str) for r in readings)

    s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )

    log.info(
        "Uploaded %d readings → s3://%s/%s (%.1f KB)",
        len(readings),
        bucket_name,
        object_key,
        len(body) / 1024,
    )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def run(
    duration_minutes: int,
    batch_size: int,
    tick_seconds: int,
    dry_run: bool = False,
):
    """
    Main generator loop.

    Args:
        duration_minutes: How long to generate data (0 = one batch only).
        batch_size:       Max readings per upload file.
        tick_seconds:     Seconds between batch flushes.
        dry_run:          If True, print readings to stdout instead of uploading.
    """
    # Load configs
    devices_cfg = load_yaml(CONFIG_DIR / "devices.yaml")
    sensor_cfg  = load_yaml(CONFIG_DIR / "sensor_config.yaml")

    sensors = flatten_devices(devices_cfg)
    log.info("Loaded %d sensors from devices.yaml", len(sensors))

    simulator = SensorSimulator(sensor_cfg)

    # S3 client (skip if dry-run)
    s3_client = None
    bucket_name = None
    if not dry_run:
        s3_client = create_s3_client()
        bucket_name = os.environ.get("HF_BUCKET_NAME", "iot-bronze")
        ensure_bucket_exists(s3_client, bucket_name)

    # Timing
    if duration_minutes <= 0:
        total_ticks = 1
    else:
        total_ticks = max(1, (duration_minutes * 60) // tick_seconds)

    total_readings = 0
    total_batches = 0

    log.info(
        "Starting generation: %d ticks, %d sensors, tick=%ds, batch_max=%d",
        total_ticks, len(sensors), tick_seconds, batch_size,
    )

    for tick in range(total_ticks):
        tick_start = time.monotonic()
        now = datetime.now(timezone.utc)
        batch: list[dict] = []

        for sensor in sensors:
            reading = simulator.generate_reading(sensor, now)
            if reading is not None:
                batch.append(reading)

                # Flush when batch is full
                if len(batch) >= batch_size:
                    batch_id = f"batch_{uuid.uuid4().hex[:12]}"
                    if dry_run:
                        _print_batch(batch, batch_id)
                    else:
                        upload_batch(s3_client, bucket_name, batch, batch_id)
                    total_readings += len(batch)
                    total_batches += 1
                    batch = []

        # Flush remaining
        if batch:
            batch_id = f"batch_{uuid.uuid4().hex[:12]}"
            if dry_run:
                _print_batch(batch, batch_id)
            else:
                upload_batch(s3_client, bucket_name, batch, batch_id)
            total_readings += len(batch)
            total_batches += 1

        # Sleep until next tick (but not on last tick)
        elapsed = time.monotonic() - tick_start
        if tick < total_ticks - 1:
            sleep_time = max(0, tick_seconds - elapsed)
            if sleep_time > 0:
                log.debug("Sleeping %.1fs until next tick", sleep_time)
                time.sleep(sleep_time)

    log.info(
        "Generation complete: %d readings in %d batches",
        total_readings, total_batches,
    )

    # Set GitHub Actions output (used by downstream workflows)
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as f:
            f.write(f"total_readings={total_readings}\n")
            f.write(f"total_batches={total_batches}\n")


def _print_batch(batch: list[dict], batch_id: str):
    """Dry-run: print a batch to stdout."""
    print(f"\n--- Batch: {batch_id} ({len(batch)} readings) ---")
    for r in batch[:3]:  # show first 3
        print(json.dumps(r, indent=2, default=str))
    if len(batch) > 3:
        print(f"  ... and {len(batch) - 3} more readings")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="IoT Sensor Data Generator → Hugging Face Storage Buckets",
    )
    parser.add_argument(
        "--duration-minutes",
        type=int,
        default=0,
        help="How long to run (0 = generate one batch and exit). Default: 0",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Max readings per NDJSON file. Default: 500",
    )
    parser.add_argument(
        "--tick-seconds",
        type=int,
        default=30,
        help="Seconds between generation ticks. Default: 30",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print readings to stdout instead of uploading to S3",
    )

    args = parser.parse_args()

    # Validate env vars (unless dry-run)
    if not args.dry_run:
        required_vars = ["HF_ACCESS_KEY_ID", "HF_SECRET_ACCESS_KEY", "HF_NAMESPACE"]
        missing = [v for v in required_vars if not os.environ.get(v)]
        if missing:
            log.error("Missing required environment variables: %s", ", ".join(missing))
            log.error("Set these or use --dry-run for local testing")
            sys.exit(1)

    run(
        duration_minutes=args.duration_minutes,
        batch_size=args.batch_size,
        tick_seconds=args.tick_seconds,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
