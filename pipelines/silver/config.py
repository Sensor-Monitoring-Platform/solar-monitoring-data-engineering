import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# ------------------------
# Local staging paths
# Spark reads/writes here. When USE_HF_STORAGE is true, main.py syncs
# these to/from the Hugging Face bucket around the run -- same pattern
# as pipelines/bronze/config.py.
# ------------------------

BRONZE_INPUT_PATH = os.getenv("BRONZE_INPUT_PATH", str(PROJECT_ROOT / "bronze"))
SILVER_OUTPUT_PATH = os.getenv("SILVER_OUTPUT_PATH", str(PROJECT_ROOT / "silver"))

DEVICE_METADATA = PROJECT_ROOT / "generators/config/devices.yaml"
SENSOR_METADATA = PROJECT_ROOT / "generators/config/sensor_config.yaml"

# ------------------------
# Hugging Face Storage (S3-compatible bucket)
# Same bucket + credentials the Bronze pipeline uses. bronze/ and silver/
# are just prefixes inside that one bucket.
# ------------------------

USE_HF_STORAGE = os.getenv("USE_HF_STORAGE", "true").lower() == "true"

HF_NAMESPACE = os.getenv("HF_NAMESPACE")
HF_ACCESS_KEY_ID = os.getenv("HF_ACCESS_KEY_ID")
HF_SECRET_ACCESS_KEY = os.getenv("HF_SECRET_ACCESS_KEY")
HF_S3_ENDPOINT = f"https://s3.hf.co/{HF_NAMESPACE}" if HF_NAMESPACE else None

HF_BUCKET_NAME = os.getenv("HF_BUCKET_NAME", "iot-raw")
HF_BRONZE_PREFIX = os.getenv("HF_BRONZE_PREFIX", "bronze/")
HF_SILVER_PREFIX = os.getenv("HF_SILVER_PREFIX", "silver/")

HF_CHECKPOINT_PREFIX = os.getenv("HF_CHECKPOINT_PREFIX", "checkpoint/")

# Bronze's own checkpoint (must match pipelines/bronze/config.py's
# HF_CHECKPOINT_KEY exactly -- Silver reads it, never writes it) is the
# signal for "has there been anything new since Silver last ran". Silver
# doesn't track its own row-level watermark -- see main.py for why a
# full rebuild is unavoidable here, unlike Bronze's incremental reads.
HF_BRONZE_CHECKPOINT_KEY = f"{HF_CHECKPOINT_PREFIX}bronze_watermark.json"
HF_SILVER_CHECKPOINT_KEY = f"{HF_CHECKPOINT_PREFIX}silver_watermark.json"

PROCESSING_JOB_ID = os.getenv("PROCESSING_JOB_ID", "local-run")

# ------------------------
# Spark
# ------------------------

APP_NAME = "Silver Layer Pipeline"
MASTER = "local[*]"
TIMEZONE = "UTC"
SHUFFLE_PARTITIONS = 8

# ------------------------
# Transformation settings
# ------------------------

LATE_ARRIVAL_SECONDS = int(os.getenv("LATE_ARRIVAL_SECONDS", "300"))

# Bound for moving_average / rolling_stddev: "last N seconds of readings
# for this device", not "all history to date". See transformations.py --
# an unbounded expanding window defeats the purpose of a rolling stat for
# drift/anomaly detection once a device has months of history.
ROLLING_WINDOW_SECONDS = int(os.getenv("ROLLING_WINDOW_SECONDS", "900"))  # 15 minutes

UNIT_MAP = {
    "celsius": "celsius",
    "degc": "celsius",
    "°c": "celsius",
    "fahrenheit": "celsius",
    "f": "celsius",
    "bar": "bar",
    "psi": "bar",
    "amperes": "amperes",
    "amps": "amperes",
    "a": "amperes",
    "g_rms": "g_rms",
    "g": "g_rms",
    "ppm": "ppm",
    "percentage": "percentage",
    "percent": "percentage",
    "none": None,
}