import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# ------------------------
# Metadata
# ------------------------

DEVICE_METADATA = PROJECT_ROOT / "generators/config/devices.yaml"

SENSOR_METADATA = PROJECT_ROOT / "generators/config/sensor_config.yaml"

# ------------------------
# Local staging paths
# Spark always reads/writes Delta on local disk. When USE_HF_STORAGE is
# true, main.py syncs these directories to/from the Hugging Face bucket
# immediately before and after the Spark job runs, so on a fresh GitHub
# Actions runner the "local" table is really just a working copy of the
# table that lives in the bucket.
# ------------------------

RAW_INPUT_PATH = os.getenv("RAW_INPUT_PATH", str(PROJECT_ROOT / "raw"))

BRONZE_OUTPUT_PATH = os.getenv("BRONZE_OUTPUT_PATH", str(PROJECT_ROOT / "bronze"))

QUARANTINE_PATH = os.getenv("QUARANTINE_PATH", str(PROJECT_ROOT / "quarantine"))

CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", str(PROJECT_ROOT / "checkpoint"))

# ------------------------
# Hugging Face Storage (S3-compatible bucket)
# Same bucket + credentials the generator (generators/simulate_sensors.py)
# already uploads raw data to. raw/, bronze/ and bronze_quarantine/ are
# just prefixes inside that one bucket.
# ------------------------

USE_HF_STORAGE = os.getenv("USE_HF_STORAGE", "true").lower() == "true"

HF_NAMESPACE = os.getenv("HF_NAMESPACE")

HF_ACCESS_KEY_ID = os.getenv("HF_ACCESS_KEY_ID")

HF_SECRET_ACCESS_KEY = os.getenv("HF_SECRET_ACCESS_KEY")

HF_S3_ENDPOINT = f"https://s3.hf.co/{HF_NAMESPACE}" if HF_NAMESPACE else None

HF_BUCKET_NAME = os.getenv("HF_BUCKET_NAME", "iot-raw")

HF_RAW_PREFIX = os.getenv("HF_RAW_PREFIX", "raw/")

HF_BRONZE_PREFIX = os.getenv("HF_BRONZE_PREFIX", "bronze/")

HF_QUARANTINE_PREFIX = os.getenv("HF_QUARANTINE_PREFIX", "bronze_quarantine/")

# ------------------------
# Spark
# ------------------------

APP_NAME = "Bronze Layer Pipeline"

MASTER = "local[*]"

SHUFFLE_PARTITIONS = 8

TIMEZONE = "UTC"

# ------------------------
# Validation
# ------------------------

ALLOWED_TIME_TOLERANCE_MINUTES = 5

PROCESSING_JOB_ID = os.getenv("PROCESSING_JOB_ID", "local-run")