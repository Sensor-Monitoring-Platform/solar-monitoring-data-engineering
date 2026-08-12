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
# immediately before and after the Spark job runs.
# ------------------------

RAW_INPUT_PATH = os.getenv("RAW_INPUT_PATH", str(PROJECT_ROOT / "raw"))

BRONZE_OUTPUT_PATH = os.getenv("BRONZE_OUTPUT_PATH", str(PROJECT_ROOT / "bronze"))

QUARANTINE_PATH = os.getenv("QUARANTINE_PATH", str(PROJECT_ROOT / "quarantine"))

CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", str(PROJECT_ROOT / "checkpoint"))

# ------------------------
# Hugging Face Storage (S3-compatible bucket)
# Same bucket + credentials the generator (generators/simulate_sensors.py)
# already uploads raw data to. raw/, bronze/, bronze_quarantine/ and
# checkpoint/ are just prefixes inside that one bucket.
# ------------------------

USE_HF_STORAGE = os.getenv("USE_HF_STORAGE", "true").lower() == "true"

HF_NAMESPACE = os.getenv("HF_NAMESPACE")

HF_ACCESS_KEY_ID = os.getenv("HF_ACCESS_KEY_ID")

HF_SECRET_ACCESS_KEY = os.getenv("HF_SECRET_ACCESS_KEY")

HF_S3_ENDPOINT = f"https://s3.hf.co/{HF_NAMESPACE}" if HF_NAMESPACE else None

HF_BUCKET_NAME = os.getenv("HF_BUCKET_NAME", "sensor-bucket")

HF_RAW_PREFIX = os.getenv("HF_RAW_PREFIX", "raw/")

HF_BRONZE_PREFIX = os.getenv("HF_BRONZE_PREFIX", "bronze/")

HF_QUARANTINE_PREFIX = os.getenv("HF_QUARANTINE_PREFIX", "bronze_quarantine/")

HF_CHECKPOINT_PREFIX = os.getenv("HF_CHECKPOINT_PREFIX", "checkpoint/")

HF_CHECKPOINT_KEY = f"{HF_CHECKPOINT_PREFIX}bronze_watermark.json"

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

# Now that the reader only ever looks at raw objects newer than the last
# checkpoint (see hf_storage.read_checkpoint / write_checkpoint), this no
# longer needs to be wide enough to tolerate re-reading your entire raw/
# history. 60 minutes gives comfortable margin for however long a batch
# sits in the bucket between generation and the next bronze run.
ALLOWED_TIME_TOLERANCE_MINUTES = int(os.getenv("ALLOWED_TIME_TOLERANCE_MINUTES", "60"))

PROCESSING_JOB_ID = os.getenv("PROCESSING_JOB_ID", "local-run")