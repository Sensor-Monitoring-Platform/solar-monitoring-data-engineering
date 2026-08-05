import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# ------------------------
# Metadata
# ------------------------

DEVICE_METADATA = PROJECT_ROOT / "generators/config/devices.yaml"

SENSOR_METADATA = PROJECT_ROOT / "generators/config/sensor_config.yaml"

# ------------------------
# Bronze IO
# ------------------------

RAW_INPUT_PATH = os.getenv("RAW_INPUT_PATH", str(PROJECT_ROOT / "raw"))

BRONZE_OUTPUT_PATH = os.getenv("BRONZE_OUTPUT_PATH", str(PROJECT_ROOT / "bronze"))

QUARANTINE_PATH = os.getenv("QUARANTINE_PATH", str(PROJECT_ROOT / "quarantine"))

CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", str(PROJECT_ROOT / "checkpoint"))

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