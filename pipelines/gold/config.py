import os
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Local staging paths
SILVER_INPUT_PATH = os.getenv("SILVER_INPUT_PATH", str(PROJECT_ROOT / "silver"))
GOLD_OUTPUT_PATH = os.getenv("GOLD_OUTPUT_PATH", str(PROJECT_ROOT / "gold"))

# Metadata
DEVICE_METADATA = PROJECT_ROOT / "generators/config/devices.yaml"
SENSOR_METADATA = PROJECT_ROOT / "generators/config/sensor_config.yaml"

# Hugging Face Storage
USE_HF_STORAGE = (os.getenv("USE_HF_STORAGE", "true").lower() == "true")
HF_NAMESPACE = os.getenv("HF_NAMESPACE")
HF_ACCESS_KEY_ID = os.getenv("HF_ACCESS_KEY_ID")
HF_SECRET_ACCESS_KEY = os.getenv("HF_SECRET_ACCESS_KEY")
HF_S3_ENDPOINT = (f"https://s3.hf.co/{HF_NAMESPACE}" if HF_NAMESPACE else None)
HF_BUCKET_NAME = os.getenv("HF_BUCKET_NAME","iot-raw")

# Storage prefixes
HF_SILVER_PREFIX = os.getenv("HF_SILVER_PREFIX","silver/")
HF_GOLD_PREFIX = os.getenv("HF_GOLD_PREFIX","gold/")

# Checkpoint
HF_CHECKPOINT_PREFIX = os.getenv("HF_CHECKPOINT_PREFIX","checkpoint/")
HF_SILVER_CHECKPOINT_KEY = f"{HF_CHECKPOINT_PREFIX}silver_watermark.json"
HF_GOLD_CHECKPOINT_KEY = f"{HF_CHECKPOINT_PREFIX}gold_watermark.json"

# Processing
PROCESSING_JOB_ID = os.getenv("PROCESSING_JOB_ID","gold-local-run")

# Spark
APP_NAME = "Gold Layer Pipeline"
MASTER = "local[*]"
TIMEZONE = "UTC"
SHUFFLE_PARTITIONS = int(os.getenv("SHUFFLE_PARTITIONS", "8"))

# Writing mode
GOLD_WRITE_MODE = os.getenv("GOLD_WRITE_MODE","overwrite")

# Default business aggregation levels.
GOLD_TIME_GRANULARITY = os.getenv("GOLD_TIME_GRANULARITY","hour")

# Optional limit for how much historical Silver data Gold reads (0 means all available Silver history.)
GOLD_LOOKBACK_DAYS = int(os.getenv("GOLD_LOOKBACK_DAYS", "0"))