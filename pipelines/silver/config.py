import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_INPUT_PATH = os.getenv("BRONZE_INPUT_PATH", str(PROJECT_ROOT / "bronze"))
SILVER_OUTPUT_PATH = os.getenv("SILVER_OUTPUT_PATH", str(PROJECT_ROOT / "silver"))

DEVICE_METADATA = PROJECT_ROOT / "generators/config/devices.yaml"
SENSOR_METADATA = PROJECT_ROOT / "generators/config/sensor_config.yaml"

APP_NAME = "Silver Layer Pipeline"
MASTER = "local[*]"
TIMEZONE = "UTC"
SHUFFLE_PARTITIONS = 8

LATE_ARRIVAL_SECONDS = int(os.getenv("LATE_ARRIVAL_SECONDS", "300"))

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
