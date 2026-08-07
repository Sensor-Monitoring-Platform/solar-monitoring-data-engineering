from pathlib import Path

import yaml
from pyspark.sql import SparkSession

try:
    from .config import PROJECT_ROOT  # noqa: F401
except ImportError:  # pragma: no cover - direct script execution
    from config import PROJECT_ROOT  # noqa: F401


# load the file
def load_yaml(path):
    with open(Path(path), "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


# flatten device data
def flatten_devices(devices):
    rows = []
    for plant in devices.get("plants", []):
        for line in plant.get("production_lines", []):
            for machine in line.get("machines", []):
                for sensor in machine.get("sensors", []):
                    rows.append(
                        {
                            "device_id": sensor["device_id"],
                            "device_type": sensor["device_type"],
                            "plant_id": plant["plant_id"],
                            "plant_name": plant["plant_name"],
                            "line_id": line["line_id"],
                            "line_name": line["line_name"],
                            "machine_id": machine["machine_id"],
                            "machine_name": machine["machine_name"],
                            "machine_type": machine["machine_type"],
                            "manufacturer": machine["manufacturer"],
                            "firmware_version": sensor["firmware_version"],
                        }
                    )
    return rows


# flatten sensor data
def flatten_sensor_config(config):
    rows = []
    for sensor_type, value in config.get("sensor_types", {}).items():
        rows.append({"device_type": sensor_type, **value})
    return rows


# creatig dataframes
def create_metadata_dataframes(spark: SparkSession, devices, sensor_config):
    device_df = spark.createDataFrame(flatten_devices(devices))
    sensor_df = spark.createDataFrame(flatten_sensor_config(sensor_config))
    return device_df, sensor_df


# broadcasting data
# device_df = broadcast(device_df)
# sensor_df = broadcast(sensor_df)

def build_lookup_dicts(device_df, sensor_df):
    device_rows = device_df.select(
        "device_id",
        "device_type",
        "plant_id",
        "machine_id",
        "firmware_version",
    ).collect()
    sensor_rows = sensor_df.select("device_type", "unit").collect()

    device_lookup = {
        row.device_id: {
            "device_type": row.device_type,
            "plant_id": row.plant_id,
            "machine_id": row.machine_id,
            "firmware_version": row.firmware_version,
        }
        for row in device_rows
    }
    sensor_lookup = {
        row.device_type: {"unit": row.unit}
        for row in sensor_rows
        if row.device_type is not None
    }
    return device_lookup, sensor_lookup