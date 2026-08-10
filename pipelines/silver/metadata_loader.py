from pathlib import Path

import yaml
from pyspark.sql import SparkSession


def load_yaml(path):
    with open(Path(path), "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


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
                            "production_line": line["line_name"],
                            "line_id": line["line_id"],
                            "machine_id": machine["machine_id"],
                            "machine_name": machine["machine_name"],
                            "machine_type": machine["machine_type"],
                            "manufacturer": machine["manufacturer"],
                            "firmware_version": sensor["firmware_version"],
                        }
                    )
    return rows


def flatten_sensor_config(config):
    rows = []
    for sensor_type, value in config.get("sensor_types", {}).items():
        normal = value.get("normal_range") or {}
        warning = value.get("warning_threshold") or {}
        critical = value.get("critical_threshold") or {}

        rows.append(
            {
                "device_type": sensor_type,
                "unit": value.get("unit"),
                "is_safety_critical": bool(value.get("is_safety_critical", False)),
                "sampling_frequency_seconds": value.get("sampling_frequency_seconds"),
                "normal_min": normal.get("min"),
                "normal_max": normal.get("max"),
                "warning_low": warning.get("low"),
                "warning_high": warning.get("high"),
                "critical_low": critical.get("low"),
                "critical_high": critical.get("high"),
                "noise_std_dev": value.get("noise_std_dev"),
                "drift_rate_per_hour": value.get("drift_rate_per_hour"),
                "offline_grace_period_seconds": value.get("offline_grace_period_seconds"),
            }
        )
    return rows


def create_metadata_dataframes(spark: SparkSession, devices, sensor_config):
    device_df = spark.createDataFrame(flatten_devices(devices))
    sensor_df = spark.createDataFrame(flatten_sensor_config(sensor_config))
    return device_df, sensor_df


def build_metadata_lookup(device_df, sensor_df):
    device_lookup = {
        row.device_id: {
            "plant_name": row.plant_name,
            "production_line": row.production_line,
            "machine_name": row.machine_name,
            "machine_type": row.machine_type,
            "manufacturer": row.manufacturer,
            "firmware_version": row.firmware_version,
        }
        for row in device_df.select(
            "device_id",
            "plant_name",
            "production_line",
            "machine_name",
            "machine_type",
            "manufacturer",
            "firmware_version",
        ).collect()
    }

    sensor_lookup = {
        row.device_type: {
            "unit": row.unit,
            "normal_min": row.normal_min,
            "normal_max": row.normal_max,
            "warning_low": row.warning_low,
            "warning_high": row.warning_high,
            "critical_low": row.critical_low,
            "critical_high": row.critical_high,
            "is_safety_critical": row.is_safety_critical,
            "sampling_frequency_seconds": row.sampling_frequency_seconds,
        }
        for row in sensor_df.select(
            "device_type",
            "unit",
            "normal_min",
            "normal_max",
            "warning_low",
            "warning_high",
            "critical_low",
            "critical_high",
            "is_safety_critical",
            "sampling_frequency_seconds",
        ).collect()
    }

    return device_lookup, sensor_lookup
