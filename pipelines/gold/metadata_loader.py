from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession


def load_yaml(path: str | Path) -> dict[str, Any]:
    """
    Load a YAML file and return its contents as a dictionary.

    Gold metadata is sourced from:
        1. Device topology metadata
        2. Sensor type configuration
    """
    with Path(path).open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def flatten_devices(devices: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Flatten the hierarchical device topology:
        Plant
            -> Production Line
                -> Machine
                    -> Sensor

    into one row per sensor/device.
    This produces the descriptive metadata required by dim_sensor.
    """
    rows = []

    for plant in devices.get("plants", []):
        for line in plant.get("production_lines", []):
            for machine in line.get("machines", []):
                for sensor in machine.get("sensors", []):
                    rows.append(
                        {
                            # Sensor identity
                            "device_id": sensor["device_id"],
                            "device_type": sensor["device_type"],

                            # Plant hierarchy
                            "plant_id": plant["plant_id"],
                            "plant_name": plant["plant_name"],
                            "city": plant["city"],
                            "country": plant["country"],
                            "timezone": plant["timezone"],
                            "plant_commissioned_date": plant.get(
                                "commissioned_date"
                            ),

                            # Production line
                            "line_id": line["line_id"],
                            "production_line": line["line_name"],
                            "line_type": line["line_type"],

                            # Machine
                            "machine_id": machine["machine_id"],
                            "machine_name": machine["machine_name"],
                            "machine_type": machine["machine_type"],
                            "manufacturer": machine["manufacturer"],
                            "machine_install_date": machine.get(
                                "install_date"
                            ),
                            "rated_capacity_units_per_hour": machine.get(
                                "rated_capacity_units_per_hour"
                            ),

                            # Sensor configuration
                            "sensor_install_date": sensor.get(
                                "install_date"
                            ),
                            "last_calibration_date": sensor.get(
                                "last_calibration_date"
                            ),
                            "firmware_version": sensor.get(
                                "firmware_version"
                            ),
                        }
                    )

    return rows


def flatten_sensor_config(sensor_config: dict[str, Any],) -> list[dict[str, Any]]:
    """
    Flatten sensor type configuration.
    Produces one row per sensor type containing:
        - unit
        - sampling frequency
        - safety classification
        - normal range
        - warning thresholds
        - critical thresholds
        - operational configuration
    """
    rows = []

    for sensor_type, config in sensor_config.get(
        "sensor_types", {}
    ).items():
        normal = config.get("normal_range") or {}
        warning = config.get("warning_threshold") or {}
        critical = config.get("critical_threshold") or {}

        rows.append(
            {
                "device_type": sensor_type,

                # Measurement configuration
                "sensor_unit": config.get("unit"),
                "sampling_frequency_seconds": config.get(
                    "sampling_frequency_seconds"
                ),
                "is_safety_critical": bool(
                    config.get("is_safety_critical", False)
                ),

                # Normal operating range
                "normal_min": normal.get("min"),
                "normal_max": normal.get("max"),

                # Warning thresholds
                "warning_low": warning.get("low"),
                "warning_high": warning.get("high"),

                # Critical thresholds
                "critical_low": critical.get("low"),
                "critical_high": critical.get("high"),

                # Sensor behaviour
                "noise_std_dev": config.get("noise_std_dev"),
                "drift_rate_per_hour": config.get(
                    "drift_rate_per_hour"
                ),
                "offline_grace_period_seconds": config.get(
                    "offline_grace_period_seconds"
                ),
            }
        )

    return rows


def create_metadata_dataframes(spark: SparkSession,devices: dict[str, Any],sensor_config: dict[str, Any],) -> tuple[DataFrame, DataFrame]:
    """
    Create Spark DataFrames from the flattened metadata.

    Returns:
        device_df
            One row per physical sensor/device.

        sensor_config_df
            One row per sensor type.
    """
    device_rows = flatten_devices(devices)
    sensor_config_rows = flatten_sensor_config(sensor_config)

    device_df = spark.createDataFrame(device_rows)
    sensor_config_df = spark.createDataFrame(sensor_config_rows)

    return device_df, sensor_config_df