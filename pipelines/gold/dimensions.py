from datetime import date, timedelta
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# DIM_SENSOR
# ---------------------------------------------------------------------------

def build_dim_sensor(
    device_df: DataFrame,
    sensor_config_df: DataFrame,
) -> DataFrame:
    """
    Build the Gold dim_sensor dimension.

    Grain:
        One row per physical sensor/device configuration.

    Source:
        devices.yaml
        sensor_config.yaml

    Current implementation:
        SCD Type 1

    The schema intentionally contains effective_from/effective_to fields
    so that the dimension can evolve into SCD Type 2 later.
    """

    # ------------------------------------------------------------------
    # Join physical topology metadata with sensor-type configuration.
    #
    # device_df:
    #     one row per physical sensor/device
    #
    # sensor_config_df:
    #     one row per sensor type
    #
    # Relationship:
    #
    #     device_df.device_type
    #                 =
    #     sensor_config_df.device_type
    # ------------------------------------------------------------------

    dim_sensor = (
        device_df.alias("device")
        .join(
            sensor_config_df.alias("config"),
            on="device_type",
            how="left",
        )
        .select(
            # Natural/business identity
            F.col("device.device_id"),
            F.col("device.device_type"),

            # Plant hierarchy
            F.col("device.plant_id"),
            F.col("device.plant_name"),
            F.col("device.city"),
            F.col("device.country"),
            F.col("device.timezone"),

            # Production line
            F.col("device.line_id"),
            F.col("device.production_line"),
            F.col("device.line_type"),

            # Machine
            F.col("device.machine_id"),
            F.col("device.machine_name"),
            F.col("device.machine_type"),
            F.col("device.manufacturer"),
            F.col("device.rated_capacity_units_per_hour"),

            # Sensor configuration
            F.col("config.sensor_unit"),
            F.col("config.is_safety_critical"),
            F.col("config.sampling_frequency_seconds"),

            # Sensor lifecycle
            F.col("device.sensor_install_date").alias(
                "sensor_install_date"
            ),
            F.col("device.last_calibration_date").alias(
                "last_calibration_date"
            ),
            F.col("device.firmware_version"),

            # Threshold/configuration metadata
            F.col("config.normal_min"),
            F.col("config.normal_max"),
            F.col("config.warning_low"),
            F.col("config.warning_high"),
            F.col("config.critical_low"),
            F.col("config.critical_high"),
            F.col("config.offline_grace_period_seconds"),
        )
    )

    # ------------------------------------------------------------------
    # Generate deterministic surrogate sensor key.
    #
    # device_id is the business/natural key.
    #
    # sensor_key is used by Gold facts and Power BI relationships.
    #
    # xxhash64 gives us a deterministic integer key for the current
    # SCD Type 1 implementation.
    # ------------------------------------------------------------------

    dim_sensor = dim_sensor.withColumn(
        "sensor_key",
        F.xxhash64(F.col("device_id")).cast(LongType()),
    )

    # ------------------------------------------------------------------
    # Current SCD Type 1 implementation.
    #
    # We retain these fields so that the dimension can later become
    # SCD Type 2 without changing the overall dimensional design.
    # ------------------------------------------------------------------

    current_timestamp = F.current_timestamp()

    dim_sensor = (
        dim_sensor
        .withColumn(
            "effective_from",
            current_timestamp,
        )
        .withColumn(
            "effective_to",
            F.lit(None).cast(TimestampType()),
        )
        .withColumn(
            "is_active",
            F.lit(True).cast(BooleanType()),
        )
        .withColumn(
            "is_current",
            F.lit(True).cast(BooleanType()),
        )
    )

    # ------------------------------------------------------------------
    # Reorder columns into the business-friendly dimensional layout.
    # ------------------------------------------------------------------

    dim_sensor = dim_sensor.select(
        "sensor_key",

        "device_id",
        "device_type",

        "plant_id",
        "plant_name",
        "city",
        "country",
        "timezone",

        "line_id",
        "production_line",
        "line_type",

        "machine_id",
        "machine_name",
        "machine_type",
        "manufacturer",
        "rated_capacity_units_per_hour",

        "sensor_unit",
        "is_safety_critical",
        "sampling_frequency_seconds",

        "sensor_install_date",
        "last_calibration_date",
        "firmware_version",

        "normal_min",
        "normal_max",
        "warning_low",
        "warning_high",
        "critical_low",
        "critical_high",

        "offline_grace_period_seconds",

        "is_active",
        "effective_from",
        "effective_to",
        "is_current",
    )

    return dim_sensor


# ---------------------------------------------------------------------------
# DIM_DATE
# ---------------------------------------------------------------------------

def build_dim_date(
    spark: SparkSession,
    start_date: str,
    end_date: str,
) -> DataFrame:
    """
    Build the Gold dim_date dimension.

    Grain:
        One row per calendar date.

    Args:
        spark:
            Active SparkSession.

        start_date:
            Inclusive start date in YYYY-MM-DD format.

        end_date:
            Inclusive end date in YYYY-MM-DD format.
    """

    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    if end < start:
        raise ValueError(
            "end_date must be greater than or equal to start_date"
        )

    number_of_days = (end - start).days + 1

    # ------------------------------------------------------------------
    # Create one row per calendar date.
    # ------------------------------------------------------------------

    dates = [
        (start + timedelta(days=offset),)
        for offset in range(number_of_days)
    ]

    date_df = spark.createDataFrame(
        dates,
        schema=StructType(
            [
                StructField(
                    "full_date",
                    DateType(),
                    False,
                )
            ]
        ),
    )

    # ------------------------------------------------------------------
    # Date attributes
    # ------------------------------------------------------------------

    dim_date = (
        date_df
        .withColumn(
            "date_key",
            F.date_format(
                F.col("full_date"),
                "yyyyMMdd",
            ).cast(IntegerType()),
        )
        .withColumn(
            "year",
            F.year("full_date"),
        )
        .withColumn(
            "quarter",
            F.quarter("full_date"),
        )
        .withColumn(
            "month",
            F.month("full_date"),
        )
        .withColumn(
            "month_name",
            F.date_format(
                "full_date",
                "MMMM",
            ),
        )
        .withColumn(
            "week_of_year",
            F.weekofyear("full_date"),
        )
        .withColumn(
            "day",
            F.dayofmonth("full_date"),
        )
        .withColumn(
            "day_name",
            F.date_format(
                "full_date",
                "EEEE",
            ),
        )
        .withColumn(
            "day_of_week",
            F.dayofweek("full_date"),
        )
        .withColumn(
            "is_weekend",
            F.dayofweek("full_date").isin(1, 7),
        )
        .withColumn(
            "year_month",
            F.date_format(
                "full_date",
                "yyyy-MM",
            ),
        )
        .withColumn(
            "year_month_name",
            F.date_format(
                "full_date",
                "yyyy-MMMM",
            ),
        )
        .withColumn(
            "quarter_name",
            F.concat(
                F.lit("Q"),
                F.quarter("full_date"),
            ),
        )
        .withColumn(
            "day_of_year",
            F.dayofyear("full_date"),
        )
    )

    # ------------------------------------------------------------------
    # Final column order
    # ------------------------------------------------------------------

    dim_date = dim_date.select(
        "date_key",
        "full_date",

        "year",
        "quarter",

        "month",
        "month_name",

        "week_of_year",

        "day",
        "day_name",
        "day_of_week",
        "day_of_year",

        "is_weekend",

        "year_month",
        "year_month_name",
        "quarter_name",
    )

    return dim_date