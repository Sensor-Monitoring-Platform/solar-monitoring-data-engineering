from pyspark.sql import Window
from pyspark.sql import functions as F

try:
    from .config import LATE_ARRIVAL_SECONDS, UNIT_MAP
except ImportError:  # pragma: no cover
    from config import LATE_ARRIVAL_SECONDS, UNIT_MAP


def _standardize_unit(value, raw_unit):
    if value is None or raw_unit is None:
        return None

    normalized = str(raw_unit).strip().lower()
    canonical = UNIT_MAP.get(normalized, normalized)

    if canonical == "celsius" and normalized in {"fahrenheit", "f"}:
        return (float(value) - 32.0) * 5.0 / 9.0
    return float(value)


def enrich_with_metadata(df, device_df, sensor_df):
    device_df = device_df.select(
        "device_id",
        "plant_id",
        "plant_name",
        "production_line",
        "machine_name",
        "machine_type",
        "manufacturer",
        "firmware_version",
    )
    sensor_df = sensor_df.select(
        "device_type",
        "unit",
        "is_safety_critical",
        "sampling_frequency_seconds",
        "normal_range",
        "warning_threshold",
        "critical_threshold",
    )

    return df.join(device_df, on="device_id", how="left").join(sensor_df, on="device_type", how="left")


def normalize_units(df):
    return df.withColumn(
        "reading_value",
        F.when(F.col("reading_value").isNull(), None)
        .otherwise(
            F.udf(lambda v, unit: _standardize_unit(v, unit), "double")(F.col("reading_value"), F.col("unit"))
        ),
    ).withColumn(
        "reading_unit",
        F.when(F.col("unit").isNull(), F.col("reading_unit")).otherwise(F.col("unit")),
    )


def normalize_timestamps(df):
    df = df.withColumn("timestamp", F.to_utc_timestamp(F.col("timestamp"), "UTC"))
    df = df.withColumn("event_date", F.to_date(F.col("timestamp")))
    df = df.withColumn("event_hour", F.hour(F.col("timestamp")))
    df = df.withColumn("event_minute", F.minute(F.col("timestamp")))
    df = df.withColumn("event_week", F.weekofyear(F.col("timestamp")))
    df = df.withColumn("event_month", F.month(F.col("timestamp")))
    df = df.withColumn("event_year", F.year(F.col("timestamp")))
    df = df.withColumn("day_of_week", F.dayofweek(F.col("timestamp")))
    return df


def flag_late_arrivals(df):
    return df.withColumn(
        "is_late_arrival",
        F.when(
            F.col("timestamp").cast("long") < (F.current_timestamp().cast("long") - LATE_ARRIVAL_SECONDS),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )


def fill_missing_values(df):
    device_window = Window.partitionBy("device_id").orderBy("timestamp")
    return df.withColumn(
        "reading_value",
        F.when(
            F.col("reading_value").isNull(),
            F.last("reading_value", ignorenulls=True).over(device_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
        ).otherwise(F.col("reading_value")),
    )


def detect_outliers(df):
    return df.withColumn(
        "is_outlier",
        F.when(
            (F.col("reading_value").isNull()) | (F.col("normal_range").isNull()),
            F.lit(False),
        )
        .otherwise(
            (F.col("reading_value") < F.expr("normal_range.min"))
            | (F.col("reading_value") > F.expr("normal_range.max"))
        ),
    )


def derive_reading_status(df):
    return df.withColumn(
        "reading_status",
        F.when(F.col("warning_threshold").isNull(), F.lit("UNKNOWN"))
        .when(
            (F.col("reading_value") < F.expr("warning_threshold.low"))
            | (F.col("reading_value") > F.expr("warning_threshold.high")),
            F.lit("WARNING"),
        )
        .when(
            (F.col("reading_value") < F.expr("critical_threshold.low"))
            | (F.col("reading_value") > F.expr("critical_threshold.high")),
            F.lit("CRITICAL"),
        )
        .otherwise(F.lit("NORMAL")),
    )


def derive_sensor_health(df):
    return df.withColumn(
        "sensor_health",
        F.when(F.col("is_outlier"), F.lit("UNSTABLE"))
        .when(F.col("reading_value").isNull(), F.lit("UNKNOWN"))
        .otherwise(F.lit("GOOD")),
    )


def derive_machine_condition(df):
    return df.withColumn(
        "machine_operating_condition",
        F.when(F.col("is_outlier"), F.lit("WARNING"))
        .when(F.col("reading_status") == "CRITICAL", F.lit("FAULT"))
        .otherwise(F.lit("NORMAL")),
    )


def add_feature_columns(df):
    device_window = Window.partitionBy("device_id").orderBy("timestamp")
    rolling_window = Window.partitionBy("device_id").orderBy("timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    df = df.withColumn("moving_average", F.avg("reading_value").over(rolling_window))
    df = df.withColumn("rolling_stddev", F.stddev("reading_value").over(rolling_window))
    df = df.withColumn("delta_previous", F.col("reading_value") - F.lag("reading_value", 1).over(device_window))
    df = df.withColumn(
        "percent_change",
        F.when(F.lag("reading_value", 1).over(device_window).isNull(), F.lit(0.0))
        .otherwise((F.col("delta_previous") / F.lag("reading_value", 1).over(device_window)) * 100.0),
    )
    df = df.withColumn(
        "reading_velocity",
        F.when(F.lag("timestamp", 1).over(device_window).isNull(), F.lit(0.0))
        .otherwise(
            F.col("delta_previous") / (
                F.unix_timestamp(F.col("timestamp")) - F.unix_timestamp(F.lag("timestamp", 1).over(device_window))
            )
        ),
    )
    df = df.withColumn(
        "reading_acceleration",
        F.col("reading_velocity") - F.lag("reading_velocity", 1).over(device_window),
    )
    df = df.withColumn(
        "consecutive_nulls",
        F.when(F.col("reading_value").isNull(), F.coalesce(F.lag("consecutive_nulls", 1).over(device_window), F.lit(0)) + 1).otherwise(F.lit(0)),
    )
    df = df.withColumn(
        "consecutive_outliers",
        F.when(F.col("is_outlier"), F.coalesce(F.lag("consecutive_outliers", 1).over(device_window), F.lit(0)) + 1).otherwise(F.lit(0)),
    )
    return df


def prepare_silver(df, device_df, sensor_df):
    df = enrich_with_metadata(df, device_df, sensor_df)
    df = normalize_units(df)
    df = normalize_timestamps(df)
    df = flag_late_arrivals(df)
    df = fill_missing_values(df)
    df = detect_outliers(df)
    df = derive_reading_status(df)
    df = derive_sensor_health(df)
    df = derive_machine_condition(df)
    df = add_feature_columns(df)
    return df.drop("unit", "normal_range", "warning_threshold", "critical_threshold")
