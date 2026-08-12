from pyspark.sql import Window
from pyspark.sql import functions as F

try:
    from .config import LATE_ARRIVAL_SECONDS, ROLLING_WINDOW_SECONDS, UNIT_MAP
except ImportError:  # pragma: no cover
    from config import LATE_ARRIVAL_SECONDS, ROLLING_WINDOW_SECONDS, UNIT_MAP


def _standardize_unit(value, raw_unit):
    if value is None or raw_unit is None:
        return None

    normalized = str(raw_unit).strip().lower()
    canonical = UNIT_MAP.get(normalized, normalized)

    if canonical == "celsius" and normalized in {"fahrenheit", "f"}:
        return (float(value) - 32.0) * 5.0 / 9.0
    return float(value)


def enrich_with_metadata(df, device_df, sensor_df):
    # plant_id and firmware_version are deliberately NOT selected here --
    # Bronze's own df already has both (plant_id from the raw reading,
    # firmware_version as reported by the device that produced THIS
    # reading). Pulling them again from device_df would create two
    # columns with the identical name, which Spark tolerates right up
    # until you try to write the DataFrame -- Delta/Parquet writers
    # reject duplicate column names at write time. device_df is only
    # for genuinely new enrichment columns.
    device_df = device_df.select(
        "device_id",
        "plant_name",
        "production_line",
        "machine_name",
        "machine_type",
        "manufacturer",
    )
    # sensor_df's columns are flat (normal_min/normal_max/warning_low/...),
    # not nested structs -- see metadata_loader.flatten_sensor_config().
    # The previous version of this function selected "normal_range" /
    # "warning_threshold" / "critical_threshold", which don't exist under
    # those names anywhere in this DataFrame and would fail immediately
    # with an AnalysisException.
    sensor_df = sensor_df.select(
        "device_type",
        "unit",
        "is_safety_critical",
        "sampling_frequency_seconds",
        "normal_min",
        "normal_max",
        "warning_low",
        "warning_high",
        "critical_low",
        "critical_high",
    )

    return df.join(device_df, on="device_id", how="left").join(sensor_df, on="device_type", how="left")


def normalize_units(df):
    # Convert FROM the unit the reading actually arrived in (reading_unit)
    # TO the sensor's canonical unit (unit, from sensor_config.yaml) --
    # the previous version passed `unit` (the canonical target) as the
    # "unit to convert from", so the Fahrenheit-check inside
    # _standardize_unit could never be true regardless of input.
    #
    # In today's data this branch is close to a no-op in practice: Bronze
    # already quarantines any record whose reading_unit doesn't match the
    # sensor's expected unit (see pipelines/bronze/reader.py), so a
    # genuinely mismatched unit shouldn't reach Silver at all. Kept as
    # defense-in-depth for if that Bronze rule is ever relaxed, or a
    # sensor type is configured to legitimately accept more than one unit.
    udf = F.udf(_standardize_unit, "double")
    return df.withColumn(
        "reading_value",
        F.when(F.col("reading_value").isNull(), None).otherwise(
            udf(F.col("reading_value"), F.col("reading_unit"))
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
    df = df.withColumn("event_day", F.dayofmonth(F.col("timestamp")))
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
    # Only forward-fill sensor types that actually have a continuous
    # numeric reading. access_control (badge scans) has reading_value =
    # NULL by design -- there's no "previous numeric value" to carry
    # forward, and doing so would fabricate a number for an event-driven
    # sensor that was never supposed to have one. unit IS NULL in
    # sensor_config.yaml specifically for this kind of sensor (see
    # enrich_with_metadata), so it's the signal to use here rather than
    # hardcoding "access_control" by name.
    device_window = Window.partitionBy("device_id").orderBy("timestamp").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    filled = F.last("reading_value", ignorenulls=True).over(device_window)
    return df.withColumn(
        "reading_value",
        F.when(F.col("unit").isNull(), F.col("reading_value"))
        .when(F.col("reading_value").isNull(), filled)
        .otherwise(F.col("reading_value")),
    )


def detect_outliers(df):
    return df.withColumn(
        "is_outlier",
        F.when(
            F.col("reading_value").isNull() | F.col("normal_min").isNull(),
            F.lit(False),
        ).otherwise(
            (F.col("reading_value") < F.col("normal_min")) | (F.col("reading_value") > F.col("normal_max"))
        ),
    )


def derive_reading_status(df):
    # Check CRITICAL before WARNING. Critical thresholds are always the
    # wider band (e.g. temperature_process: warning >190, critical >210),
    # so a critical reading also satisfies the warning condition -- the
    # previous when()-chain checked warning first, which meant a truly
    # critical reading got classified as WARNING and never reached the
    # critical branch at all. Most-severe-first is required for a
    # when().when() chain like this since it short-circuits on the first
    # true condition.
    return df.withColumn(
        "reading_status",
        F.when(F.col("warning_low").isNull() | F.col("critical_low").isNull(), F.lit("UNKNOWN"))
        .when(
            (F.col("reading_value") < F.col("critical_low")) | (F.col("reading_value") > F.col("critical_high")),
            F.lit("CRITICAL"),
        )
        .when(
            (F.col("reading_value") < F.col("warning_low")) | (F.col("reading_value") > F.col("warning_high")),
            F.lit("WARNING"),
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
    # Same most-severe-first fix as derive_reading_status: CRITICAL status
    # implies is_outlier is also true (critical is outside the normal
    # range too), so checking is_outlier first meant a critical reading
    # could never reach the FAULT branch.
    return df.withColumn(
        "machine_operating_condition",
        F.when(F.col("reading_status") == "CRITICAL", F.lit("FAULT"))
        .when(F.col("is_outlier"), F.lit("WARNING"))
        .otherwise(F.lit("NORMAL")),
    )


def add_feature_columns(df):
    device_window = Window.partitionBy("device_id").orderBy("timestamp")

    # Time-bounded rolling window (default 15 min, see
    # config.ROLLING_WINDOW_SECONDS) instead of an expanding window over
    # ALL history. rangeBetween operates on the ORDER BY value itself
    # (here, unix seconds), not row count, so it stays correct across
    # devices with different sampling frequencies. The previous
    # implementation used rowsBetween(unboundedPreceding, currentRow),
    # which is a cumulative average since the beginning of time -- it
    # becomes less and less sensitive to recent drift as history grows,
    # which defeats the point of a rolling stat for anomaly detection.
    rolling_window = (
        Window.partitionBy("device_id")
        .orderBy(F.col("timestamp").cast("long"))
        .rangeBetween(-ROLLING_WINDOW_SECONDS, 0)
    )

    df = df.withColumn("moving_average", F.avg("reading_value").over(rolling_window))
    df = df.withColumn("rolling_stddev", F.stddev("reading_value").over(rolling_window))

    prev_value = F.lag("reading_value", 1).over(device_window)
    prev_timestamp = F.lag("timestamp", 1).over(device_window)

    df = df.withColumn("delta_previous", F.col("reading_value") - prev_value)
    df = df.withColumn(
        "percent_change",
        F.when(prev_value.isNull() | (prev_value == 0), F.lit(0.0)).otherwise(
            (F.col("delta_previous") / prev_value) * 100.0
        ),
    )
    df = df.withColumn(
        "reading_velocity",
        F.when(prev_timestamp.isNull(), F.lit(0.0)).otherwise(
            F.col("delta_previous") / (F.unix_timestamp(F.col("timestamp")) - F.unix_timestamp(prev_timestamp))
        ),
    )
    df = df.withColumn(
        "reading_acceleration",
        F.col("reading_velocity") - F.lag("reading_velocity", 1).over(device_window),
    )

    # Consecutive-streak counts ("gaps and islands"): the previous
    # version tried F.lag("consecutive_nulls", 1) / F.lag("consecutive_
    # outliers", 1) inside the SAME withColumn() that defines those
    # columns -- that's not valid in Spark. A window function can only
    # see columns that already exist on the DataFrame before this
    # transformation runs; it can't reference its own in-progress output,
    # so this raised an AnalysisException (cannot resolve column) every
    # time. The standard fix: flag where a streak breaks, cumulative-sum
    # those breaks to assign each streak a group id, then count position
    # within each group.
    expanding = device_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    df = df.withColumn("_is_null", F.col("reading_value").isNull())
    df = df.withColumn(
        "_null_streak_reset",
        F.when(F.col("_is_null") != F.coalesce(F.lag("_is_null", 1).over(device_window), F.lit(False)), 1).otherwise(0),
    )
    df = df.withColumn("_null_streak_group", F.sum("_null_streak_reset").over(expanding))
    null_streak_window = Window.partitionBy("device_id", "_null_streak_group").orderBy("timestamp")
    df = df.withColumn(
        "consecutive_nulls",
        F.when(F.col("_is_null"), F.count(F.lit(1)).over(null_streak_window)).otherwise(F.lit(0)),
    )

    df = df.withColumn(
        "_outlier_streak_reset",
        F.when(
            F.col("is_outlier") != F.coalesce(F.lag("is_outlier", 1).over(device_window), F.lit(False)), 1
        ).otherwise(0),
    )
    df = df.withColumn("_outlier_streak_group", F.sum("_outlier_streak_reset").over(expanding))
    outlier_streak_window = Window.partitionBy("device_id", "_outlier_streak_group").orderBy("timestamp")
    df = df.withColumn(
        "consecutive_outliers",
        F.when(F.col("is_outlier"), F.count(F.lit(1)).over(outlier_streak_window)).otherwise(F.lit(0)),
    )

    return df.drop(
        "_is_null", "_null_streak_reset", "_null_streak_group", "_outlier_streak_reset", "_outlier_streak_group"
    )


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

    # SILVER_OUTPUT_SCHEMA expects a "sensor_unit" column (the sensor's
    # canonical unit, as metadata) distinct from "reading_unit" (this
    # specific reading's unit, post-standardization). The previous
    # version dropped "unit" outright without ever producing
    # "sensor_unit" -- schema_manager would then report it as a missing
    # column and main.py would raise on every single run. Rename instead
    # of dropping. normal_min/normal_max/warning_low/warning_high/
    # critical_low/critical_high are intermediate metadata used only for
    # outlier/status derivation above and aren't part of the output
    # schema, so those genuinely do get dropped.
    df = df.withColumnRenamed("unit", "sensor_unit")
    df = df.drop("normal_min", "normal_max", "warning_low", "warning_high", "critical_low", "critical_high")

    # Match SILVER_OUTPUT_SCHEMA's declared types explicitly, rather than
    # letting whatever Spark happens to infer drift from the contract.
    # Confirmed by actually running schema_manager against real output --
    # F.to_date() produces DateType (schema wants StringType), and
    # F.count()/F.sum() always produce LongType (schema wants IntegerType)
    # regardless of the actual value range. Without these casts,
    # schema_manager.generate_report() reports non-empty type_changes and
    # main.py raises ValueError on every run, even with every other fix
    # in place.
    return (
        df.withColumn("event_date", F.col("event_date").cast("string"))
        .withColumn("sampling_frequency_seconds", F.col("sampling_frequency_seconds").cast("int"))
        .withColumn("consecutive_nulls", F.col("consecutive_nulls").cast("int"))
        .withColumn("consecutive_outliers", F.col("consecutive_outliers").cast("int"))
    )