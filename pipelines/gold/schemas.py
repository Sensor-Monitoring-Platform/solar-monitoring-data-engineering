from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# DIM_SENSOR
# ---------------------------------------------------------------------------

DIM_SENSOR_SCHEMA = StructType(
    [
        StructField("sensor_key", LongType(), False),
        StructField("device_id", StringType(), False),
        StructField("device_type", StringType(), False),

        StructField("plant_id", StringType(), True),
        StructField("plant_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("timezone", StringType(), True),

        StructField("line_id", StringType(), True),
        StructField("production_line", StringType(), True),
        StructField("line_type", StringType(), True),

        StructField("machine_id", StringType(), True),
        StructField("machine_name", StringType(), True),
        StructField("machine_type", StringType(), True),
        StructField("manufacturer", StringType(), True),
        StructField("rated_capacity_units_per_hour", IntegerType(), True),

        StructField("sensor_unit", StringType(), True),
        StructField("is_safety_critical", BooleanType(), True),
        StructField("sampling_frequency_seconds", IntegerType(), True),

        StructField("sensor_install_date", DateType(), True),
        StructField("last_calibration_date", DateType(), True),
        StructField("firmware_version", StringType(), True),

        StructField("normal_min", DoubleType(), True),
        StructField("normal_max", DoubleType(), True),

        StructField("warning_low", DoubleType(), True),
        StructField("warning_high", DoubleType(), True),

        StructField("critical_low", DoubleType(), True),
        StructField("critical_high", DoubleType(), True),

        StructField("offline_grace_period_seconds", IntegerType(), True),

        StructField("is_active", BooleanType(), False),
        StructField("effective_from", TimestampType(), True),
        StructField("effective_to", TimestampType(), True),
        StructField("is_current", BooleanType(), False),
    ]
)


# ---------------------------------------------------------------------------
# DIM_DATE
# ---------------------------------------------------------------------------

DIM_DATE_SCHEMA = StructType(
    [
        StructField("date_key", IntegerType(), False),
        StructField("full_date", DateType(), False),

        StructField("year", IntegerType(), False),
        StructField("quarter", IntegerType(), False),
        StructField("quarter_name", StringType(), False),

        StructField("month", IntegerType(), False),
        StructField("month_name", StringType(), False),

        StructField("week_of_year", IntegerType(), False),

        StructField("day", IntegerType(), False),
        StructField("day_of_year", IntegerType(), False),

        StructField("day_of_week", IntegerType(), False),
        StructField("day_name", StringType(), False),

        StructField("is_weekend", BooleanType(), False),

        StructField("year_month", StringType(), False),
        StructField("year_month_name", StringType(), False),
    ]
)


# ---------------------------------------------------------------------------
# FACT_SENSOR_READING_DAILY
# ---------------------------------------------------------------------------

FACT_SENSOR_READING_DAILY_SCHEMA = StructType(
    [
        StructField("sensor_key", LongType(), False),
        StructField("date_key", IntegerType(), False),

        StructField("device_id", StringType(), False),
        StructField("device_type", StringType(), False),
        StructField("machine_id", StringType(), True),

        StructField("reading_count", LongType(), False),

        StructField("avg_reading_value", DoubleType(), True),
        StructField("min_reading_value", DoubleType(), True),
        StructField("max_reading_value", DoubleType(), True),

        StructField("normal_count", LongType(), False),
        StructField("warning_count", LongType(), False),
        StructField("critical_count", LongType(), False),
        StructField("outlier_count", LongType(), False),
        StructField("late_arrival_count", LongType(), False),

        StructField("avg_rolling_stddev", DoubleType(), True),
        StructField("max_rolling_stddev", DoubleType(), True),

        StructField("avg_delta_previous", DoubleType(), True),
        StructField("max_delta_previous", DoubleType(), True),

        StructField("avg_percent_change", DoubleType(), True),
        StructField("max_percent_change", DoubleType(), True),

        StructField("avg_reading_velocity", DoubleType(), True),
        StructField("max_reading_velocity", DoubleType(), True),

        StructField("avg_reading_acceleration", DoubleType(), True),
        StructField("max_reading_acceleration", DoubleType(), True),

        StructField("max_consecutive_outliers", IntegerType(), True),
        StructField("max_consecutive_nulls", IntegerType(), True),

        StructField("first_reading_timestamp", TimestampType(), True),
        StructField("last_reading_timestamp", TimestampType(), True),

        StructField("processing_timestamp", TimestampType(), False),
    ]
)


# ---------------------------------------------------------------------------
# FACT_SENSOR_HEALTH_DAILY
# ---------------------------------------------------------------------------

FACT_SENSOR_HEALTH_DAILY_SCHEMA = StructType(
    [
        StructField("sensor_key", LongType(), False),
        StructField("date_key", IntegerType(), False),

        StructField("device_id", StringType(), False),
        StructField("device_type", StringType(), False),
        StructField("machine_id", StringType(), True),

        StructField("sampling_frequency_seconds", IntegerType(), True),
        StructField("expected_reading_count", LongType(), True),
        StructField("actual_reading_count", LongType(), False),
        StructField("missing_reading_count", LongType(), True),

        StructField("normal_count", LongType(), False),
        StructField("warning_count", LongType(), False),
        StructField("critical_count", LongType(), False),
        StructField("outlier_count", LongType(), False),
        StructField("late_arrival_count", LongType(), False),

        StructField("data_completeness_percentage", DoubleType(), True),
        StructField("normal_percentage", DoubleType(), True),
        StructField("warning_percentage", DoubleType(), True),
        StructField("critical_percentage", DoubleType(), True),
        StructField("outlier_percentage", DoubleType(), True),
        StructField("late_arrival_percentage", DoubleType(), True),

        StructField("max_consecutive_nulls", IntegerType(), True),
        StructField("max_consecutive_outliers", IntegerType(), True),

        StructField("health_score", DoubleType(), True),

        StructField("processing_timestamp", TimestampType(), False),
    ]
)


# ---------------------------------------------------------------------------
# FACT_MACHINE_DAILY
# ---------------------------------------------------------------------------

FACT_MACHINE_DAILY_SCHEMA = StructType(
    [
        StructField("machine_id", StringType(), False),
        StructField("date_key", IntegerType(), False),

        StructField("plant_id", StringType(), True),
        StructField("machine_name", StringType(), True),
        StructField("machine_type", StringType(), True),
        StructField("production_line", StringType(), True),

        StructField("sensor_count", LongType(), False),
        StructField("active_sensor_count", LongType(), False),

        StructField("total_readings", LongType(), False),

        StructField("critical_readings", LongType(), False),
        StructField("warning_readings", LongType(), False),
        StructField("outlier_readings", LongType(), False),
        StructField("late_arrival_count", LongType(), False),

        StructField("avg_sensor_health_score", DoubleType(), True),
        StructField("critical_sensor_count", LongType(), False),
        StructField("warning_sensor_count", LongType(), False),
        StructField("unhealthy_sensor_count", LongType(), False),

        StructField("data_completeness_percentage", DoubleType(), True),

        StructField("avg_sensor_reading", DoubleType(), True),

        StructField("max_process_temperature", DoubleType(), True),
        StructField("max_ambient_temperature", DoubleType(), True),
        StructField("max_vibration", DoubleType(), True),
        StructField("max_pressure", DoubleType(), True),
        StructField("max_current", DoubleType(), True),

        StructField("machine_operating_condition", StringType(), True),

        StructField("processing_timestamp", TimestampType(), False),
    ]
)

# ---------------------------------------------------------------------------
# SILVER_INPUT_SCHEMA
# ---------------------------------------------------------------------------
#
# IMPORTANT: this MUST mirror pipelines/silver/schemas.py's
# SILVER_OUTPUT_SCHEMA exactly -- Gold and Silver are separate deploy
# artifacts with no shared import, so this is a manually-kept copy, not
# an automatic one. It is used as the schema for an EMPTY DataFrame when
# the Silver Delta table doesn't exist yet (see reader.py). A mismatch
# here doesn't crash on the empty-table path itself (main.py checks
# rdd.isEmpty() before any fact function runs), but IS the reference
# every Gold fact/dimension function is written against -- letting it
# drift from real Silver output is what caused the earlier
# `event_timestamp` UNRESOLVED_COLUMN crash (real Silver calls it
# `timestamp`) and the `machine_name` ambiguous-column crash (real
# Silver already carries plant_id/machine_name/machine_type/
# production_line directly; Gold's fact_machine_daily used to re-join
# device_df for the same columns).
#
# If Silver's schema changes, update this to match.
# ---------------------------------------------------------------------------

SILVER_INPUT_SCHEMA = StructType(
    [
        StructField("device_id", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("plant_id", StringType(), True),
        StructField("machine_id", StringType(), True),
        StructField("plant_name", StringType(), True),
        StructField("production_line", StringType(), True),
        StructField("machine_name", StringType(), True),
        StructField("machine_type", StringType(), True),
        StructField("manufacturer", StringType(), True),
        StructField("firmware_version", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("event_date", StringType(), True),
        StructField("event_hour", IntegerType(), True),
        StructField("event_minute", IntegerType(), True),
        StructField("event_week", IntegerType(), True),
        StructField("event_month", IntegerType(), True),
        StructField("event_year", IntegerType(), True),
        StructField("event_day", IntegerType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("reading_value", DoubleType(), True),
        StructField("reading_unit", StringType(), True),
        StructField("sensor_unit", StringType(), True),
        StructField("is_safety_critical", BooleanType(), True),
        StructField("sampling_frequency_seconds", IntegerType(), True),
        StructField("is_late_arrival", BooleanType(), True),
        StructField("is_outlier", BooleanType(), True),
        StructField("reading_status", StringType(), True),
        StructField("sensor_health", StringType(), True),
        StructField("machine_operating_condition", StringType(), True),
        StructField("moving_average", DoubleType(), True),
        StructField("rolling_stddev", DoubleType(), True),
        StructField("delta_previous", DoubleType(), True),
        StructField("percent_change", DoubleType(), True),
        StructField("reading_velocity", DoubleType(), True),
        StructField("reading_acceleration", DoubleType(), True),
        StructField("consecutive_nulls", IntegerType(), True),
        StructField("consecutive_outliers", IntegerType(), True),
        StructField("processing_timestamp", TimestampType(), True),
    ]
)

# ============================================================================
# SCHEMA REGISTRY
# ============================================================================

GOLD_SCHEMAS = {
    "dim_sensor": DIM_SENSOR_SCHEMA,
    "dim_date": DIM_DATE_SCHEMA,
    "fact_sensor_reading_daily": FACT_SENSOR_READING_DAILY_SCHEMA,
    "fact_sensor_health_daily": FACT_SENSOR_HEALTH_DAILY_SCHEMA,
    "fact_machine_daily": FACT_MACHINE_DAILY_SCHEMA,
}