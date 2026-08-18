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
#
# Grain:
#   One row per sensor configuration/version.
#
# Current implementation:
#   SCD Type 1.
#
# Future-compatible with SCD Type 2 through:
#   effective_from
#   effective_to
#   is_current
#
# sensor_key is the surrogate key used by Gold facts.
# device_id remains the natural/business key for traceability.
# ---------------------------------------------------------------------------

DIM_SENSOR_SCHEMA = StructType(
    [
        # Surrogate / natural keys
        StructField("sensor_key", LongType(), False),
        StructField("device_id", StringType(), False),
        StructField("device_type", StringType(), False),

        # Plant
        StructField("plant_id", StringType(), True),
        StructField("plant_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("timezone", StringType(), True),

        # Production line
        StructField("line_id", StringType(), True),
        StructField("production_line", StringType(), True),
        StructField("line_type", StringType(), True),

        # Machine
        StructField("machine_id", StringType(), True),
        StructField("machine_name", StringType(), True),
        StructField("machine_type", StringType(), True),
        StructField("manufacturer", StringType(), True),
        StructField("rated_capacity_units_per_hour", IntegerType(), True),

        # Sensor configuration
        StructField("sensor_unit", StringType(), True),
        StructField("is_safety_critical", BooleanType(), True),
        StructField("sampling_frequency_seconds", IntegerType(), True),

        # Sensor lifecycle
        StructField("sensor_install_date", DateType(), True),
        StructField("last_calibration_date", DateType(), True),
        StructField("firmware_version", StringType(), True),

        # Configured operating ranges
        StructField("normal_min", DoubleType(), True),
        StructField("normal_max", DoubleType(), True),

        StructField("warning_low", DoubleType(), True),
        StructField("warning_high", DoubleType(), True),

        StructField("critical_low", DoubleType(), True),
        StructField("critical_high", DoubleType(), True),

        StructField("offline_grace_period_seconds", IntegerType(), True),

        # Dimension lifecycle
        StructField("is_active", BooleanType(), False),
        StructField("effective_from", TimestampType(), True),
        StructField("effective_to", TimestampType(), True),
        StructField("is_current", BooleanType(), False),
    ]
)


# ---------------------------------------------------------------------------
# DIM_DATE
# ---------------------------------------------------------------------------
#
# Grain:
#   One row per calendar date.
#
# date_key:
#   YYYYMMDD integer representation of full_date.
#
# Example:
#   2026-08-17 -> 20260817
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
#
# Grain:
#   One row per sensor per calendar day.
#
# Business key:
#   (sensor_key, date_key)
#
# This is the primary analytical sensor fact.
#
# Silver contains individual sensor events.
# Gold compresses those events into daily business-level metrics.
# ---------------------------------------------------------------------------

FACT_SENSOR_READING_DAILY_SCHEMA = StructType(
    [
        # Dimension keys
        StructField("sensor_key", LongType(), False),
        StructField("date_key", IntegerType(), False),

        # Traceability
        StructField("device_id", StringType(), False),
        StructField("device_type", StringType(), False),
        StructField("machine_id", StringType(), True),

        # Reading volume
        StructField("reading_count", LongType(), False),

        # Reading statistics
        StructField("avg_reading_value", DoubleType(), True),
        StructField("min_reading_value", DoubleType(), True),
        StructField("max_reading_value", DoubleType(), True),

        # Reading status counts
        StructField("normal_count", LongType(), False),
        StructField("warning_count", LongType(), False),
        StructField("critical_count", LongType(), False),
        StructField("outlier_count", LongType(), False),
        StructField("late_arrival_count", LongType(), False),

        # Rolling / temporal behaviour
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

        # Consecutive anomaly/null behaviour
        StructField("max_consecutive_outliers", IntegerType(), True),
        StructField("max_consecutive_nulls", IntegerType(), True),

        # Event time boundaries for traceability
        StructField("first_reading_timestamp", TimestampType(), True),
        StructField("last_reading_timestamp", TimestampType(), True),

        # Processing metadata
        StructField("processing_timestamp", TimestampType(), False),
    ]
)


# ---------------------------------------------------------------------------
# FACT_SENSOR_HEALTH_DAILY
# ---------------------------------------------------------------------------
#
# Grain:
#   One row per sensor per calendar day.
#
# Business key:
#   (sensor_key, date_key)
#
# This fact focuses on data reliability / sensor health rather than
# physical sensor measurements.
# ---------------------------------------------------------------------------

FACT_SENSOR_HEALTH_DAILY_SCHEMA = StructType(
    [
        # Dimension keys
        StructField("sensor_key", LongType(), False),
        StructField("date_key", IntegerType(), False),

        # Traceability
        StructField("device_id", StringType(), False),
        StructField("device_type", StringType(), False),
        StructField("machine_id", StringType(), True),

        # Sampling / availability
        StructField("sampling_frequency_seconds", IntegerType(), True),
        StructField("expected_reading_count", LongType(), True),
        StructField("actual_reading_count", LongType(), False),
        StructField("missing_reading_count", LongType(), True),

        # Data quality
        StructField("normal_count", LongType(), False),
        StructField("warning_count", LongType(), False),
        StructField("critical_count", LongType(), False),
        StructField("outlier_count", LongType(), False),
        StructField("late_arrival_count", LongType(), False),

        # Percentages / KPIs
        StructField("data_completeness_percentage", DoubleType(), True),
        StructField("normal_percentage", DoubleType(), True),
        StructField("warning_percentage", DoubleType(), True),
        StructField("critical_percentage", DoubleType(), True),
        StructField("outlier_percentage", DoubleType(), True),
        StructField("late_arrival_percentage", DoubleType(), True),

        # Reliability indicators
        StructField("max_consecutive_nulls", IntegerType(), True),
        StructField("max_consecutive_outliers", IntegerType(), True),

        # Overall reporting score
        StructField("health_score", DoubleType(), True),

        # Processing metadata
        StructField("processing_timestamp", TimestampType(), False),
    ]
)


# ---------------------------------------------------------------------------
# FACT_MACHINE_DAILY
# ---------------------------------------------------------------------------
#
# Grain:
#   One row per machine per calendar day.
#
# Business key:
#   (machine_id, date_key)
#
# This aggregates the behaviour of all sensors attached to a machine.
# ---------------------------------------------------------------------------

FACT_MACHINE_DAILY_SCHEMA = StructType(
    [
        # Dimension / business keys
        StructField("machine_id", StringType(), False),
        StructField("date_key", IntegerType(), False),

        # Descriptive traceability
        StructField("plant_id", StringType(), True),
        StructField("machine_name", StringType(), True),
        StructField("machine_type", StringType(), True),
        StructField("production_line", StringType(), True),

        # Sensor coverage
        StructField("sensor_count", LongType(), False),
        StructField("active_sensor_count", LongType(), False),

        # Overall reading volume
        StructField("total_readings", LongType(), False),

        # Event counts
        StructField("critical_readings", LongType(), False),
        StructField("warning_readings", LongType(), False),
        StructField("outlier_readings", LongType(), False),
        StructField("late_arrival_count", LongType(), False),

        # Sensor health
        StructField("avg_sensor_health_score", DoubleType(), True),
        StructField("critical_sensor_count", LongType(), False),
        StructField("warning_sensor_count", LongType(), False),
        StructField("unhealthy_sensor_count", LongType(), False),

        # Machine-level data completeness
        StructField("data_completeness_percentage", DoubleType(), True),

        # Sensor-specific physical metrics.
        #
        # These remain NULL when a machine does not have that sensor type.
        # We must NOT convert missing sensor types to zero.
        StructField("max_temperature", DoubleType(), True),
        StructField("max_vibration", DoubleType(), True),
        StructField("max_pressure", DoubleType(), True),
        StructField("max_current", DoubleType(), True),

        # Proxy condition from Silver.
        #
        # This is NOT claimed to be a true machine state.
        StructField("machine_operating_condition", StringType(), True),

        # Processing metadata
        StructField("processing_timestamp", TimestampType(), False),
    ]
)

# ---------------------------------------------------------------------------
# SILVER_INPUT_SCHEMA
# ---------------------------------------------------------------------------
#
# Schema for Silver layer output that Gold consumes.
# Gold expects this structure from the Silver layer.
# ---------------------------------------------------------------------------

SILVER_INPUT_SCHEMA = StructType(
    [
        StructField("event_timestamp", TimestampType(), False),
        StructField("event_date", DateType(), False),
        StructField("device_id", StringType(), False),
        StructField("device_type", StringType(), False),
        StructField("machine_id", StringType(), True),
        StructField("reading_value", DoubleType(), True),
        StructField("reading_status", StringType(), True),
        StructField("is_outlier", BooleanType(), True),
        StructField("is_late_arrival", BooleanType(), True),
        StructField("rolling_stddev", DoubleType(), True),
        StructField("delta_previous", DoubleType(), True),
        StructField("percent_change", DoubleType(), True),
        StructField("reading_velocity", DoubleType(), True),
        StructField("reading_acceleration", DoubleType(), True),
        StructField("consecutive_outliers", IntegerType(), True),
        StructField("consecutive_nulls", IntegerType(), True),
        StructField("moving_average", DoubleType(), True),
        StructField("sampling_frequency_seconds", IntegerType(), True),
        StructField("machine_operating_condition", StringType(), True),
    ]
)

# ============================================================================
# SCHEMA REGISTRY
# ============================================================================
#
# Central registry used by the Gold orchestration/storage layer.
#
# This avoids scattering schema names throughout main.py and storage code.
# ============================================================================

GOLD_SCHEMAS = {
    "dim_sensor": DIM_SENSOR_SCHEMA,
    "dim_date": DIM_DATE_SCHEMA,
    "fact_sensor_reading_daily": FACT_SENSOR_READING_DAILY_SCHEMA,
    "fact_sensor_health_daily": FACT_SENSOR_HEALTH_DAILY_SCHEMA,
    "fact_machine_daily": FACT_MACHINE_DAILY_SCHEMA,
}