from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Must match pipelines/bronze/schemas.py's BRONZE_OUTPUT_SCHEMA exactly --
# this is only used as the schema for an EMPTY DataFrame (see reader.py's
# early-return when the bronze table doesn't exist yet), but an empty
# DataFrame with the wrong schema will still break any code downstream
# that references a column this declares as missing.
BRONZE_INPUT_SCHEMA = StructType(
    [
        StructField("device_id", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("plant_id", StringType(), True),
        StructField("machine_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("reading_value", DoubleType(), True),
        StructField("reading_unit", StringType(), True),
        StructField("badge_id", StringType(), True),
        StructField("access_granted", BooleanType(), True),
        StructField("firmware_version", StringType(), True),
        StructField("quality_flag", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("source_file_name", StringType(), True),
        StructField("processing_timestamp", TimestampType(), True),
        StructField("processing_job_id", StringType(), True),
        StructField("bronze_version", StringType(), True),
        StructField("record_hash", StringType(), True),
        StructField("is_corrupt", BooleanType(), True),
        StructField("error_reason", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("hour", IntegerType(), True),
    ]
)

SILVER_OUTPUT_SCHEMA = StructType(
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
        StructField("quality_flag", StringType(), True),
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
        StructField("source_file_name", StringType(), True),
        StructField("processing_timestamp", TimestampType(), True),
    ]
)