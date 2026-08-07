from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RAW_INPUT_SCHEMA = StructType(
    [
        StructField("device_id", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("plant_id", StringType(), True),
        StructField("machine_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("reading_value", DoubleType(), True),
        StructField("reading_unit", StringType(), True),
        StructField("firmware_version", StringType(), True),
        StructField("quality_flag", StringType(), True),
        StructField("ingestion_timestamp", StringType(), True),
        StructField("source_file_name", StringType(), True),
    ]
)

BRONZE_OUTPUT_SCHEMA = StructType(
    [
        StructField("device_id", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("plant_id", StringType(), True),
        StructField("machine_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("reading_value", DoubleType(), True),
        StructField("reading_unit", StringType(), True),
        # Event-driven sensor fields (e.g. access_control badge scans).
        # NULL for every other device_type — this is exactly the kind of
        # nullable schema evolution Bronze.md describes for new firmware
        # fields: existing records get NULL, nothing breaks.
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
    ]
)

QUARANTINE_SCHEMA = StructType(
    [
        StructField("source_file_name", StringType(), True),
        StructField("raw_payload", StringType(), True),
        StructField("error_reason", StringType(), True),
        StructField("processing_timestamp", TimestampType(), True),
        StructField("processing_job_id", StringType(), True),
        StructField("is_corrupt", BooleanType(), True),
    ]
)