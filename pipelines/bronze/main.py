from pyspark.sql.functions import col, dayofmonth, hour, month, to_timestamp, year

try:
    from .config import (
        ALLOWED_TIME_TOLERANCE_MINUTES,
        BRONZE_OUTPUT_PATH,
        DEVICE_METADATA,
        PROCESSING_JOB_ID,
        QUARANTINE_PATH,
        RAW_INPUT_PATH,
        SENSOR_METADATA,
    )
    from .metadata_loader import build_lookup_dicts, create_metadata_dataframes, load_yaml
    from .reader import read_raw_data
    from .schema_manager import SchemaManager
    from .schemas import BRONZE_OUTPUT_SCHEMA
    from .spark_session import create_spark
    from .utils import log
except ImportError:  # pragma: no cover - direct script execution
    from config import (
        ALLOWED_TIME_TOLERANCE_MINUTES,
        BRONZE_OUTPUT_PATH,
        DEVICE_METADATA,
        PROCESSING_JOB_ID,
        QUARANTINE_PATH,
        RAW_INPUT_PATH,
        SENSOR_METADATA,
    )
    from metadata_loader import build_lookup_dicts, create_metadata_dataframes, load_yaml
    from reader import read_raw_data
    from schema_manager import SchemaManager
    from schemas import BRONZE_OUTPUT_SCHEMA
    from spark_session import create_spark
    from utils import log


def main():
    spark = create_spark()
    log("Loading metadata...")
    devices = load_yaml(DEVICE_METADATA)
    sensors = load_yaml(SENSOR_METADATA)

    device_df, sensor_df = create_metadata_dataframes(spark, devices, sensors)
    device_lookup, sensor_lookup = build_lookup_dicts(device_df, sensor_df)

    log(f"Devices: {device_df.count()}")
    log(f"Sensor Types: {sensor_df.count()}")

    log("Reading raw input files...")
    bronze_df, quarantine_df = read_raw_data(
        spark,
        RAW_INPUT_PATH,
        device_lookup,
        sensor_lookup,
        PROCESSING_JOB_ID,
        ALLOWED_TIME_TOLERANCE_MINUTES,
    )

    bronze_df = (
        bronze_df.withColumn("timestamp", to_timestamp(col("timestamp")))
        .withColumn("ingestion_timestamp", to_timestamp(col("ingestion_timestamp")))
        .withColumn("processing_timestamp", to_timestamp(col("processing_timestamp")))
        .withColumn("year", year(col("timestamp")))
        .withColumn("month", month(col("timestamp")))
        .withColumn("day", dayofmonth(col("timestamp")))
        .withColumn("hour", hour(col("timestamp")))
    )

    quarantine_df = (
        quarantine_df.withColumn("processing_timestamp", to_timestamp(col("processing_timestamp")))
        .withColumn("year", year(col("processing_timestamp")))
        .withColumn("month", month(col("processing_timestamp")))
        .withColumn("day", dayofmonth(col("processing_timestamp")))
        .withColumn("hour", hour(col("processing_timestamp")))
    )

    schema_manager = SchemaManager(BRONZE_OUTPUT_SCHEMA)
    schema_report = schema_manager.generate_report(bronze_df)
    if schema_report["missing_columns"] or schema_report["type_changes"]:
        raise ValueError(f"Bronze schema validation failed: {schema_report}")


    try:
        bronze_df.write.mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("year", "month", "day", "hour", "plant_id") \
            .parquet(BRONZE_OUTPUT_PATH)

        log("Bronze data written successfully.")

    except Exception as e:
        log(f"Error writing bronze data: {str(e)}")
        raise

    try:
        quarantine_df.write.mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("year", "month", "day", "hour") \
            .parquet(QUARANTINE_PATH)

        log("Quarantine data written successfully.")

    except Exception as e:
        log(f"Error writing quarantine data: {str(e)}")
        raise

    log(f"Bronze data written to: {BRONZE_OUTPUT_PATH}")
    log(f"Quarantine data written to: {QUARANTINE_PATH}")
    log(f"""
    Bronze Job Summary
    ------------------
    Valid Records      : {bronze_df.count()}
    Quarantined        : {quarantine_df.count()}
    Processing Job ID  : {PROCESSING_JOB_ID}
    """)


if __name__ == "__main__":
    main()