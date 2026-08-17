import time

from pyspark.sql.functions import col, dayofmonth, hour, month, to_timestamp, year

try:
    from .config import (
        ALLOWED_TIME_TOLERANCE_MINUTES,
        BRONZE_OUTPUT_PATH,
        DEVICE_METADATA,
        HF_BRONZE_PREFIX,
        HF_BUCKET_NAME,
        HF_CHECKPOINT_KEY,
        HF_QUARANTINE_PREFIX,
        HF_RAW_PREFIX,
        PROCESSING_JOB_ID,
        QUARANTINE_PATH,
        RAW_INPUT_PATH,
        SENSOR_METADATA,
        USE_HF_STORAGE,
    )
    from .hf_storage import (
        create_s3_client,
        download_prefix,
        parse_checkpoint_watermark,
        read_checkpoint,
        upload_directory,
        write_checkpoint,
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
        HF_BRONZE_PREFIX,
        HF_BUCKET_NAME,
        HF_CHECKPOINT_KEY,
        HF_QUARANTINE_PREFIX,
        HF_RAW_PREFIX,
        PROCESSING_JOB_ID,
        QUARANTINE_PATH,
        RAW_INPUT_PATH,
        SENSOR_METADATA,
        USE_HF_STORAGE,
    )
    from hf_storage import (
        create_s3_client,
        download_prefix,
        parse_checkpoint_watermark,
        read_checkpoint,
        upload_directory,
        write_checkpoint,
    )
    from metadata_loader import build_lookup_dicts, create_metadata_dataframes, load_yaml
    from reader import read_raw_data
    from schema_manager import SchemaManager
    from schemas import BRONZE_OUTPUT_SCHEMA
    from spark_session import create_spark
    from utils import log


def main():
    spark = create_spark()

    s3_client = None
    watermark = None

    if USE_HF_STORAGE:
        log("Connecting to Hugging Face storage bucket...")
        s3_client = create_s3_client()

        # Pull down whatever Bronze / quarantine Delta table already exists
        # in the bucket before writing, so this run's append continues the
        # same table history instead of starting a brand-new table on the
        # runner's empty filesystem.
        n = download_prefix(s3_client, HF_BUCKET_NAME, HF_BRONZE_PREFIX, BRONZE_OUTPUT_PATH)
        log(f"Restored {n} existing Bronze Delta file(s) from HF bucket")

        n = download_prefix(s3_client, HF_BUCKET_NAME, HF_QUARANTINE_PREFIX, QUARANTINE_PATH)
        log(f"Restored {n} existing quarantine Delta file(s) from HF bucket")

        # Everything downloaded above now sits on disk with an mtime of
        # "now". Capturing the cutoff here, before any Spark write
        # happens, means upload_directory() can later tell "existed
        # before this run" apart from "created or overwritten by this
        # run's append" -- see hf_storage.upload_directory.
        sync_cutoff = time.time()

        # Watermark: only look at raw objects uploaded after the last
        # successful run. Without this, every run re-lists and re-validates
        # the entire raw/ history, and old-but-already-processed batches
        # fail the timestamp-freshness check and pile up in quarantine.
        checkpoint = read_checkpoint(s3_client, HF_BUCKET_NAME, HF_CHECKPOINT_KEY)
        watermark = parse_checkpoint_watermark(checkpoint)
        if watermark:
            log(f"Resuming from checkpoint: only reading raw objects uploaded after {watermark.isoformat()}")
        else:
            log("No checkpoint found — this is the first run, reading full raw/ history")

    log("Loading metadata...")
    devices = load_yaml(DEVICE_METADATA)
    sensors = load_yaml(SENSOR_METADATA)

    device_df, sensor_df = create_metadata_dataframes(spark, devices, sensors)
    device_lookup, sensor_lookup = build_lookup_dicts(device_df, sensor_df)

    log(f"Devices: {device_df.count()}")
    log(f"Sensor Types: {sensor_df.count()}")

    log("Reading raw input files...")
    bronze_df, quarantine_df, new_watermark = read_raw_data(
        spark,
        device_lookup,
        sensor_lookup,
        PROCESSING_JOB_ID,
        ALLOWED_TIME_TOLERANCE_MINUTES,
        input_path=RAW_INPUT_PATH,
        use_hf_storage=USE_HF_STORAGE,
        s3_client=s3_client,
        hf_bucket=HF_BUCKET_NAME,
        hf_prefix=HF_RAW_PREFIX,
        since=watermark,
    )

    if USE_HF_STORAGE and new_watermark == watermark:
        log("No new raw objects since the last checkpoint — nothing to process this run.")
        return

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
        bronze_df.write.format("delta").mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("year", "month", "day", "hour", "plant_id") \
            .save(BRONZE_OUTPUT_PATH)

        log("Bronze data written successfully.")

    except Exception as e:
        log(f"Error writing bronze data: {str(e)}")
        raise

    try:
        quarantine_df.write.format("delta").mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("year", "month", "day", "hour") \
            .save(QUARANTINE_PATH)

        log("Quarantine data written successfully.")

    except Exception as e:
        log(f"Error writing quarantine data: {str(e)}")
        raise

    bronze_count = bronze_df.count()
    quarantine_count = quarantine_df.count()

    if USE_HF_STORAGE:
        log("Syncing Bronze Delta table back to Hugging Face bucket...")
        n = upload_directory(s3_client, HF_BUCKET_NAME, BRONZE_OUTPUT_PATH, HF_BRONZE_PREFIX, modified_after=sync_cutoff)
        log(f"Uploaded {n} Bronze file(s) -> s3://{HF_BUCKET_NAME}/{HF_BRONZE_PREFIX}")

        n = upload_directory(s3_client, HF_BUCKET_NAME, QUARANTINE_PATH, HF_QUARANTINE_PREFIX, modified_after=sync_cutoff)
        log(f"Uploaded {n} quarantine file(s) -> s3://{HF_BUCKET_NAME}/{HF_QUARANTINE_PREFIX}")

        # Only advance the checkpoint after the writes and uploads above
        # have succeeded — if anything failed, we want the next run to
        # retry the same objects rather than silently skip them.
        write_checkpoint(
            s3_client,
            HF_BUCKET_NAME,
            HF_CHECKPOINT_KEY,
            watermark=new_watermark,
            processing_job_id=PROCESSING_JOB_ID,
            files_processed=bronze_count + quarantine_count,
        )
        log(f"Checkpoint advanced to {new_watermark.isoformat()}")

    log(f"Bronze data written to: {BRONZE_OUTPUT_PATH}")
    log(f"Quarantine data written to: {QUARANTINE_PATH}")
    log(f"""
    Bronze Job Summary
    ------------------
    Valid Records      : {bronze_count}
    Quarantined        : {quarantine_count}
    Processing Job ID  : {PROCESSING_JOB_ID}
    """)


if __name__ == "__main__":
    main()