from pyspark.sql import functions as F

from config import (
    BRONZE_INPUT_PATH,
    DEVICE_METADATA,
    HF_BRONZE_CHECKPOINT_KEY,
    HF_BRONZE_PREFIX,
    HF_BUCKET_NAME,
    HF_SILVER_CHECKPOINT_KEY,
    HF_SILVER_PREFIX,
    PROCESSING_JOB_ID,
    SENSOR_METADATA,
    SILVER_OUTPUT_PATH,
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
from metadata_loader import create_metadata_dataframes, load_yaml
from reader import read_bronze_data
from schema_manager import SchemaManager
from schemas import SILVER_OUTPUT_SCHEMA
from transformations import prepare_silver
from spark_session import create_spark
from utils import log


def main():
    s3_client = None
    bronze_watermark = None

    if USE_HF_STORAGE:
        log("Connecting to Hugging Face storage bucket...")
        s3_client = create_s3_client()

        # Run-level checkpoint check, before Spark or any download even
        # starts: if Bronze's watermark hasn't moved since Silver's own
        # checkpoint recorded, there's nothing new to rebuild from, so
        # skip the entire (expensive) run rather than pay for JVM
        # startup + a full Bronze download only to recompute an
        # identical table. Not a row-level watermark -- see
        # hf_storage.py's module docstring for why Silver can't do that.
        bronze_checkpoint = read_checkpoint(s3_client, HF_BUCKET_NAME, HF_BRONZE_CHECKPOINT_KEY)
        bronze_watermark = parse_checkpoint_watermark(bronze_checkpoint)

        if bronze_watermark is None:
            log("Bronze has no checkpoint yet -- nothing to process. Silver layer skipped.")
            return

        silver_checkpoint = read_checkpoint(s3_client, HF_BUCKET_NAME, HF_SILVER_CHECKPOINT_KEY)
        silver_last_processed = parse_checkpoint_watermark(silver_checkpoint)

        if silver_last_processed == bronze_watermark:
            log(
                f"Bronze hasn't advanced since Silver's last run "
                f"(watermark {bronze_watermark.isoformat()}) -- nothing to process."
            )
            return

    spark = create_spark()

    if USE_HF_STORAGE:
        log("Downloading Bronze Delta table from HF bucket...")
        n = download_prefix(s3_client, HF_BUCKET_NAME, HF_BRONZE_PREFIX, BRONZE_INPUT_PATH)
        log(f"Downloaded {n} Bronze file(s)")

        if n == 0:
            log("Bronze table is empty in the bucket -- nothing to process. Silver layer skipped.")
            return

    log("Loading metadata...")
    devices = load_yaml(DEVICE_METADATA)
    sensor_config = load_yaml(SENSOR_METADATA)

    device_df, sensor_df = create_metadata_dataframes(spark, devices, sensor_config)
    device_df.cache()
    sensor_df.cache()

    log(f"Loaded device metadata rows: {device_df.count()}")
    log(f"Loaded sensor metadata rows: {sensor_df.count()}")

    log("Reading Bronze Delta table...")
    bronze_df = read_bronze_data(spark, BRONZE_INPUT_PATH)
    if bronze_df.rdd.isEmpty():
        log("No valid Bronze data found. Silver layer skipped.")
        return

    silver_df = prepare_silver(bronze_df, device_df, sensor_df)

    silver_df = silver_df.withColumn("processing_timestamp", F.current_timestamp())

    # Enforce the SILVER_OUTPUT_SCHEMA contract exactly. prepare_silver's
    # transformations only ever add or rename columns -- nothing in the
    # pipeline drops bronze-origin passthrough columns (is_corrupt,
    # error_reason, bronze_version, record_hash, processing_job_id,
    # ingestion_timestamp, badge_id, access_granted, Bronze's own
    # year/month/day/hour), so without this select they were silently
    # riding into the Delta table even though schemas.py never declared
    # them. schema_manager only flags MISSING expected columns, never
    # unexpected extra ones, so this went undetected until now.
    silver_df = silver_df.select(*[f.name for f in SILVER_OUTPUT_SCHEMA.fields])

    schema_manager = SchemaManager(SILVER_OUTPUT_SCHEMA)
    schema_report = schema_manager.generate_report(silver_df)
    if schema_report["missing_columns"] or schema_report["type_changes"]:
        raise ValueError(f"Silver schema validation failed: {schema_report}")

    # Delta, not Parquet -- spark_session.py already configures the Delta
    # catalog/extensions (Bronze writes Delta too), so this was an
    # inconsistency rather than a deliberate choice. mode("overwrite")
    # here means every run fully rebuilds Silver from the complete Bronze
    # table -- correct for this pipeline's dedup/rolling-window/streak
    # logic, which all need the full device history to be right, not
    # just what's new since last run.
    # Delta forbids combining overwriteSchema with the session's dynamic
    # partitionOverwriteMode (set in spark_session.py, which matters for
    # Bronze's appends but not here) -- ambiguous whether a schema change
    # applies to the whole table or just the touched partitions, so Delta
    # refuses rather than guess. Overriding to static here is also the
    # correct semantic anyway: Silver rebuilds fully from the complete
    # Bronze table every run, so the whole table should be replaced, not
    # just whichever partitions happened to appear in this run's data.
    silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option(
        "partitionOverwriteMode", "static"
    ).partitionBy("event_year", "event_month", "event_day", "plant_id").save(SILVER_OUTPUT_PATH)

    log(f"Silver output written to {SILVER_OUTPUT_PATH}")
    silver_count = silver_df.count()
    log(f"Silver records: {silver_count}")

    if USE_HF_STORAGE:
        log("Syncing Silver Delta table back to Hugging Face bucket...")
        n = upload_directory(s3_client, HF_BUCKET_NAME, SILVER_OUTPUT_PATH, HF_SILVER_PREFIX)
        log(f"Uploaded {n} Silver file(s) -> s3://{HF_BUCKET_NAME}/{HF_SILVER_PREFIX}")

        # Only record the checkpoint after the write and upload above have
        # both succeeded -- if either failed, the next run should retry
        # a full rebuild rather than think this Bronze watermark is
        # already reflected in Silver when it isn't.
        write_checkpoint(
            s3_client,
            HF_BUCKET_NAME,
            HF_SILVER_CHECKPOINT_KEY,
            watermark=bronze_watermark,
            processing_job_id=PROCESSING_JOB_ID,
            files_processed=silver_count,
        )
        log(f"Silver checkpoint advanced to {bronze_watermark.isoformat()}")


if __name__ == "__main__":
    main()