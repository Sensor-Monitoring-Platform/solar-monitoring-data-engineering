from pyspark.sql import functions as F

from config import (
    BRONZE_INPUT_PATH,
    DEVICE_METADATA,
    HF_BRONZE_PREFIX,
    HF_BUCKET_NAME,
    HF_SILVER_PREFIX,
    SENSOR_METADATA,
    SILVER_OUTPUT_PATH,
    USE_HF_STORAGE,
)
from hf_storage import create_s3_client, download_prefix, upload_directory
from metadata_loader import create_metadata_dataframes, load_yaml
from reader import read_bronze_data
from schema_manager import SchemaManager
from schemas import SILVER_OUTPUT_SCHEMA
from transformations import prepare_silver
from spark_session import create_spark
from utils import log


def main():
    # create_spark() takes no arguments -- APP_NAME is set once in
    # config.py (already "Silver Layer Pipeline") and read from there.
    # The previous call, create_spark(app_name="Silver Layer Pipeline"),
    # would raise TypeError immediately, since spark_session.py's
    # create_spark() has no app_name parameter.
    spark = create_spark()

    s3_client = None
    if USE_HF_STORAGE:
        log("Connecting to Hugging Face storage bucket...")
        s3_client = create_s3_client()

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
    silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy(
        "event_year", "event_month", "event_day", "plant_id"
    ).save(SILVER_OUTPUT_PATH)

    log(f"Silver output written to {SILVER_OUTPUT_PATH}")
    log(f"Silver records: {silver_df.count()}")

    if USE_HF_STORAGE:
        log("Syncing Silver Delta table back to Hugging Face bucket...")
        n = upload_directory(s3_client, HF_BUCKET_NAME, SILVER_OUTPUT_PATH, HF_SILVER_PREFIX)
        log(f"Uploaded {n} Silver file(s) -> s3://{HF_BUCKET_NAME}/{HF_SILVER_PREFIX}")


if __name__ == "__main__":
    main()