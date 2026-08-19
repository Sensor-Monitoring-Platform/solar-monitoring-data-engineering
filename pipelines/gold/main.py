from datetime import timedelta
from pathlib import Path

from pyspark.sql import functions as F

from config import (
    DEVICE_METADATA,
    GOLD_LOOKBACK_DAYS,
    GOLD_OUTPUT_PATH,
    GOLD_WRITE_MODE,
    HF_BUCKET_NAME,
    HF_GOLD_CHECKPOINT_KEY,
    HF_GOLD_PREFIX,
    HF_SILVER_CHECKPOINT_KEY,
    HF_SILVER_PREFIX,
    PROCESSING_JOB_ID,
    SENSOR_METADATA,
    SILVER_INPUT_PATH,
    USE_HF_STORAGE,
)
from dimensions import build_dim_date, build_dim_sensor
from facts import (
    fact_machine_daily,
    fact_sensor_health_daily,
    fact_sensor_reading_daily,
)
from hf_storage import (
    create_s3_client,
    download_prefix,
    parse_checkpoint_watermark,
    read_checkpoint,
    upload_directory,
    write_checkpoint,
)
from metadata_loader import (
    create_metadata_dataframes,
    load_yaml,
)
from reader import read_silver_data
from schemas import GOLD_SCHEMAS
from spark_session import create_spark
from utils import log
from validations import (
    require_columns,
    validate_dimension,
    validate_fact,
)

GOLD_TABLE_PATHS = {
    name: str(Path(GOLD_OUTPUT_PATH) / name) for name in GOLD_SCHEMAS
}

GOLD_TABLE_KEYS = {
    "dim_sensor": ("dimension", "sensor_key"),
    "dim_date": ("dimension", "date_key"),
    "fact_sensor_reading_daily": ("fact", ["device_id", "date_key"]),
    "fact_sensor_health_daily": ("fact", ["device_id", "date_key"]),
    "fact_machine_daily": ("fact", ["machine_id", "date_key"]),
}

DEFAULT_START_DATE = "2026-01-01"
DEFAULT_END_DATE = "2026-12-31"


def _apply_lookback(silver):
    """
    Restrict Silver to the trailing GOLD_LOOKBACK_DAYS days, when
    configured. GOLD_LOOKBACK_DAYS=0 means "no limit".

    Real Silver's event_date is StringType (see silver/schemas.py), not
    DateType -- we explicitly F.to_date(...) it inside the aggregation so
    .collect() reliably hands back a Python `datetime.date` regardless of
    the column's declared type, avoiding a `str - timedelta` TypeError.
    """
    if GOLD_LOOKBACK_DAYS <= 0:
        return silver

    max_date = silver.agg(F.max(F.to_date(F.col("event_date")))).collect()[0][0]
    if max_date is None:
        return silver

    cutoff = max_date - timedelta(days=GOLD_LOOKBACK_DAYS - 1)
    log(f"GOLD_LOOKBACK_DAYS={GOLD_LOOKBACK_DAYS} -- restricting Silver to event_date >= {cutoff.isoformat()}")
    return silver.filter(F.to_date(F.col("event_date")) >= F.lit(cutoff))


def _get_silver_date_range(silver) -> tuple[str, str]:
    """
    Return (start_date, end_date) as "yyyy-MM-dd" strings, for build_dim_date().

    F.to_date(...) first normalizes event_date to a real date type
    regardless of whether the underlying column is StringType or
    DateType, then F.date_format(...) forces the result to a plain
    string before it leaves Spark -- so the Python side never has to
    guess what type .collect() handed back.
    """
    row = silver.agg(
        F.date_format(F.min(F.to_date(F.col("event_date"))), "yyyy-MM-dd").alias("min_date"),
        F.date_format(F.max(F.to_date(F.col("event_date"))), "yyyy-MM-dd").alias("max_date"),
    ).collect()[0]

    start_date = row["min_date"]
    end_date = row["max_date"]

    if start_date is None or end_date is None:
        log(
            "Silver has rows but no non-null event_date values -- "
            f"falling back to default date range {DEFAULT_START_DATE}..{DEFAULT_END_DATE}."
        )
        return DEFAULT_START_DATE, DEFAULT_END_DATE

    return start_date, end_date


def _write_table(df, name: str):
    expected_columns = [f.name for f in GOLD_SCHEMAS[name].fields]
    require_columns(df, expected_columns, name)
    df = df.select(*expected_columns)

    writer = df.write.format("delta").mode(GOLD_WRITE_MODE)
    if GOLD_WRITE_MODE == "overwrite":
        writer = writer.option("overwriteSchema", "true")

    writer.save(GOLD_TABLE_PATHS[name])

    return df


def main():
    s3_client = None
    silver_watermark = None

    if USE_HF_STORAGE:
        log("Connecting to Hugging Face storage bucket...")
        s3_client = create_s3_client()

        silver_checkpoint = read_checkpoint(s3_client, HF_BUCKET_NAME, HF_SILVER_CHECKPOINT_KEY)
        silver_watermark = parse_checkpoint_watermark(silver_checkpoint)

        if silver_watermark is None:
            log("Silver has no checkpoint yet -- nothing to process. Gold layer skipped.")
            return

        gold_checkpoint = read_checkpoint(s3_client, HF_BUCKET_NAME, HF_GOLD_CHECKPOINT_KEY)
        gold_last_processed = parse_checkpoint_watermark(gold_checkpoint)

        if gold_last_processed == silver_watermark:
            log(
                f"Silver hasn't advanced since Gold's last run "
                f"(watermark {silver_watermark.isoformat()}) -- nothing to process."
            )
            return

        log("Downloading Silver Delta table from HF bucket...")
        n = download_prefix(s3_client, HF_BUCKET_NAME, HF_SILVER_PREFIX, SILVER_INPUT_PATH)
        log(f"Downloaded {n} Silver file(s)")

        if n == 0:
            log("Silver table is empty in the bucket -- nothing to process. Gold layer skipped.")
            return

    spark = create_spark()

    try:
        silver = read_silver_data(spark)

        if silver.rdd.isEmpty():
            log("Silver table has no rows -- nothing to process. Gold layer skipped.")
            return

        silver = _apply_lookback(silver)

        log("Loading metadata...")
        devices = load_yaml(DEVICE_METADATA)
        sensor_config = load_yaml(SENSOR_METADATA)

        device_df, sensor_config_df = create_metadata_dataframes(
            spark, devices, sensor_config
        )

        log("Building dim_sensor...")
        dim_sensor = build_dim_sensor(device_df, sensor_config_df)

        start_date, end_date = _get_silver_date_range(silver)
        log(f"Building dim_date for range {start_date}..{end_date}...")
        dim_date = build_dim_date(spark, start_date, end_date)

        log("Building fact_sensor_reading_daily...")
        reading_fact = fact_sensor_reading_daily(silver)

        log("Building fact_sensor_health_daily...")
        health_fact = fact_sensor_health_daily(silver)

        log("Building fact_machine_daily...")
        # device_df no longer passed -- real Silver already carries
        # machine_name/machine_type/production_line/plant_id directly.
        machine_fact = fact_machine_daily(silver, health_fact)

        tables = {
            "dim_sensor": dim_sensor,
            "dim_date": dim_date,
            "fact_sensor_reading_daily": reading_fact,
            "fact_sensor_health_daily": health_fact,
            "fact_machine_daily": machine_fact,
        }

        log("Validating dimensions and facts...")
        for name, df in tables.items():
            kind, keys = GOLD_TABLE_KEYS[name]
            if kind == "dimension":
                validate_dimension(df, name, keys)
            else:
                validate_fact(df, name, keys)
        log("Validation passed.")

        written = {}
        for name, df in tables.items():
            log(f"Writing {name} -> {GOLD_TABLE_PATHS[name]} (mode={GOLD_WRITE_MODE}) ...")
            written[name] = _write_table(df, name)
            count = written[name].count()
            log(f"{name}: {count} row(s) written")

        if USE_HF_STORAGE:
            log("Syncing Gold Delta tables back to Hugging Face bucket...")
            total_files = 0
            for name in tables:
                prefix = f"{HF_GOLD_PREFIX.rstrip('/')}/{name}"
                n = upload_directory(s3_client, HF_BUCKET_NAME, GOLD_TABLE_PATHS[name], prefix)
                log(f"Uploaded {n} file(s) for {name} -> s3://{HF_BUCKET_NAME}/{prefix}")
                total_files += n

            write_checkpoint(
                s3_client,
                HF_BUCKET_NAME,
                HF_GOLD_CHECKPOINT_KEY,
                watermark=silver_watermark,
                processing_job_id=PROCESSING_JOB_ID,
                files_processed=total_files,
            )
            log(f"Gold checkpoint advanced to {silver_watermark.isoformat()}")

        return written

    finally:
        spark.stop()


if __name__ == "__main__":
    main()