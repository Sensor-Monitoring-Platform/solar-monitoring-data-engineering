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
from validations import (
    require_columns,
    validate_dimension,
    validate_fact,
)

# Per-table local Delta paths, derived from the single GOLD_OUTPUT_PATH
# config value -- config.py deliberately only defines the base directory,
# not one constant per table, so this mapping lives here instead.
GOLD_TABLE_PATHS = {
    name: str(Path(GOLD_OUTPUT_PATH) / name) for name in GOLD_SCHEMAS
}

# Business key per table, used both for grain validation and to decide
# validate_fact vs validate_dimension.
GOLD_TABLE_KEYS = {
    "dim_sensor": ("dimension", "sensor_key"),
    "dim_date": ("dimension", "date_key"),
    "fact_sensor_reading_daily": ("fact", ["device_id", "date_key"]),
    "fact_sensor_health_daily": ("fact", ["device_id", "date_key"]),
    "fact_machine_daily": ("fact", ["machine_id", "date_key"]),
}


def _apply_lookback(silver):
    """
    Restrict Silver to the trailing GOLD_LOOKBACK_DAYS days (inclusive of
    the max event_date present), when configured. GOLD_LOOKBACK_DAYS=0
    means "no limit -- use all available Silver history", matching
    config.py's own comment.
    """
    if GOLD_LOOKBACK_DAYS <= 0:
        return silver

    max_date = silver.agg(F.max("event_date")).collect()[0][0]
    if max_date is None:
        return silver

    cutoff = max_date - timedelta(days=GOLD_LOOKBACK_DAYS - 1)
    print(f"GOLD_LOOKBACK_DAYS={GOLD_LOOKBACK_DAYS} -- restricting Silver to event_date >= {cutoff.isoformat()}")
    return silver.filter(F.col("event_date") >= F.lit(cutoff))


def _write_table(df, name: str):
    """
    Enforce the declared GOLD_SCHEMAS contract for this table, then write
    it to its own local Delta path under GOLD_OUTPUT_PATH.

    Full overwrite per table (GOLD_WRITE_MODE, default "overwrite"): every
    Gold table here is a complete re-derivation from Silver each run
    (dim_sensor is SCD1 over the whole device topology; every fact is a
    full re-aggregation), so there is no meaningful incremental write.
    """
    expected_columns = [f.name for f in GOLD_SCHEMAS[name].fields]

    # Friendlier error than a bare AnalysisException if a fact/dimension
    # function silently dropped or renamed a column upstream.
    require_columns(df, expected_columns, name)

    # Enforce exact column set + order, and drop anything extra that
    # rode along -- schema drift should fail loudly, not silently ride
    # into the Delta table.
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
        s3_client = create_s3_client()

        # Run-level checkpoint check, before Spark starts: if Silver's
        # watermark hasn't moved since Gold's own checkpoint recorded,
        # there's nothing new to rebuild five tables from, so skip the
        # entire run rather than pay for JVM startup + a full Silver
        # download only to recompute identical tables.
        silver_checkpoint = read_checkpoint(s3_client, HF_BUCKET_NAME, HF_SILVER_CHECKPOINT_KEY)
        silver_watermark = parse_checkpoint_watermark(silver_checkpoint)

        if silver_watermark is None:
            print("Silver has no checkpoint yet -- nothing to process. Gold layer skipped.")
            return

        gold_checkpoint = read_checkpoint(s3_client, HF_BUCKET_NAME, HF_GOLD_CHECKPOINT_KEY)
        gold_last_processed = parse_checkpoint_watermark(gold_checkpoint)

        if gold_last_processed == silver_watermark:
            print(
                f"Silver hasn't advanced since Gold's last run "
                f"(watermark {silver_watermark.isoformat()}) -- nothing to process."
            )
            return

        print("Downloading Silver Delta table from HF bucket...")
        n = download_prefix(s3_client, HF_BUCKET_NAME, HF_SILVER_PREFIX, SILVER_INPUT_PATH)
        print(f"Downloaded {n} Silver file(s)")

        if n == 0:
            print("Silver table is empty in the bucket -- nothing to process. Gold layer skipped.")
            return

    spark = create_spark()

    try:
        silver = read_silver_data(spark)

        if silver.rdd.isEmpty():
            print("Silver table has no rows -- nothing to process. Gold layer skipped.")
            return

        silver = _apply_lookback(silver)

        devices = load_yaml(DEVICE_METADATA)
        sensor_config = load_yaml(SENSOR_METADATA)

        device_df, sensor_config_df = create_metadata_dataframes(
            spark, devices, sensor_config
        )

        dim_sensor = build_dim_sensor(device_df, sensor_config_df)

        date_range = silver.agg(
            F.min(F.col("event_date")).alias("min_date"),
            F.max(F.col("event_date")).alias("max_date"),
        ).collect()[0]
        start_date = date_range["min_date"].isoformat()
        end_date = date_range["max_date"].isoformat()

        dim_date = build_dim_date(spark, start_date, end_date)

        reading_fact = fact_sensor_reading_daily(silver)
        health_fact = fact_sensor_health_daily(silver)
        machine_fact = fact_machine_daily(silver, device_df, health_fact)

        tables = {
            "dim_sensor": dim_sensor,
            "dim_date": dim_date,
            "fact_sensor_reading_daily": reading_fact,
            "fact_sensor_health_daily": health_fact,
            "fact_machine_daily": machine_fact,
        }

        # Validate grain/nullability BEFORE writing anything, so a bad
        # table can't get partially synced to HF.
        for name, df in tables.items():
            kind, keys = GOLD_TABLE_KEYS[name]
            if kind == "dimension":
                validate_dimension(df, name, keys)
            else:
                validate_fact(df, name, keys)

        written = {}
        for name, df in tables.items():
            print(f"Writing {name} -> {GOLD_TABLE_PATHS[name]} (mode={GOLD_WRITE_MODE}) ...")
            written[name] = _write_table(df, name)
            count = written[name].count()
            print(f"{name}: {count} row(s) written")

        if USE_HF_STORAGE:
            print("Syncing Gold Delta tables back to Hugging Face bucket...")
            total_files = 0
            for name in tables:
                prefix = f"{HF_GOLD_PREFIX.rstrip('/')}/{name}"
                n = upload_directory(s3_client, HF_BUCKET_NAME, GOLD_TABLE_PATHS[name], prefix)
                print(f"Uploaded {n} file(s) for {name} -> s3://{HF_BUCKET_NAME}/{prefix}")
                total_files += n

            # Only record the checkpoint after every table's write AND
            # upload has succeeded -- if any failed, the next run should
            # retry a full rebuild rather than think this Silver
            # watermark is already reflected in Gold when it isn't.
            write_checkpoint(
                s3_client,
                HF_BUCKET_NAME,
                HF_GOLD_CHECKPOINT_KEY,
                watermark=silver_watermark,
                processing_job_id=PROCESSING_JOB_ID,
                files_processed=total_files,
            )
            print(f"Gold checkpoint advanced to {silver_watermark.isoformat()}")

        return written

    finally:
        spark.stop()


if __name__ == "__main__":
    main()