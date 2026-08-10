from pyspark.sql import functions as F

from config import BRONZE_INPUT_PATH, DEVICE_METADATA, SENSOR_METADATA, SILVER_OUTPUT_PATH
from metadata_loader import create_metadata_dataframes, load_yaml
from reader import read_bronze_data
from schema_manager import SchemaManager
from schemas import SILVER_OUTPUT_SCHEMA
from transformations import prepare_silver
from spark_session import create_spark
from utils import log


def main():
    spark = create_spark(app_name="Silver Layer Pipeline")
    log("Loading metadata...")
    devices = load_yaml(DEVICE_METADATA)
    sensor_config = load_yaml(SENSOR_METADATA)

    device_df, sensor_df = create_metadata_dataframes(spark, devices, sensor_config)
    device_df.cache()
    sensor_df.cache()

    log(f"Loaded device metadata rows: {device_df.count()}")
    log(f"Loaded sensor metadata rows: {sensor_df.count()}")

    log("Reading Bronze parquet...")
    bronze_df = read_bronze_data(spark, BRONZE_INPUT_PATH)
    if bronze_df.rdd.isEmpty():
        log("No valid Bronze data found. Silver layer skipped.")
        return

    silver_df = prepare_silver(bronze_df, device_df, sensor_df)

    silver_df = silver_df.withColumn("processing_timestamp", F.current_timestamp())
    silver_df = silver_df.withColumn("event_date", F.to_date(F.col("timestamp")))
    silver_df = silver_df.withColumn("event_day", F.dayofmonth(F.col("timestamp")))

    schema_manager = SchemaManager(SILVER_OUTPUT_SCHEMA)
    schema_report = schema_manager.generate_report(silver_df)
    if schema_report["missing_columns"] or schema_report["type_changes"]:
        raise ValueError(f"Silver schema validation failed: {schema_report}")

    silver_df.write.mode("overwrite").option("mergeSchema", "true").partitionBy(
        "event_year", "event_month", "event_day", "plant_id"
    ).parquet(SILVER_OUTPUT_PATH)

    log(f"Silver output written to {SILVER_OUTPUT_PATH}")


if __name__ == "__main__":
    main()
