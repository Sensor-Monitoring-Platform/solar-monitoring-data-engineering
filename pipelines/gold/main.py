from pyspark.sql import functions as F

from config import DEVICE_METADATA, SENSOR_METADATA
from dimensions import build_dim_date, build_dim_sensor
from facts import (
    fact_machine_daily,
    fact_sensor_health_daily,
    fact_sensor_reading_daily,
)
from metadata_loader import (
    create_metadata_dataframes,
    load_yaml,
)
from reader import read_silver_data
from spark_session import create_spark
from validations import (
    validate_dimension,
    validate_fact,
)


def main():
    spark = create_spark()

    try:
        silver = read_silver_data(spark)

        devices = load_yaml(DEVICE_METADATA)
        sensor_config = load_yaml(SENSOR_METADATA)

        device_df, sensor_config_df = create_metadata_dataframes(
            spark, devices, sensor_config
        )

        # NOTE: We intentionally do NOT enrich `silver` with device_df
        # attributes here. fact_sensor_reading_daily / fact_sensor_health_daily
        # don't need plant/machine descriptive columns, and fact_machine_daily
        # now performs its own join internally — so each fact function is
        # self-sufficient and doesn't depend on caller-side prep.

        dim_sensor = build_dim_sensor(device_df, sensor_config_df)

        # Extract date range from Silver data
        if not silver.rdd.isEmpty():
            date_range = silver.agg(
                F.min(F.col("event_date")).alias("min_date"),
                F.max(F.col("event_date")).alias("max_date"),
            ).collect()[0]
            start_date = (
                date_range["min_date"].isoformat()
                if date_range["min_date"]
                else "2026-01-01"
            )
            end_date = (
                date_range["max_date"].isoformat()
                if date_range["max_date"]
                else "2026-12-31"
            )
        else:
            start_date, end_date = "2026-01-01", "2026-12-31"

        dim_date = build_dim_date(spark, start_date, end_date)

        reading_fact = fact_sensor_reading_daily(silver)
        health_fact = fact_sensor_health_daily(silver)

        # machine_fact depends on health_fact (reuses its completeness/
        # health numbers) and on device_df (for plant/machine attributes).
        machine_fact = fact_machine_daily(silver, device_df, health_fact)

        validate_dimension(dim_sensor, "dim_sensor", "sensor_key")
        validate_dimension(dim_date, "dim_date", "date_key")

        validate_fact(
            reading_fact,
            "fact_sensor_reading_daily",
            ["device_id", "date_key"],
        )

        validate_fact(
            health_fact,
            "fact_sensor_health_daily",
            ["device_id", "date_key"],
        )

        validate_fact(
            machine_fact,
            "fact_machine_daily",
            ["machine_id", "date_key"],
        )

        return {
            "dim_sensor": dim_sensor,
            "dim_date": dim_date,
            "fact_sensor_reading_daily": reading_fact,
            "fact_sensor_health_daily": health_fact,
            "fact_machine_daily": machine_fact,
        }

    finally:
        spark.stop()


if __name__ == "__main__":
    main()