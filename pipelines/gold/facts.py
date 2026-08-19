from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def fact_sensor_reading_daily(silver: DataFrame) -> DataFrame:
    """Daily analytical summary: one row per sensor/day."""
    return (
        silver
        .groupBy("device_id", "event_date")
        .agg(
            F.count("reading_value").alias("reading_count"),
            F.avg("reading_value").alias("avg_reading_value"),
            F.min("reading_value").alias("min_reading_value"),
            F.max("reading_value").alias("max_reading_value"),

            F.sum(F.when(F.col("reading_status") == "NORMAL", 1).otherwise(0))
                .alias("normal_count"),
            F.sum(F.when(F.col("reading_status") == "WARNING", 1).otherwise(0))
                .alias("warning_count"),
            F.sum(F.when(F.col("reading_status") == "CRITICAL", 1).otherwise(0))
                .alias("critical_count"),

            F.sum(F.col("is_outlier").cast("int")).alias("outlier_count"),
            F.sum(F.col("is_late_arrival").cast("int")).alias("late_arrival_count"),

            F.avg("rolling_stddev").alias("avg_rolling_stddev"),
            F.max("rolling_stddev").alias("max_rolling_stddev"),
            F.avg("delta_previous").alias("avg_delta_previous"),
            F.max("delta_previous").alias("max_delta_previous"),
            F.avg("percent_change").alias("avg_percent_change"),
            F.max("percent_change").alias("max_percent_change"),
            F.avg("reading_velocity").alias("avg_reading_velocity"),
            F.max("reading_velocity").alias("max_reading_velocity"),
            F.avg("reading_acceleration").alias("avg_reading_acceleration"),
            F.max("reading_acceleration").alias("max_reading_acceleration"),

            F.max("consecutive_outliers").alias("max_consecutive_outliers"),
            F.max("consecutive_nulls").alias("max_consecutive_nulls"),

            F.first("device_type").alias("device_type"),
            F.first("machine_id").alias("machine_id"),
            F.min("event_timestamp").alias("first_reading_timestamp"),
            F.max("event_timestamp").alias("last_reading_timestamp"),
        )
        .withColumn("date_key", F.date_format("event_date", "yyyyMMdd").cast("int"))
        .withColumn("sensor_key", F.xxhash64(F.col("device_id")).cast("long"))
        .withColumn("processing_timestamp", F.current_timestamp())
    )


def fact_sensor_health_daily(silver: DataFrame) -> DataFrame:
    """Daily sensor reliability summary: one row per sensor/day."""
    daily = (
        silver
        .groupBy("device_id", "event_date")
        .agg(
            F.count("*").alias("total_events"),
            F.count("reading_value").alias("actual_reading_count"),

            F.sum(F.when(F.col("reading_status") == "NORMAL", 1).otherwise(0))
                .alias("normal_count"),
            F.sum(F.when(F.col("reading_status") == "WARNING", 1).otherwise(0))
                .alias("warning_count"),
            F.sum(F.when(F.col("reading_status") == "CRITICAL", 1).otherwise(0))
                .alias("critical_count"),

            F.sum(F.col("is_outlier").cast("int")).alias("outlier_count"),
            F.sum(F.col("is_late_arrival").cast("int")).alias("late_arrival_count"),
            F.max("consecutive_nulls").alias("max_consecutive_nulls"),
            F.max("consecutive_outliers").alias("max_consecutive_outliers"),
            F.first("sampling_frequency_seconds").alias(
                "sampling_frequency_seconds"
            ),
            F.first("device_type").alias("device_type"),
            F.first("machine_id").alias("machine_id"),
        )
    )

    return (
        daily
        .withColumn(
            "expected_reading_count",
            F.when(
                F.col("sampling_frequency_seconds") > 0,
                F.lit(86400) / F.col("sampling_frequency_seconds"),
            ),
        )
        .withColumn(
            "missing_reading_count",
            F.when(
                F.col("expected_reading_count").isNotNull(),
                F.greatest(
                    F.col("expected_reading_count") - F.col("actual_reading_count"),
                    F.lit(0),
                ),
            ),
        )
        .withColumn(
            "data_completeness_percentage",
            F.when(
                F.col("expected_reading_count") > 0,
                F.col("actual_reading_count") / F.col("expected_reading_count") * 100,
            ),
        )
        .withColumn(
            "normal_percentage",
            F.when(
                F.col("total_events") > 0,
                F.col("normal_count") / F.col("total_events") * 100,
            ),
        )
        .withColumn(
            "warning_percentage",
            F.when(
                F.col("total_events") > 0,
                F.col("warning_count") / F.col("total_events") * 100,
            ),
        )
        .withColumn(
            "critical_percentage",
            F.when(
                F.col("total_events") > 0,
                F.col("critical_count") / F.col("total_events") * 100,
            ),
        )
        .withColumn(
            "outlier_percentage",
            F.when(
                F.col("total_events") > 0,
                F.col("outlier_count") / F.col("total_events") * 100,
            ),
        )
        .withColumn(
            "late_arrival_percentage",
            F.when(
                F.col("total_events") > 0,
                F.col("late_arrival_count") / F.col("total_events") * 100,
            ),
        )
        .withColumn(
            "health_score",
            F.when(
                F.col("data_completeness_percentage").isNotNull(),
                (F.col("data_completeness_percentage") * 0.4
                 + (100 - F.col("critical_percentage")) * 0.3
                 + (100 - F.col("outlier_percentage")) * 0.2
                 + (100 - F.col("late_arrival_percentage")) * 0.1) / 100,
            ),
        )
        .withColumn("date_key", F.date_format("event_date", "yyyyMMdd").cast("int"))
        .withColumn("sensor_key", F.xxhash64(F.col("device_id")).cast("long"))
        .withColumn("processing_timestamp", F.current_timestamp())
    )


def fact_machine_daily(
    silver: DataFrame,
    device_df: DataFrame,
    health_fact: DataFrame,
) -> DataFrame:
    """
    Daily machine summary: one row per machine/day.

    Design notes:
      - machine_id alone is not on the raw Silver schema's descriptive
        attributes (plant_id, machine_name, ...), so we explicitly join
        device_df here instead of relying on the caller to have already
        enriched `silver`.
      - Completeness, health score, and per-sensor-status counts are
        rolled up FROM fact_sensor_health_daily rather than recomputed
        independently, so sensor-grain and machine-grain numbers stay
        consistent with each other.
      - Physical measurement metrics (temperature/vibration/pressure/
        current) still come straight from Silver, since health_fact
        doesn't carry reading values.
    """

    machine_attrs = (
        device_df
        .select(
            "machine_id",
            "machine_name",
            "machine_type",
            "production_line",
            "plant_id",
        )
        .dropDuplicates(["machine_id"])
    )

    enriched = silver.join(machine_attrs, "machine_id", "left")

    # ------------------------------------------------------------------
    # Physical measurement metrics, straight from Silver readings.
    # ------------------------------------------------------------------
    physical = (
        enriched
        .groupBy("machine_id", "event_date")
        .agg(
            F.avg("moving_average").alias("avg_sensor_reading"),

            F.max(
                F.when(
                    F.col("device_type") == "temperature_process",
                    F.col("reading_value"),
                )
            ).alias("max_process_temperature"),

            F.max(
                F.when(
                    F.col("device_type") == "temperature_ambient",
                    F.col("reading_value"),
                )
            ).alias("max_ambient_temperature"),

            F.max(
                F.when(
                    F.col("device_type") == "vibration",
                    F.col("reading_value"),
                )
            ).alias("max_vibration"),

            F.max(
                F.when(
                    F.col("device_type") == "pressure",
                    F.col("reading_value"),
                )
            ).alias("max_pressure"),

            F.max(
                F.when(
                    F.col("device_type") == "current_draw",
                    F.col("reading_value"),
                )
            ).alias("max_current"),

            F.first("machine_name").alias("machine_name"),
            F.first("machine_type").alias("machine_type"),
            F.first("production_line").alias("production_line"),
            F.first("plant_id").alias("plant_id"),
            F.first("machine_operating_condition").alias(
                "machine_operating_condition"
            ),
        )
    )

    # ------------------------------------------------------------------
    # Reliability / completeness / health rolled up from the sensor-grain
    # health fact, so both facts agree with each other.
    # ------------------------------------------------------------------
    machine_health = (
        health_fact
        .groupBy("machine_id", "event_date")
        .agg(
            F.countDistinct("device_id").alias("sensor_count"),
            F.sum("actual_reading_count").alias("total_readings"),
            F.sum("expected_reading_count").alias("expected_reading_count_total"),

            F.sum("critical_count").alias("critical_readings"),
            F.sum("warning_count").alias("warning_readings"),
            F.sum("outlier_count").alias("outlier_readings"),
            F.sum("late_arrival_count").alias("late_arrival_count"),

            F.countDistinct(
                F.when(F.col("critical_count") > 0, F.col("device_id"))
            ).alias("critical_sensor_count"),
            F.countDistinct(
                F.when(F.col("warning_count") > 0, F.col("device_id"))
            ).alias("warning_sensor_count"),

            F.avg("health_score").alias("avg_sensor_health_score"),
        )
    )

    return (
        physical
        .join(machine_health, ["machine_id", "event_date"], "left")
        .withColumn("active_sensor_count", F.col("sensor_count"))
        .withColumn(
            "unhealthy_sensor_count",
            F.col("critical_sensor_count") + F.col("warning_sensor_count"),
        )
        .withColumn(
            "data_completeness_percentage",
            F.when(
                F.col("expected_reading_count_total") > 0,
                F.col("total_readings")
                / F.col("expected_reading_count_total")
                * 100,
            ),
        )
        .drop("expected_reading_count_total")
        .withColumn("date_key", F.date_format("event_date", "yyyyMMdd").cast("int"))
        .withColumn("processing_timestamp", F.current_timestamp())
    )