from pyspark.sql import SparkSession

try:
    from .config import APP_NAME, MASTER, SHUFFLE_PARTITIONS, TIMEZONE
except ImportError:  # pragma: no cover - direct script execution
    from config import APP_NAME, MASTER, SHUFFLE_PARTITIONS, TIMEZONE

def create_spark():
    spark = (
        SparkSession.builder.appName(APP_NAME)
        .master(MASTER)
        .config("spark.sql.session.timeZone", TIMEZONE)
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .config("spark.sql.parquet.mergeSchema", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark