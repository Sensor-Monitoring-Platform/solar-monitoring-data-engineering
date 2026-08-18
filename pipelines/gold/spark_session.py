from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

try:
    from .config import APP_NAME, MASTER, SHUFFLE_PARTITIONS, TIMEZONE
except ImportError:  # pragma: no cover - direct script execution
    from config import APP_NAME, MASTER, SHUFFLE_PARTITIONS, TIMEZONE


def create_spark():
    builder = (
        SparkSession.builder.appName(APP_NAME)
        .master(MASTER)
        .config("spark.sql.session.timeZone", TIMEZONE)
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark