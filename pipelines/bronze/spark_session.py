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
    )

    # configure_spark_with_delta_pip pulls in the delta-core/delta-storage
    # jars that match whatever delta-spark version is pip-installed
    # (see requirements.txt), so we don't hand-pin a jar version here.
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark