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
        # Hadoop's default LocalFileSystem wraps every local read/write with
        # a checksum verification layer (.crc sidecar files) -- meant for
        # HDFS-style distributed reliability, not a single ephemeral CI
        # runner whose disk is already trustworthy for the few minutes a
        # job runs. Those .crc files get uploaded to the bucket alongside
        # real Delta files (see hf_storage.upload_directory), and if a
        # sidecar and its data file ever land out of sync -- two runs
        # overlapping, a checkpoint file rewritten between uploads -- Spark
        # raises ChecksumException and retries the failing task repeatedly
        # instead of failing fast. RawLocalFileSystem skips checksumming
        # entirely, which is the standard fix for this class of issue.
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    )

    # configure_spark_with_delta_pip pulls in the delta-core/delta-storage
    # jars that match whatever delta-spark version is pip-installed
    # (see requirements.txt), so we don't hand-pin a jar version here.
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark