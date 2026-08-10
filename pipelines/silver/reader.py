from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

try:
    from .config import BRONZE_INPUT_PATH
    from .schemas import BRONZE_INPUT_SCHEMA
except ImportError:  # pragma: no cover
    from config import BRONZE_INPUT_PATH
    from schemas import BRONZE_INPUT_SCHEMA


def read_bronze_data(spark: SparkSession, bronze_path: str = BRONZE_INPUT_PATH) -> DataFrame:
    bronze_path_obj = Path(bronze_path)
    if not bronze_path_obj.exists():
        return spark.createDataFrame([], schema=BRONZE_INPUT_SCHEMA)

    df = spark.read.option("mergeSchema", "true").parquet(str(bronze_path_obj))

    if df.rdd.isEmpty():
        return spark.createDataFrame([], schema=BRONZE_INPUT_SCHEMA)

    df = df.filter(F.col("is_corrupt") == False)
    df = df.filter(F.col("error_reason").isNull())
    df = df.dropDuplicates(["device_id", "timestamp", "reading_value", "record_hash"])

    return df
