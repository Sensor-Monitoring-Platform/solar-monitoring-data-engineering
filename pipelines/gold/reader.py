from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

try:
    from .config import SILVER_INPUT_PATH
    from .schemas import SILVER_INPUT_SCHEMA
except ImportError:
    from config import SILVER_INPUT_PATH
    from schemas import SILVER_INPUT_SCHEMA


def read_silver_data(
    spark: SparkSession,
    silver_path: str = SILVER_INPUT_PATH,
) -> DataFrame:
    path = Path(silver_path)

    if not path.exists() or not (path / "_delta_log").exists():
        return spark.createDataFrame([], schema=SILVER_INPUT_SCHEMA)

    df = spark.read.format("delta").load(str(path))

    if df.rdd.isEmpty():
        return spark.createDataFrame([], schema=SILVER_INPUT_SCHEMA)

    return df