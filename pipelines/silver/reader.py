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
    if not bronze_path_obj.exists() or not (bronze_path_obj / "_delta_log").exists():
        return spark.createDataFrame([], schema=BRONZE_INPUT_SCHEMA)

    # Bronze is written as a Delta table (see pipelines/bronze/main.py),
    # not plain Parquet. Reading it with .parquet(...) would ignore
    # _delta_log/ entirely -- it happens to work while the table has
    # never been compacted or vacuumed, but breaks the moment it has,
    # since a bare Parquet reader can't tell current files from
    # superseded ones. .format("delta") respects the transaction log.
    df = spark.read.format("delta").load(str(bronze_path_obj))

    if df.rdd.isEmpty():
        return spark.createDataFrame([], schema=BRONZE_INPUT_SCHEMA)

    # Defensive: Bronze's own reader never writes is_corrupt=True /
    # error_reason-populated rows into the bronze table itself (those go
    # to the separate quarantine table), so this should always be a
    # no-op in practice -- kept as a safety net in case that invariant
    # ever changes upstream.
    df = df.filter(F.col("is_corrupt") == False)  # noqa: E712
    df = df.filter(F.col("error_reason").isNull())

    # record_hash is already SHA-256(device_id | timestamp | reading_value)
    # -- see pipelines/bronze/reader.py -- so deduping on it alone is
    # equivalent to deduping on those three fields, without repeating the
    # definition in two places.
    df = df.dropDuplicates(["record_hash"])

    return df