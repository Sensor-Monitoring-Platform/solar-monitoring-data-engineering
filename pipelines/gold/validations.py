from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def require_columns(df: DataFrame, columns: list[str], name: str) -> None:
    missing = set(columns) - set(df.columns)
    if missing:
        raise ValueError(f"{name} is missing columns: {sorted(missing)}")


def validate_not_null(df: DataFrame, columns: list[str], name: str) -> None:
    for column in columns:
        if df.filter(F.col(column).isNull()).limit(1).count():
            raise ValueError(f"{name}.{column} contains NULL values")


def validate_unique(df: DataFrame, keys: list[str], name: str) -> None:
    duplicates = (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicates:
        raise ValueError(
            f"{name} violates unique grain: {keys}"
        )


def validate_fact(
    df: DataFrame,
    name: str,
    keys: list[str],
) -> None:
    validate_not_null(df, keys, name)
    validate_unique(df, keys, name)


def validate_dimension(
    df: DataFrame,
    name: str,
    key: str,
) -> None:
    validate_not_null(df, [key], name)
    validate_unique(df, [key], name)