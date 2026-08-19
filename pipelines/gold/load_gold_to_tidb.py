"""
scripts/load_gold_to_tidb.py

Reads the Gold Delta tables from the Hugging Face bucket and loads each
into its own TiDB Cloud table, so you can browse/query them from the
TiDB Cloud SQL Editor. Same rationale as scripts/load_silver_to_tidb.py
for why this exists instead of TiDB Cloud's native cloud-storage import.

Gold produces FIVE tables (dim_sensor, dim_date, fact_sensor_reading_daily,
fact_sensor_health_daily, fact_machine_daily), each synced under its own
subfolder of HF_GOLD_PREFIX by pipelines/gold/main.py, e.g.
gold/fact_machine_daily/... This script downloads and loads each one
independently.

Like the Silver loader, this ALWAYS does a full replace per table --
there's no incremental mode. Every Gold table is fully rebuilt on every
Gold run (see pipelines/gold/main.py, GOLD_WRITE_MODE), so a full replace
matches Gold's own rebuild semantics exactly.

Install once:
    pip install "deltalake[pyarrow]" pandas sqlalchemy pymysql

Usage:
    export HF_NAMESPACE=... HF_ACCESS_KEY_ID=... HF_SECRET_ACCESS_KEY=... HF_BUCKET_NAME=iot-raw
    export TIDB_HOST=... TIDB_PORT=4000 TIDB_USER=... TIDB_PASSWORD=... TIDB_DATABASE=gold_data
    python scripts/load_gold_to_tidb.py
    python scripts/load_gold_to_tidb.py --tables dim_sensor,fact_machine_daily   # subset only
    python scripts/load_gold_to_tidb.py --tidb-prefix gold_v2_                   # custom prefix
"""

import argparse
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipelines" / "gold"))

from config import HF_BUCKET_NAME, HF_GOLD_PREFIX  # noqa: E402
from hf_storage import create_s3_client, download_prefix  # noqa: E402
from schemas import GOLD_SCHEMAS  # noqa: E402

# Canonical table name list -- sourced from the schema registry rather
# than duplicated here, so a new Gold table added to schemas.py is
# automatically loadable without touching this script.
ALL_TABLES = list(GOLD_SCHEMAS.keys())


def build_tidb_engine():
    from sqlalchemy import create_engine

    host = os.environ["TIDB_HOST"]
    port = os.getenv("TIDB_PORT", "4000")
    user = os.environ["TIDB_USER"]
    password = os.environ["TIDB_PASSWORD"]
    database = os.environ["TIDB_DATABASE"]
    ssl_ca = os.getenv("TIDB_SSL_CA")  # only needed if your cluster's Connect dialog says so

    connect_args = {}
    if ssl_ca:
        connect_args["ssl"] = {"ca": ssl_ca}
    else:
        connect_args["ssl"] = {"ssl_mode": "REQUIRED"}

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url, connect_args=connect_args)


def load_one_table(s3_client, engine, table_name: str, tidb_table: str):
    from deltalake import DeltaTable

    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = str(Path(tmp_dir) / table_name)
        prefix = f"{HF_GOLD_PREFIX.rstrip('/')}/{table_name}"
        print(f"Downloading s3://{HF_BUCKET_NAME}/{prefix} -> {local_path} ...")
        n = download_prefix(s3_client, HF_BUCKET_NAME, prefix, local_path)
        print(f"Downloaded {n} file(s).")

        if n == 0:
            print(f"'{table_name}' is empty or doesn't exist yet in the bucket -- skipped.")
            return

        dt = DeltaTable(local_path)
        df = dt.to_pandas()
        print(f"Read {len(df)} row(s) from '{table_name}'.")

    print(f"Writing {len(df)} row(s) to TiDB table '{tidb_table}' (full replace)...")
    df.to_sql(tidb_table, engine, if_exists="replace", index=False, chunksize=1000)
    print(f"Done. {len(df)} row(s) written to TiDB Cloud table '{tidb_table}'.")


def main():
    parser = argparse.ArgumentParser(description="Load Gold tables into TiDB Cloud (full replace, per table).")
    parser.add_argument(
        "--tables",
        default=",".join(ALL_TABLES),
        help=f"Comma-separated subset of Gold tables to load. Default: all ({', '.join(ALL_TABLES)}).",
    )
    parser.add_argument(
        "--tidb-prefix",
        default="gold_",
        help="Prefix applied to each table name in TiDB, e.g. gold_dim_sensor.",
    )
    args = parser.parse_args()

    requested = [t.strip() for t in args.tables.split(",") if t.strip()]
    unknown = set(requested) - set(ALL_TABLES)
    if unknown:
        raise ValueError(f"Unknown Gold table(s): {sorted(unknown)}. Valid options: {ALL_TABLES}")

    s3_client = create_s3_client()

    print(f"Connecting to TiDB Cloud (database={os.environ['TIDB_DATABASE']})...")
    engine = build_tidb_engine()

    for table_name in requested:
        tidb_table = f"{args.tidb_prefix}{table_name}"
        print(f"\n--- {table_name} -> {tidb_table} ---")
        load_one_table(s3_client, engine, table_name, tidb_table)


if __name__ == "__main__":
    main()