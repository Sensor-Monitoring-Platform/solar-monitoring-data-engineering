"""
scripts/load_silver_to_tidb.py

Reads the Silver Delta table from the Hugging Face bucket and loads it
into a TiDB Cloud table, so you can browse/query it from the TiDB Cloud
SQL Editor. Same rationale as scripts/load_bronze_to_tidb.py for why
this exists instead of TiDB Cloud's native cloud-storage import.

Unlike the Bronze loader, this ALWAYS does a full replace -- there's no
incremental mode. Silver fully rebuilds its whole table on every run
(see pipelines/silver/main.py), and every row gets a fresh
processing_timestamp regardless of whether the underlying data actually
changed for that row. There's no processing_job_id column on
SILVER_OUTPUT_SCHEMA the way Bronze has, so there's no reliable per-row
signal to filter "just this run's rows" the way the Bronze loader does --
attempting one would either load nothing (nothing uniquely identifies
"new" rows) or load duplicates. A full replace matches Silver's own
rebuild semantics exactly, so it's the only mode offered here.

Install once:
    pip install "deltalake[pyarrow]" pandas sqlalchemy pymysql

Usage:
    export HF_NAMESPACE=... HF_ACCESS_KEY_ID=... HF_SECRET_ACCESS_KEY=... HF_BUCKET_NAME=sensors-bucket
    export TIDB_HOST=... TIDB_PORT=4000 TIDB_USER=... TIDB_PASSWORD=... TIDB_DATABASE=bronze_data
    python scripts/load_silver_to_tidb.py
    python scripts/load_silver_to_tidb.py --tidb-table silver_readings_v2   # custom target name
"""

import argparse
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipelines" / "silver"))

from config import HF_BUCKET_NAME, HF_SILVER_PREFIX  # noqa: E402
from hf_storage import create_s3_client, download_prefix  # noqa: E402


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


def main():
    parser = argparse.ArgumentParser(description="Load Silver data into TiDB Cloud (full replace).")
    parser.add_argument("--tidb-table", default="silver_readings", help="Target table name in TiDB.")
    args = parser.parse_args()

    s3_client = create_s3_client()

    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = str(Path(tmp_dir) / "silver")
        print(f"Downloading s3://{HF_BUCKET_NAME}/{HF_SILVER_PREFIX} -> {local_path} ...")
        n = download_prefix(s3_client, HF_BUCKET_NAME, HF_SILVER_PREFIX, local_path)
        print(f"Downloaded {n} file(s).")

        if n == 0:
            print("Silver table is empty or doesn't exist yet -- nothing to load.")
            return

        from deltalake import DeltaTable

        dt = DeltaTable(local_path)
        df = dt.to_pandas()
        print(f"Read {len(df)} row(s) from the Delta table.")

    print(f"Connecting to TiDB Cloud (database={os.environ['TIDB_DATABASE']})...")
    engine = build_tidb_engine()

    print(f"Writing {len(df)} row(s) to TiDB table '{args.tidb_table}' (full replace)...")
    df.to_sql(args.tidb_table, engine, if_exists="replace", index=False, chunksize=1000)

    print(f"Done. {len(df)} row(s) written to TiDB Cloud table '{args.tidb_table}'.")


if __name__ == "__main__":
    main()