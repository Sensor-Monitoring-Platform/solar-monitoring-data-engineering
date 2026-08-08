"""
scripts/load_bronze_to_tidb.py

Reads the Bronze (or quarantine) Delta table from the Hugging Face bucket
and loads it into a TiDB Cloud table, so you can browse/query it from the
TiDB Cloud SQL Editor.

Why this exists instead of TiDB Cloud's native "Import from Cloud
Storage": that feature only supports real AWS S3 / GCS / Azure Blob /
Alibaba OSS (with each provider's own auth), not arbitrary S3-compatible
endpoints like Hugging Face's, and it only understands CSV/Parquet/
Dumpling/Aurora-snapshot as source formats — not Delta Lake's
transaction log. This script reads the Delta table properly (respecting
_delta_log, via the same deltalake library scripts/read_bronze.py uses)
and pushes the resulting DataFrame over TiDB's MySQL protocol instead.

Install once:
    pip install deltalake pandas sqlalchemy pymysql

Get your connection details from the TiDB Cloud console:
    Cluster -> Connect -> Connect With: SQLAlchemy (or General/Python)
    That dialog shows the exact host, port, user, and whether your
    cluster requires a CA certificate path.

Usage:
    export TIDB_HOST=xxxxxx.aws.tidbcloud.com
    export TIDB_PORT=4000
    export TIDB_USER=xxxxxx.root
    export TIDB_PASSWORD=...
    export TIDB_DATABASE=bronze_data
    python scripts/load_bronze_to_tidb.py                     # loads bronze table
    python scripts/load_bronze_to_tidb.py --table quarantine   # loads quarantine table
    python scripts/load_bronze_to_tidb.py --mode append        # keep history instead of replacing
"""

import argparse
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipelines" / "bronze"))

from config import HF_BRONZE_PREFIX, HF_BUCKET_NAME, HF_QUARANTINE_PREFIX  # noqa: E402
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
        # TiDB Cloud Serverless clusters currently use a publicly trusted
        # certificate, so the system trust store is usually enough. If
        # you get an SSL error, download the CA path shown in your
        # cluster's Connect dialog and set TIDB_SSL_CA to it.
        connect_args["ssl"] = {"ssl_mode": "REQUIRED"}

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url, connect_args=connect_args)


def main():
    parser = argparse.ArgumentParser(description="Load the Bronze Delta table into TiDB Cloud.")
    parser.add_argument("--table", choices=["bronze", "quarantine"], default="bronze")
    parser.add_argument(
        "--mode",
        choices=["replace", "append"],
        default="replace",
        help="replace: table always reflects the current Delta snapshot (default). "
        "append: keep every load as history -- you'll get duplicates on repeat runs "
        "unless you dedupe on record_hash yourself.",
    )
    parser.add_argument(
        "--tidb-table",
        default=None,
        help="Target table name in TiDB. Defaults to 'bronze_sensor_readings' or 'bronze_quarantine'.",
    )
    args = parser.parse_args()

    prefix = HF_BRONZE_PREFIX if args.table == "bronze" else HF_QUARANTINE_PREFIX
    tidb_table = args.tidb_table or ("bronze_sensor_readings" if args.table == "bronze" else "bronze_quarantine")

    s3_client = create_s3_client()

    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = str(Path(tmp_dir) / args.table)
        print(f"Downloading s3://{HF_BUCKET_NAME}/{prefix} -> {local_path} ...")
        n = download_prefix(s3_client, HF_BUCKET_NAME, prefix, local_path)
        print(f"Downloaded {n} file(s).")

        if n == 0:
            print("Table is empty or doesn't exist yet -- nothing to load.")
            return

        from deltalake import DeltaTable

        dt = DeltaTable(local_path)
        df = dt.to_pandas()
        print(f"Read {len(df)} row(s) from the Delta table.")

    print(f"Connecting to TiDB Cloud (database={os.environ['TIDB_DATABASE']})...")
    engine = build_tidb_engine()

    print(f"Writing to TiDB table '{tidb_table}' (mode={args.mode})...")
    df.to_sql(tidb_table, engine, if_exists=args.mode, index=False, chunksize=1000)

    print(f"Done. {len(df)} row(s) now in TiDB Cloud table '{tidb_table}'.")


if __name__ == "__main__":
    main()