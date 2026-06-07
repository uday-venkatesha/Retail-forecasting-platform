"""
Load the bronze Parquet into Postgres using COPY (bulk load), streaming in
batches so we never hold the full 58M rows in memory.

After loading, runs the GX bronze layer checkpoint as a quality gate. If the
gate fails, the script exits with a non-zero status so callers (Airflow, CI,
shell pipelines) know the pipeline must not proceed.
"""
import io
import os
import sys
import time
import psycopg2
import pyarrow.parquet as pq
from dotenv import load_dotenv

from gx_gate import run_bronze_gate, GxGateFailure

load_dotenv()

PARQUET_PATH = "data/raw/sales_long.parquet"
TARGET_TABLE = "bronze.sales_long"
BATCH_ROWS = 1_000_000

conn = psycopg2.connect(
    host=os.environ["POSTGRES_HOST"],
    port=os.environ["POSTGRES_PORT"],
    dbname=os.environ["POSTGRES_DB"],
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
)


def copy_batch(cursor, table, df):
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    cursor.copy_expert(f"COPY {table} FROM STDIN WITH (FORMAT csv)", buf)


def main():
    # 1. Load Parquet into Postgres
    pf = pq.ParquetFile(PARQUET_PATH)
    cols = ["id","item_id","dept_id","cat_id","store_id","state_id","d","units_sold"]

    total = 0
    start = time.time()
    with conn:
        with conn.cursor() as cur:
            for batch in pf.iter_batches(batch_size=BATCH_ROWS, columns=cols):
                df = batch.to_pandas()
                copy_batch(cur, TARGET_TABLE, df)
                total += len(df)
                print(f"loaded {total:>12,} rows | {time.time()-start:6.1f}s")

    conn.close()
    print(f"\nLoad complete. {total:,} rows in {time.time()-start:.1f}s")

    # 2. Run the GX quality gate
    try:
        run_bronze_gate()
    except GxGateFailure as e:
        print(f"\nPIPELINE HALTED: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()