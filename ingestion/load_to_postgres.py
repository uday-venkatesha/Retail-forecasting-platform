"""
Load the bronze Parquet into Postgres using COPY (bulk load), streaming in
batches so we never hold the full 58M rows in memory.
"""
import io
import os
import time
import psycopg2
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv()  # read .env into environment variables

PARQUET_PATH = "data/raw/sales_long.parquet"
TARGET_TABLE = "bronze.sales_long"
BATCH_ROWS = 1_000_000  # rows per COPY batch

# Build the connection from environment variables (single source of truth).
conn = psycopg2.connect(
    host=os.environ["POSTGRES_HOST"],
    port=os.environ["POSTGRES_PORT"],
    dbname=os.environ["POSTGRES_DB"],
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
)

def copy_batch(cursor, table, df):
    """Stream one DataFrame batch into Postgres via COPY."""
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)  # serialize to in-memory CSV text
    buf.seek(0)                                 # rewind to the start
    cursor.copy_expert(
        f"COPY {table} FROM STDIN WITH (FORMAT csv)",
        buf,
    )

def main():
    pf = pq.ParquetFile(PARQUET_PATH)
    cols = ["id", "item_id", "dept_id", "cat_id", "store_id", "state_id", "d", "units_sold"]

    total = 0
    start = time.time()
    with conn:
        with conn.cursor() as cur:
            # Iterate the Parquet in batches of row groups -> pandas -> COPY.
            for batch in pf.iter_batches(batch_size=BATCH_ROWS, columns=cols):
                df = batch.to_pandas()
                copy_batch(cur, TARGET_TABLE, df)
                total += len(df)
                elapsed = time.time() - start
                print(f"loaded {total:>12,} rows | {elapsed:6.1f}s elapsed")

    conn.close()
    print(f"\nDone. Loaded {total:,} rows in {time.time()-start:.1f}s")

if __name__ == "__main__":
    main()