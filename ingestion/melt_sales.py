"""
Ingestion: melt the wide M5 sales file into long format and land it as Parquet.
Reads in row-chunks so memory stays flat regardless of total output size.
"""
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

# --- Config ---
RAW_PATH  = Path("data/raw/sales_train_validation.csv")
OUT_PATH  = Path("data/raw/sales_long.parquet")   # the landed bronze output
ID_COLS   = ["id", "item_id", "dept_id", "cat_id", "store_id", "state_id"]
CHUNKSIZE = 5000                                   # wide rows per chunk

def main():
    # pandas can stream a CSV in chunks: each iteration yields CHUNKSIZE rows.
    reader = pd.read_csv(RAW_PATH, chunksize=CHUNKSIZE)

    writer = None          # we open the Parquet writer lazily, on the first chunk
    total_rows = 0

    for i, chunk in enumerate(reader):
        # Day columns are every column that isn't an identifier.
        day_cols = [c for c in chunk.columns if c.startswith("d_")]

        long = chunk.melt(
            id_vars=ID_COLS,
            value_vars=day_cols,
            var_name="d",
            value_name="units_sold",
        )

        # Be explicit about types: units are small non-negative ints,
        # the day label is a short string. Typing here = a real schema.
        long["units_sold"] = long["units_sold"].astype("int32")
        long["d"] = long["d"].astype("string")

        # Convert the pandas chunk to an Arrow table (Parquet's native form).
        table = pa.Table.from_pandas(long, preserve_index=False)

        # Open the writer using the FIRST chunk's schema, then reuse it.
        if writer is None:
            writer = pq.ParquetWriter(OUT_PATH, table.schema, compression="snappy")

        writer.write_table(table)

        total_rows += len(long)
        print(f"chunk {i:>3}: melted {len(chunk):>5} wide rows -> "
              f"{len(long):>8} long rows | running total: {total_rows:,}")

    if writer is not None:
        writer.close()

    print(f"\nDone. Wrote {total_rows:,} rows to {OUT_PATH}")

if __name__ == "__main__":
    main()