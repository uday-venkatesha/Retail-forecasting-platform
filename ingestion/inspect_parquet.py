import pyarrow.parquet as pq
import pandas as pd

PATH = "data/raw/sales_long.parquet"

# Read Parquet METADATA only — this does NOT load the 58M rows.
# Parquet stores schema + row counts in a footer, so this is instant.
pf = pq.ParquetFile(PATH)
print("Total rows (from metadata):", f"{pf.metadata.num_rows:,}")
print("Number of row groups:", pf.metadata.num_row_groups)
print("\nSchema:")
print(pf.schema_arrow)

# Now read just the FIRST few rows to eyeball the actual data.
sample = pf.read_row_group(0).to_pandas().head(5)
print("\nFirst 5 rows:")
print(sample.to_string(index=False))