import pandas as pd

PATH = "data/raw/sales_train_validation.csv"

# Read ALL rows, but ONLY the 6 identifier columns.
# usecols restricts which columns load, so we skip all 1,913 day
# columns entirely. This reads the full file's dimensions cheaply.
id_cols = ["id", "item_id", "dept_id", "cat_id", "store_id", "state_id"]
dims = pd.read_csv(PATH, usecols=id_cols)

print("Total item-store rows in the file:", len(dims))
print()
for col in ["state_id", "store_id", "cat_id", "dept_id"]:
    vals = sorted(dims[col].unique())
    print(f"{col}: {dims[col].nunique()} unique -> {vals}")

print("\nItems (item_id):", dims["item_id"].nunique(), "unique products")