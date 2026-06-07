"""
Sub-step 4.1: Initialize the GX Data Context and register the bronze layer.
Idempotent — safe to run multiple times.
"""
import os
import great_expectations as gx
from dotenv import load_dotenv

load_dotenv()

# 1. Create a file-based Data Context.
# A "context" is GX's project root — it stores config, suites, and results.
# `mode="file"` persists everything under ./great_expectations/.
context = gx.get_context(mode="file", project_root_dir="great_expectations")
print(f"Context type: {type(context).__name__}")
print(f"Context root: {context.root_directory}")

# 2. Register a Data Source pointing at our Postgres warehouse.
# Credentials come from environment variables (.env) — never hardcoded.
DATASOURCE_NAME = "retail_warehouse"

connection_string = (
    f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:"
    f"{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_HOST']}:"
    f"{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
)

# add_or_update is idempotent — re-running the script doesn't fail
# if the data source already exists.
datasource = context.data_sources.add_or_update_postgres(
    name=DATASOURCE_NAME,
    connection_string=connection_string,
)
print(f"Data source registered: {datasource.name}")

# 3. Register the bronze tables as Data Assets.
# A Data Asset is a specific table (or query) GX can validate.
assets_to_register = [
    ("bronze_sales_long",  "sales_long",  "bronze"),
    ("bronze_calendar",    "calendar",    "bronze"),
    ("bronze_sell_prices", "sell_prices", "bronze"),
]

for asset_name, table_name, schema_name in assets_to_register:
    try:
        datasource.add_table_asset(
            name=asset_name,
            table_name=table_name,
            schema_name=schema_name,
        )
        print(f"  Asset registered: {asset_name} -> {schema_name}.{table_name}")
    except Exception as e:
        # Asset already exists — that's fine on re-runs.
        if "already exists" in str(e).lower():
            print(f"  Asset exists:     {asset_name}")
        else:
            raise

print("\nSetup complete.")