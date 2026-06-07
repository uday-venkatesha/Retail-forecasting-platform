"""
Sub-step 4.2 (cont'd): Build an expectation suite for bronze.sell_prices.
Reuses the patterns from sales_long: column-loop generation + introspection.
"""
import os
import great_expectations as gx
import great_expectations.expectations as gxe
import psycopg2
from dotenv import load_dotenv

load_dotenv()
context = gx.get_context(mode="file", project_root_dir="great_expectations")

SUITE_NAME = "bronze_sell_prices_suite"
suite = context.suites.add_or_update(gx.ExpectationSuite(name=SUITE_NAME))
suite.expectations = []

# Introspect the store_id set (reuse same approach as sales_long)
def distinct_values(table, column):
    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"], port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"], user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    with conn.cursor() as cur:
        cur.execute(f"SELECT DISTINCT {column} FROM {table} ORDER BY 1")
        vals = [r[0] for r in cur.fetchall()]
    conn.close()
    return vals

store_values = distinct_values("bronze.sell_prices", "store_id")
print(f"store_id ({len(store_values)}): {store_values}")

# Table-level
suite.add_expectation(
    gxe.ExpectTableRowCountToBeBetween(min_value=6_000_000, max_value=7_500_000)
)
suite.add_expectation(
    gxe.ExpectTableColumnsToMatchSet(
        column_set=["store_id", "item_id", "wm_yr_wk", "sell_price"]
    )
)

# Not-null on every column (no nullable columns in this table)
for col in ["store_id", "item_id", "wm_yr_wk", "sell_price"]:
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=col))

# store_id must match the observed set
suite.add_expectation(
    gxe.ExpectColumnValuesToBeInSet(column="store_id", value_set=store_values)
)

# Walmart fiscal week id: integer, plausible range (covers 2011-2017)
suite.add_expectation(
    gxe.ExpectColumnValuesToBeBetween(column="wm_yr_wk", min_value=11101, max_value=17600)
)

# sell_price: must be positive, with a generous upper bound for retail items
suite.add_expectation(
    gxe.ExpectColumnValuesToBeBetween(column="sell_price", min_value=0.01, max_value=500.0)
)

suite.save()
print(f"\nSuite '{SUITE_NAME}' saved with {len(suite.expectations)} expectations.")

asset = context.data_sources.get("retail_warehouse").get_asset("bronze_sell_prices")
batch = asset.add_batch_definition_whole_table("full").get_batch()
print("\nRunning validation...")
results = batch.validate(suite)

print(f"\nValidation: success = {results.success}")
print(f"Expectations run: {len(results.results)}")
failed = [r for r in results.results if not r.success]
print(f"Failed: {len(failed)}")
for r in failed:
    print(f"  FAIL: {r.expectation_config.type}")
    print(f"        kwargs: {dict(r.expectation_config.kwargs)}")