"""
Sub-step 4.2 (cont'd): Build an expectation suite for bronze.sales_long.
Uses column-loop generation + value-set introspection to stay concise on a
58M-row, 8-column table.
"""
import os
import great_expectations as gx
import great_expectations.expectations as gxe
import psycopg2
from dotenv import load_dotenv

load_dotenv()
context = gx.get_context(mode="file", project_root_dir="great_expectations")

SUITE_NAME = "bronze_sales_long_suite"
suite = context.suites.add_or_update(gx.ExpectationSuite(name=SUITE_NAME))
suite.expectations = []   # idempotent reset

# ---- Helper: read low-cardinality categorical values directly from Postgres.
# We introspect once so our expectation matches reality, then assert membership.
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

print("Introspecting low-cardinality columns...")
cat_values   = distinct_values("bronze.sales_long", "cat_id")
state_values = distinct_values("bronze.sales_long", "state_id")
dept_values  = distinct_values("bronze.sales_long", "dept_id")
store_values = distinct_values("bronze.sales_long", "store_id")
print(f"  cat_id   ({len(cat_values)}): {cat_values}")
print(f"  state_id ({len(state_values)}): {state_values}")
print(f"  dept_id  ({len(dept_values)}): {dept_values}")
print(f"  store_id ({len(store_values)}): {store_values}")

# ---- Now build the suite ----

# Table-level expectations
suite.add_expectation(
    gxe.ExpectTableRowCountToBeBetween(min_value=50_000_000, max_value=70_000_000)
)
suite.add_expectation(
    gxe.ExpectTableColumnsToMatchSet(
        column_set=["id","item_id","dept_id","cat_id","store_id","state_id","d","units_sold"]
    )
)

# Identifier columns: not null (we already enforce this at the DB level,
# but assert it again — the data contract is independent of the DDL).
for col in ["id","item_id","dept_id","cat_id","store_id","state_id","d","units_sold"]:
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=col))

# Low-cardinality: membership in observed set (catches new values = drift)
suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="cat_id",   value_set=cat_values))
suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="state_id", value_set=state_values))
suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="dept_id",  value_set=dept_values))
suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="store_id", value_set=store_values))

# units_sold: non-negative integer with a plausible upper bound
suite.add_expectation(
    gxe.ExpectColumnValuesToBeBetween(column="units_sold", min_value=0, max_value=10_000)
)

# d column: format check — must match d_<digits>
suite.add_expectation(
    gxe.ExpectColumnValuesToMatchRegex(column="d", regex=r"^d_\d+$")
)

suite.save()
print(f"\nSuite '{SUITE_NAME}' saved with {len(suite.expectations)} expectations.")

# Validate
asset = context.data_sources.get("retail_warehouse").get_asset("bronze_sales_long")
batch = asset.add_batch_definition_whole_table("full").get_batch()
print("\nRunning validation (this scans 58M rows — give it a moment)...")
results = batch.validate(suite)

print(f"\nValidation: success = {results.success}")
print(f"Expectations run: {len(results.results)}")
failed = [r for r in results.results if not r.success]
print(f"Failed: {len(failed)}")
for r in failed:
    print(f"  FAIL: {r.expectation_config.type}")
    print(f"        kwargs: {dict(r.expectation_config.kwargs)}")