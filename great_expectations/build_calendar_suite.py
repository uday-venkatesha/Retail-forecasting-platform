"""
Sub-step 4.2: Build an expectation suite for bronze.calendar.
Explicit-API style: every expectation written by hand so the API is visible.
"""
import great_expectations as gx
import great_expectations.expectations as gxe

# 1. Load the existing context (the one our setup_context.py created).
context = gx.get_context(mode="file", project_root_dir="great_expectations")

# 2. Create the suite. add_or_update means re-running this script is safe.
SUITE_NAME = "bronze_calendar_suite"
suite = context.suites.add_or_update(gx.ExpectationSuite(name=SUITE_NAME))

# 3. Clear any previously-added expectations so re-running is fully idempotent.
suite.expectations = []

# 4. Add expectations. Each one is an assertion about the calendar table.
suite.add_expectation(
    gxe.ExpectTableRowCountToBeBetween(min_value=1900, max_value=2000)
)
suite.add_expectation(
    gxe.ExpectTableColumnsToMatchSet(
        column_set=[
            "date", "wm_yr_wk", "weekday", "wday", "month", "year", "d",
            "event_name_1", "event_type_1", "event_name_2", "event_type_2",
            "snap_ca", "snap_tx", "snap_wi",
        ]
    )
)
# Primary keys: never null, always unique
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="date"))
suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="date"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="d"))
suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="d"))

# Value-range checks: catches drift if the calendar gets extended badly
suite.add_expectation(
    gxe.ExpectColumnValuesToBeBetween(column="year", min_value=2011, max_value=2017)
)
suite.add_expectation(
    gxe.ExpectColumnValuesToBeBetween(column="month", min_value=1, max_value=12)
)
suite.add_expectation(
    gxe.ExpectColumnValuesToBeBetween(column="wday", min_value=1, max_value=7)
)
suite.add_expectation(
    gxe.ExpectColumnValuesToBeInSet(
        column="weekday",
        value_set=["Monday","Tuesday","Wednesday","Thursday",
                   "Friday","Saturday","Sunday"],
    )
)

# SNAP flags must be 0 or 1
for col in ["snap_ca", "snap_tx", "snap_wi"]:
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeInSet(column=col, value_set=[0, 1])
    )

# 5. Save the suite to disk.
suite.save()
print(f"Suite '{SUITE_NAME}' saved with {len(suite.expectations)} expectations.")

# 6. Validate immediately against the real data so we see if any fail.
asset = (
    context.data_sources.get("retail_warehouse")
    .get_asset("bronze_calendar")
)
batch = asset.add_batch_definition_whole_table("full").get_batch()

results = batch.validate(suite)

# 7. Print a summary
print(f"\nValidation: success = {results.success}")
print(f"Expectations run: {len(results.results)}")
failed = [r for r in results.results if not r.success]
print(f"Failed: {len(failed)}")
for r in failed:
    print(f"  FAIL: {r.expectation_config.type}")
    print(f"        kwargs: {dict(r.expectation_config.kwargs)}")
    print(f"        result: {dict(r.result)}")