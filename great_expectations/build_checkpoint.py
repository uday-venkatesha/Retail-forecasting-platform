"""
Sub-step 4.3: Wire all three bronze suites into a single Checkpoint.
After this, validating the entire bronze layer is one command.
"""
import great_expectations as gx
from great_expectations.checkpoint import Checkpoint, UpdateDataDocsAction

context = gx.get_context(mode="file", project_root_dir="great_expectations")

# A validation definition pairs a suite with a data asset.
# We create one per bronze table.
validation_definitions = []

for asset_name, suite_name in [
    ("bronze_calendar",    "bronze_calendar_suite"),
    ("bronze_sales_long",  "bronze_sales_long_suite"),
    ("bronze_sell_prices", "bronze_sell_prices_suite"),
]:
    asset = context.data_sources.get("retail_warehouse").get_asset(asset_name)
    batch_def = asset.add_batch_definition_whole_table(f"{asset_name}_full")
    suite = context.suites.get(suite_name)

    validation_def = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(
            name=f"validate_{asset_name}",
            data=batch_def,
            suite=suite,
        )
    )
    validation_definitions.append(validation_def)
    print(f"Validation definition created: validate_{asset_name}")

# A checkpoint groups one or more validation definitions.
# `actions` define what to do with the results — here, update Data Docs.
checkpoint = context.checkpoints.add_or_update(
    Checkpoint(
        name="bronze_layer_checkpoint",
        validation_definitions=validation_definitions,
        actions=[UpdateDataDocsAction(name="update_data_docs")],
        result_format="COMPLETE",
    )
)
print(f"\nCheckpoint '{checkpoint.name}' created with "
      f"{len(checkpoint.validation_definitions)} validation definitions.")

# Run it once to verify the wiring + populate Data Docs
print("\nRunning checkpoint...")
result = checkpoint.run()

print(f"\nCheckpoint success: {result.success}")
print(f"Validations run: {len(result.run_results)}")
for key, run_result in result.run_results.items():
    print(f"  {run_result.suite_name}: success={run_result.success}, "
          f"expectations={len(run_result.results)}")