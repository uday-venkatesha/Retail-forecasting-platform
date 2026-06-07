"""
Reusable GX gate. Loads the local context and runs a named checkpoint.
Raises on failure so callers can let the exception propagate (the right
pattern for Airflow tasks — a raised exception fails the task cleanly).
"""
import great_expectations as gx

CONTEXT_DIR  = "great_expectations"
CHECKPOINT   = "bronze_layer_checkpoint"


class GxGateFailure(Exception):
    """Raised when a GX checkpoint fails — the pipeline should stop."""


def run_bronze_gate() -> None:
    """Run the bronze layer checkpoint. Raises GxGateFailure on any failure."""
    context = gx.get_context(mode="file", project_root_dir=CONTEXT_DIR)
    checkpoint = context.checkpoints.get(CHECKPOINT)

    print(f"\n--- Running GX checkpoint: {CHECKPOINT} ---")
    result = checkpoint.run()

    # Summarize each suite's outcome
    for _, run_result in result.run_results.items():
        status = "PASS" if run_result.success else "FAIL"
        n_exp = len(run_result.results)
        print(f"  [{status}] {run_result.suite_name}: {n_exp} expectations")

    if not result.success:
        failed_suites = [
            r.suite_name for r in result.run_results.values() if not r.success
        ]
        raise GxGateFailure(
            f"Bronze quality gate failed: {failed_suites}. "
            f"Inspect Data Docs at great_expectations/gx/uncommitted/data_docs/local_site/index.html"
        )

    print(f"\n--- All {len(result.run_results)} suites passed. Pipeline may proceed. ---\n")