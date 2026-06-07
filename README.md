# Retail Data Pipeline & Forecasting Platform

An end-to-end, production-style data pipeline built on the Walmart **M5** retail
dataset. Raw CSVs are landed, loaded into a warehouse, transformed through
medallion layers with **dbt**, validated with **Great Expectations**, and
orchestrated with **Apache Airflow** — first locally, then promoted to
**Azure (ADLS Gen2 + Data Factory) + Snowflake**.

> This README is a living document. Each phase appends a section describing
> *what* was built and *why* — both as a learning record and as project docs.

---

## Why this architecture

The project is built **local-first, then promoted to cloud**. The pipeline
logic (dbt models, data quality suites, the Airflow DAG) is developed against a
local Postgres warehouse that stands in for Snowflake, then pointed at real
cloud infrastructure with only configuration changes. This mirrors how real
data teams work: you never develop against the production warehouse — you
develop locally and promote. The key principle throughout is that **pipeline
logic is environment-agnostic**; only configuration changes between environments.

## Tech stack

| Layer            | Local (Phases 0–5)      | Cloud (Phase 6+)              |
|------------------|-------------------------|-------------------------------|
| Raw storage      | `data/raw/` (local disk)| Azure Data Lake Storage Gen2  |
| Ingestion        | Python (pandas)         | Azure Data Factory            |
| Warehouse        | PostgreSQL (Docker)     | Snowflake                     |
| Transformation   | dbt                     | dbt (same models)             |
| Data quality     | Great Expectations      | Great Expectations            |
| Orchestration    | Apache Airflow          | Apache Airflow                |
| Forecasting      | Python                  | Python                        |

## Project structure

```
retail-forecasting-platform/
├── README.md                  # this living document
├── .gitignore                 # excludes secrets, data, build artifacts
├── .env.example               # template for required environment variables
├── requirements.txt           # pinned Python dependencies
├── data/raw/                  # landed source files (bronze; gitignored)
├── ingestion/                 # Python: load raw files into the warehouse
├── dbt/                       # dbt project: staging → intermediate → mart
├── great_expectations/        # data quality suites
├── airflow/                   # DAGs and orchestration
└── docs/                      # architecture notes
```

---

## Build log

### Phase 0 — Foundations & project skeleton

Set up a reproducible workspace before any data exists.

**What was built**
- Top-level directory skeleton for every component of the pipeline.
- `.gitignore` — excludes secrets (`.env`, keys, `profiles.yml`), data files,
  and tool-generated artifacts (`target/`, logs, `__pycache__`).
- `.env.example` — documents required environment variables (Postgres now,
  Azure/Snowflake later) without exposing real values.
- `requirements.txt` — pinned Python dependencies, grown per phase.
- `data/raw/.gitkeep` — preserves the empty raw-data folder in git.

**Why it matters**
- *Secrets never enter git.* Once a credential is committed, it lives in history
  forever. Real secrets go in a gitignored `.env`; a committed `.example`
  template documents their shape.
- *Data is not code.* Git tracks the logic that processes data, not the data
  itself. Large datasets belong in object storage, not version control.
- *Reproducibility.* Pinned dependencies and a documented environment mean the
  project builds identically anywhere — the antidote to "works on my machine."
- *Config over code* (twelve-factor principle). Configuration lives in the
  environment, so identical code runs locally and in the cloud by swapping
  environment variables. This is what makes "local-first, then promote" work.

### Phase 1 — Get the dataset & land the bronze layer

Acquired the real Walmart M5 dataset, understood its true structure, and landed
it as a typed, compressed Parquet file — the "bronze" (raw, landed) layer.

**What was built**
- `profile_data.py` — samples the wide sales file (`nrows=1000`) to learn its
  shape: 1,919 columns (6 identifiers + 1,913 day columns `d_1`…`d_1913`).
- `profile_dims.py` — reads all rows but only the 6 identifier columns
  (`usecols`) to learn the true dimensional grain.
- `ingestion/melt_sales.py` — melts the wide file (30,490 item-store rows ×
  1,913 day columns) into long format (one row per item-store-day) in row-chunks,
  writing incrementally to Parquet. Output: **58,327,370 rows**.
- `ingestion/inspect_parquet.py` — verifies the landed file via Parquet metadata
  (instant row count) and a small sample.

**Dataset structure (confirmed from the data)**
- Grain: 3,049 items × 10 stores = 30,490 item-store rows.
- Dimensions: 3 states (CA, TX, WI), 10 stores, 3 categories (FOODS, HOBBIES,
  HOUSEHOLD), 7 departments.
- Melt math: 30,490 × 1,913 days = 58,327,370 long rows.

**Why it matters**
- *Sampling the head of a sorted file is not random sampling.* The first 1,000
  rows showed only 1 store (CA_1) because the file is sorted by store. Designing
  a schema off that sample would have produced a warehouse that believed the
  chain had one store. Always verify cardinality across the full file (cheaply,
  via `usecols`) before modeling.
- *Wide-to-long (melt) is the defining operation here.* The "30M+ rows" claim
  comes from unpivoting day columns into rows, not from how the file is stored.
- *Chunked processing keeps memory flat.* Reading the CSV in row-chunks and
  writing Parquet incrementally means the same code scales to far larger inputs —
  it never holds all 58M rows in memory.
- *Parquet vs CSV.* The landed Parquet holds ~1,913× more rows than the source
  CSV (58.3M vs 30,490) yet is *smaller on disk* (109 MB vs 114 MB). Columnar
  layout groups like values together (millions of zeros in `units_sold`, only 10
  distinct `store_id` values), making compression extraordinarily effective.
  Parquet is also typed (`units_sold` = int32) and self-describing (row counts and
  schema in a footer, so counting rows is instant). This is the format the cloud
  data lake (ADLS Gen2) will use.

**Row-count reconciliation** — 6 chunks × 9,565,000 + 1 chunk × 937,370 =
58,327,370 = 30,490 × 1,913. The arithmetic closing exactly confirms the melt
neither dropped nor duplicated rows.


### Phase 2 — Local warehouse & loading the bronze layer

Stood up a containerized PostgreSQL warehouse and bulk-loaded the 58M-row bronze
Parquet into it, verified by end-to-end row-count reconciliation.

**What was built**
- `docker-compose.yml` — defines Postgres 16 as a reproducible containerized
  service: pinned image, credentials/port from `.env`, a named volume for
  persistent data, and a healthcheck (`pg_isready`).
- `ingestion/create_bronze_table.sql` — creates the `bronze` schema and the
  typed `bronze.sales_long` table (TEXT identifiers, INTEGER units, NOT NULL
  contracts). Idempotent via `DROP TABLE IF EXISTS`.
- `ingestion/load_to_postgres.py` — bulk-loads the Parquet via Postgres `COPY`,
  streaming in 1M-row batches (Parquet → pandas → in-memory CSV → COPY).

**Verification (reconciliation)**
- Row count in Postgres: 58,327,370 — matches the Parquet exactly.
- Grain preserved: CA=4 stores, TX=3, WI=3 (matches Phase 1 profiling).
- Value sanity: units_sold min=0, max=763, avg=1.126 (intermittent demand —
  mostly zeros).

**Why it matters**
- *Docker = reproducible infrastructure (infra as code).* The database is
  defined in a committed file, not hand-installed, so anyone gets an identical
  warehouse with one command. Mirrors the cloud model: a warehouse is a
  networked service you connect to with credentials.
- *Named volumes separate compute from data.* The container is disposable; the
  `pgdata` volume persists. Destroying the container doesn't lose the data.
  (Snowflake decouples storage and compute for the same reason.)
- *Port mapping + `.env` = adaptability.* Local 5432 was taken, so the host port
  was changed to 5433 in ONE place (`.env`); the container's internal 5432 and
  all code were untouched. Config-over-code in action.
- *`COPY` vs row-by-row INSERT.* `COPY` bulk-streams data in batches, ~1–2 orders
  of magnitude faster than per-row INSERTs (which pay round-trip overhead 58M
  times). Snowflake's equivalent is `COPY INTO` from staged files.
- *Why load into a DB at all (vs. querying Parquet)?* A warehouse is a networked
  service with users, permissions, constraints (NOT NULL), and transactions.
  The `with conn:` transaction makes the load atomic — it fully commits or fully
  rolls back, never half-loads. A file can't offer that.
- *Reconciliation is the basic trust check.* Matching row counts at both ends of
  the pipeline prove no rows were lost or duplicated.

  ### Phase 3 — dbt transformations: staging → intermediate → marts

Built the full medallion architecture in dbt on top of the bronze layer:
light staging views, a materialized intermediate fact, and a star-schema mart,
with 20 automated data quality tests.

**What was built**
- **Staging layer** (views, in `dbt_staging` schema):
  `stg_calendar`, `stg_sales_long`, `stg_sell_prices` — one-to-one over bronze,
  renaming columns to consistent conventions, no joins.
- **Intermediate layer** (table, `dbt_intermediate.int_sales_enriched`):
  the heavy three-way join. Sales × calendar (real dates) × prices (revenue).
  58.3M rows. Inner join to calendar (every sale must have a date), left join
  to prices (preserves sales even when no price is published — ~21% of rows).
- **Marts layer** (tables, `dbt_marts` schema, star schema):
  `dim_item` (3,049 products), `dim_store` (10 stores), `dim_date` (1,969 days),
  `fct_sales_daily` (58.3M sales). Fact + 3 dimensions joined by natural keys.
- **dbt tests** (20 total, all passing):
  3 `unique` on dim PKs · 11 `not_null` on required columns · 2 `accepted_values`
  (categories, states) · 3 `relationships` (FK enforcement on fact → dim) ·
  1 singular custom test (`no_negative_revenue`).
- **`sources.yml`** declaring the bronze layer to dbt for `source()` references.
- **`profiles.yml`** in `~/.dbt/` reading credentials from `env_var()` — never
  in the repo. Same profile pattern will swap to Snowflake in Phase 6 with no
  model changes.

**Verification (real business question, answered through the star)**
Top categories by state, 2015:
- FOODS dominates every state ($24M total across CA/TX/WI)
- HOUSEHOLD second ($13M); HOBBIES third ($5.9M)
- CA leads every category (4 stores, also higher per-store revenue)
- The hierarchy is consistent across states — a cross-check that joins are correct.

**Why it matters**
- *dbt is a SQL transformation framework, not a database.* It compiles SQL
  templates, manages dependencies via `ref()`, issues DDL to the warehouse,
  and exits. dbt runs on the developer's machine (or CI); transformations
  execute inside the warehouse using the warehouse's compute. This is why
  swapping Postgres for Snowflake later requires only a `profiles.yml` change.
- *`ref()` builds the dependency graph automatically.* dbt knows what depends
  on what and builds in the right order in parallel — no manual orchestration
  inside dbt itself.
- *Materialization is a deliberate cost vs. freshness tradeoff.* Views are free
  to build and always fresh (and we pay on each query). Tables pay the build
  cost once and downstream queries read cached results. Staging = views (cheap,
  fresh); intermediate and marts = tables (pre-joined, fast to query).
- *Star schema separates facts from descriptions.* Fact tables hold the
  measurable events at the lowest grain. Dimensions hold the descriptive
  attributes you slice by. Joins are on natural keys. This is the design that
  retail analytics warehouses have used for decades because it scales: dims
  are tiny and editable, facts are narrow and aggregable.
- *Tests = automated trust.* `unique` + `not_null` + `relationships` +
  `accepted_values` catch the majority of real data-quality issues. They run
  every time the pipeline runs (in Airflow next), so the pipeline polices
  itself rather than relying on humans to remember to check.

**Operational lessons learned**
- *dbt project name vs. folder name.* `dbt_project.yml`'s `name:` must match
  Python identifier rules (no hyphens). The folder name on disk has no such
  constraint. They don't need to match each other.
- *`profiles.yml` is global per-user (`~/.dbt/profiles.yml`).* Multiple
  projects' profiles coexist as top-level YAML keys. Use `env_var()` to read
  secrets — never commit credentials.
- *`set -a; source .env; set +a` loads `.env` into the shell* so dbt's
  `env_var()` function can read them. Re-run per terminal session.
- *Folder names must exactly match `dbt_project.yml`.* `mart` vs. `marts` will
  silently fail with `[WARNING] does not match any enabled nodes`. The fix is
  always on the filesystem, not in code.
- *`dbt run` builds the warehouse; editing a file does not.* dbt's compilation
  is offline; the warehouse only changes when you explicitly run.
- *Recovery is a feature of the architecture.* When the Postgres volume
  corrupted (twice, due to disk pressure), the recovery was: free space →
  rebuild bronze from Parquet/CSV → `dbt run`. Total recovery time ~15 minutes,
  zero data loss because raw stays untouched and transformations are
  version-controlled. Backed up bronze with `pg_dump` so future recovery is
  ~30 seconds.

### Phase 4 — Great Expectations: the data quality gate

Built a contract-driven quality layer that validates the bronze tables before
downstream models consume them. The pipeline now polices itself.

**What was built**
- `great_expectations/setup_context.py` — creates the GX FileDataContext,
  registers Postgres as a Data Source (credentials from `.env`), and registers
  the three bronze tables as Data Assets. Idempotent.
- Three Expectation Suites, persisted as JSON in
  `great_expectations/gx/expectations/`:
  - `bronze_calendar_suite` (13 expectations): row count, column schema,
    primary keys (date, d), value ranges (year, month, wday), SNAP flag
    membership.
  - `bronze_sales_long_suite` (16 expectations): row count band, schema, not-null
    on every column, low-cardinality value sets (introspected from data:
    3 categories, 3 states, 7 depts, 10 stores), `units_sold` range, `d` regex.
  - `bronze_sell_prices_suite` (9 expectations): row count band, schema,
    not-null on every column, store_id membership, wm_yr_wk range, sell_price
    range.
- `great_expectations/build_checkpoint.py` — defines `bronze_layer_checkpoint`,
  a single named workflow that runs all three suites and updates Data Docs.
  Result: one boolean pipeline gate, ~2 minutes wall time.
- `ingestion/gx_gate.py` — reusable Python module. Exports `run_bronze_gate()`
  which loads the context, runs the checkpoint, prints per-suite results, and
  raises `GxGateFailure` if any suite fails. Reused by the loader (now) and the
  Airflow DAG (Phase 5).
- `ingestion/load_to_postgres.py` — modified to call `run_bronze_gate()` after
  the COPY load completes. On gate failure: prints to stderr and exits 1. This
  is the contract with callers (Airflow, CI, shell pipelines).
- Data Docs site rendered to
  `great_expectations/gx/uncommitted/data_docs/local_site/index.html` — static
  HTML with browsable suites and a history of every validation run.

**Verification (the pass → fail → pass arc)**
1. Clean run: all 38 expectations pass across 3 suites. Pipeline proceeds.
2. Injected one bad row violating multiple expectations (`cat_id=TEST_CAT`,
   `state_id=XX`, `store_id=BAD_STORE`, `units_sold=-5`).
3. Re-ran the gate: `bronze_sales_long_suite` failed, exception raised, the
   script halted with non-zero exit. Calendar and prices still passed (gate is
   granular per-table).
4. Deleted the bad row, re-ran: all 38 expectations pass. Gate cleared.

That sequence is the entire point of Phase 4: one bad row out of 58M+ was
enough to halt the pipeline. The contract is sensitive enough to catch real
anomalies, granular enough to point at the failing table, and integrated
enough to surface in a single boolean any downstream component can react to.

**Why it matters**
- *GX runs at the ingestion boundary; dbt tests run after transformation.*
  Together they form a complete data contract. GX catches source-side problems
  (schema drift, value out of range, file shape) before bad data enters the
  warehouse. dbt tests catch transformation-side problems (broken joins,
  derived measures gone wrong) after models build. Different failure modes,
  different layers.
- *Expectations are version-controlled JSON.* The three `*_suite.json` files
  in `gx/expectations/` are the contract — committable, reviewable, diffable.
  Anyone who clones the project gets the same data quality assertions.
- *Introspection-then-assert pattern.* For low-cardinality columns (cat_id,
  state_id, dept_id, store_id), we read distinct values from Postgres first
  and bake them into the suite. Same result as hardcoding for a static
  dataset, but the technique is what scales: rebuild the contract from a
  known-good baseline whenever the legitimate value set legitimately changes.
- *Checkpoint = one gate, one boolean.* The checkpoint groups three suites
  into a single named workflow. `result.success` is the pipeline's gate
  signal: True → proceed, False → halt. This is what Airflow tasks check.
- *Custom exception (`GxGateFailure`) over `sys.exit(1)`.* Exceptions
  propagate through Python's call stack cleanly. Airflow handles them
  naturally — a raised exception fails the task and halts downstream tasks.
  `sys.exit` works for CLI scripts but is the wrong primitive inside a
  reusable function.
- *Data Docs = self-maintaining documentation.* Every run appends to the
  validation history. Non-engineers can browse the site and understand the
  data contract without reading code. This is the kind of artifact that
  changes "data quality" from a vague claim into a visible, shared thing.

**Operational lessons learned**
- *GX 1.x is Python-first, not YAML-first.* All configuration lives in
  Python scripts (`setup_context.py`, `build_*.py`). Older 0.18 tutorials
  showing YAML configs and `great_expectations init` wizards do not apply.
- *`project_root_dir` is the GX home, not its parent.* GX writes scaffolding
  directly into the directory you point it at, plus a `gx/` subfolder for
  internal state. Took two attempts to get the path right.
- *`add_or_update` is idempotent for names, not for contents.* Adding the
  same expectation twice creates two copies of it on the suite. Always reset
  `suite.expectations = []` before re-adding in idempotent setup scripts.
- *Validation time scales with row count, not violation count.* The same
  ~2:10 to validate 58M rows whether 0 or 1 row violated. Per-metric SQL
  scans dominate.