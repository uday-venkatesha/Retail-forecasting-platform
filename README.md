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