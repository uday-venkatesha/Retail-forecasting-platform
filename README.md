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