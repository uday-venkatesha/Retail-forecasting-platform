-- Bronze layer table: the landed sales data, one row per item-store-day.
-- Schema mirrors the typed Parquet we produced in Phase 1.

CREATE SCHEMA IF NOT EXISTS bronze;

DROP TABLE IF EXISTS bronze.sales_long;

CREATE TABLE bronze.sales_long (
    id        TEXT      NOT NULL,
    item_id   TEXT      NOT NULL,
    dept_id   TEXT      NOT NULL,
    cat_id    TEXT      NOT NULL,
    store_id  TEXT      NOT NULL,
    state_id  TEXT      NOT NULL,
    d         TEXT      NOT NULL,
    units_sold INTEGER  NOT NULL
);