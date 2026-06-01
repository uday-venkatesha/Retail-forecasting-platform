-- Dimension: product master.
-- One row per item with its categorical attributes.

with sales as (
    select distinct
        item_id,
        dept_id,
        cat_id
    from {{ ref('stg_sales_long') }}
)

select
    item_id,                             -- the natural key
    dept_id   as department,
    cat_id    as category
from sales