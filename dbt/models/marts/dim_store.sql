-- Dimension: store master.
-- One row per store with its state.

with sales as (
    select distinct
        store_id,
        state_id
    from {{ ref('stg_sales_long') }}
)

select
    store_id,
    state_id   as state
from sales