-- Staging model for the long-format sales facts.
-- Source: bronze.sales_long (58.3M rows landed in Phase 2)

select
    id                as sales_id,        -- composite key: item_store_validation
    item_id,
    dept_id,
    cat_id,
    store_id,
    state_id,
    d                 as day_id,           -- joins to stg_calendar.day_id
    units_sold
from {{ source('bronze', 'sales_long') }}