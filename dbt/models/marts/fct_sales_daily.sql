-- Fact table: daily sales at item-store grain.
-- Pulls from the pre-joined intermediate model — the heavy join already happened.

select
    item_id,
    store_id,
    calendar_date,                       -- foreign key to dim_date

    -- measures
    units_sold,
    sell_price,
    revenue
from {{ ref('int_sales_enriched') }}