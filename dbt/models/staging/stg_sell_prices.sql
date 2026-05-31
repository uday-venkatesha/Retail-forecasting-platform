-- Staging model for weekly sell prices.
-- Source: bronze.sell_prices (6.8M item-store-week rows)

select
    store_id,
    item_id,
    wm_yr_wk          as week_id,         -- joins to stg_calendar.week_id
    sell_price
from {{ source('bronze', 'sell_prices') }}