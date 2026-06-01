-- Revenue should never be negative.
-- If this query returns any rows, the test FAILS — they are the violations.

select
    item_id,
    store_id,
    calendar_date,
    revenue
from {{ ref('fct_sales_daily') }}
where revenue < 0
