-- Intermediate model: sales enriched with calendar dates and weekly prices.
-- Grain: one row per item-store-day (58.3M rows).
-- Joins:
--   sales -> calendar  on day_id   (gets real date + week_id)
--   sales -> prices    on (store_id, item_id, week_id)  (gets sell_price)
--
-- Materialized as a TABLE (per dbt_project.yml) so downstream marts query
-- pre-joined data at table-scan speed instead of recomputing 3-way joins.

with sales as (
    select * from {{ ref('stg_sales_long') }}
),

calendar as (
    select * from {{ ref('stg_calendar') }}
),

prices as (
    select * from {{ ref('stg_sell_prices') }}
)

select
    s.sales_id,
    s.item_id,
    s.dept_id,
    s.cat_id,
    s.store_id,
    s.state_id,

    -- Time dimensions (from calendar)
    c.calendar_date,
    c.calendar_year,
    c.calendar_month,
    c.weekday_name,
    c.week_id,
    c.event_name_1,                          -- holiday / event for this date
    c.event_type_1,

    -- Measures
    s.units_sold,
    p.sell_price,
    s.units_sold * p.sell_price as revenue   -- the headline derived measure

from sales s
inner join calendar c
    on s.day_id = c.day_id
left join prices p                            -- left join: keep sales even if no price
    on  s.store_id = p.store_id
    and s.item_id  = p.item_id
    and c.week_id  = p.week_id