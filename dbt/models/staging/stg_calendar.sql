-- Staging model for the calendar dimension.
-- Source: bronze.calendar (loaded in Phase 2)
-- Job: rename columns to project conventions, cast where needed, nothing more.

select
    "date"            as calendar_date,
    wm_yr_wk          as week_id,
    weekday           as weekday_name,
    wday              as weekday_num,
    month             as calendar_month,
    year              as calendar_year,
    d                 as day_id,
    event_name_1,
    event_type_1,
    event_name_2,
    event_type_2,
    snap_ca,
    snap_tx,
    snap_wi
from {{ source('bronze', 'calendar') }}