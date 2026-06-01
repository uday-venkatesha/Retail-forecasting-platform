-- Dimension: date.
-- One row per calendar day with descriptive attributes for slicing.

select
    calendar_date,                       -- the natural key for joining
    calendar_year   as year,
    calendar_month  as month,
    weekday_name    as weekday,
    weekday_num,
    week_id,
    event_name_1    as event_name,
    event_type_1    as event_type,
    case when event_name_1 is not null then true else false end as is_event_day
from {{ ref('stg_calendar') }}