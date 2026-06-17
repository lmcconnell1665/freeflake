{{ config(location=external_location('gold', 'finance/dates')) }}

-- Date dimension: one row per calendar day. Generated (not sourced) from
-- 2025-01-01 through the last day of the current year (range() upper bound is
-- exclusive, so we stop at Jan 1 of next year). Join facts on the `date` column.

with spine as (
    select unnest(range(
        date '2025-01-01',
        make_date(year(current_date) + 1, 1, 1),
        interval 1 day
    ))::date as date
)

select
    date                                            as date,
    year(date)                                      as year,
    month(date)                                     as month,
    day(date)                                       as day,
    monthname(date)                                 as month_name,
    quarter(date)                                   as quarter,
    dayofyear(date)                                 as day_of_year,
    weekofyear(date)                                as week_of_year,
    -- Day-of-week: Sunday = 0 ... Saturday = 6.
    dayofweek(date)                                 as day_of_week_number,
    dayname(date)                                   as day_of_week_name,
    dayofweek(date) in (0, 6)                       as is_weekend,
    date = last_day(date)                           as is_last_day_of_month,
    date_trunc('month', date)::date                 as first_day_of_month,
    last_day(date)                                  as last_day_of_month,
    strftime(date, '%Y-%m')                         as month_year
from spine
