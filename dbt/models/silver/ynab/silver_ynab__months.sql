{{ config(location=external_location('silver', 'ynab/months')) }}

with source as (
    select * from {{ source('ynab', 'months') }}
),

-- Bronze accumulates one row per record per ingest run; keep the newest version of each month.
latest as (
    select *
    from source
    qualify row_number() over (partition by _budget_id, month order by _ingested_at desc) = 1
)

select
    try_cast(month as date)                                 as month,
    _budget_id                                              as budget_id,
    try_cast(note as varchar)                               as note,
    -- YNAB stores money as integer milliunits; divide by 1000 for currency units.
    income / 1000.0                                         as income,
    budgeted / 1000.0                                       as budgeted,
    activity / 1000.0                                       as activity,
    to_be_budgeted / 1000.0                                 as to_be_budgeted,
    age_of_money                                            as age_of_money,
    deleted                                                 as is_deleted,
    strptime(_ingested_at, '%Y%m%dT%H%M%SZ')                as ingested_at
from latest
