{{ config(location=external_location('silver', 'ynab/category_months')) }}

with source as (
    select * from {{ source('ynab', 'category_months') }}
),

-- Bronze re-snapshots every category for every month each run (no delta on the
-- month-detail endpoint); keep the newest version of each category-month.
latest as (
    select *
    from source
    qualify row_number() over (partition by _budget_id, _month, id order by _ingested_at desc) = 1
)

select
    id                                                     as category_id,
    _budget_id                                             as budget_id,
    try_cast(_month as date)                               as month,
    category_group_id                                      as category_group_id,
    category_group_name                                    as category_group_name,
    name                                                   as category_name,
    hidden                                                 as is_hidden,
    internal                                               as is_internal,
    -- YNAB stores money as integer milliunits; divide by 1000 for currency units.
    budgeted / 1000.0                                      as budgeted,
    activity / 1000.0                                      as activity,
    balance / 1000.0                                       as balance,
    goal_type                                              as goal_type,
    goal_target / 1000.0                                   as goal_target,
    deleted                                                as is_deleted,
    strptime(_ingested_at, '%Y%m%dT%H%M%SZ')               as ingested_at
from latest
