{{ config(location=external_location('silver', 'ynab/categories')) }}

with source as (
    select * from {{ source('ynab', 'categories') }}
),

-- Bronze accumulates one flat row per category per ingest run (delta loads only
-- write changed categories); keep the newest version of each category.
latest as (
    select *
    from source
    qualify row_number() over (partition by _budget_id, id order by _ingested_at desc) = 1
),

-- The flat category row carries category_group_id but not the group name, so
-- join it from the groups model.
groups as (
    select budget_id, category_group_id, category_group_name
    from {{ ref('silver_ynab__category_groups') }}
)

select
    c.id                                                   as category_id,
    c._budget_id                                           as budget_id,
    c.category_group_id                                    as category_group_id,
    g.category_group_name                                  as category_group_name,
    c.name                                                 as category_name,
    c.hidden                                               as is_hidden,
    c.internal                                             as is_internal,
    c.note                                                 as note,
    -- YNAB stores money as integer milliunits; divide by 1000 for currency units.
    c.budgeted / 1000.0                                    as budgeted,
    c.activity / 1000.0                                    as activity,
    c.balance  / 1000.0                                    as balance,
    c.goal_type                                            as goal_type,
    c.goal_target / 1000.0                                 as goal_target,
    try_cast(c.goal_percentage_complete as integer)        as goal_percentage_complete,
    c.deleted                                              as is_deleted,
    strptime(c._ingested_at, '%Y%m%dT%H%M%SZ')             as ingested_at
from latest c
left join groups g
    on c._budget_id = g.budget_id
   and c.category_group_id = g.category_group_id
